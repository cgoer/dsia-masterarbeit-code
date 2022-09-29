from masterthesis.db.dbconnection import DbConnection
import pandas as pd
import re
import gensim
import math
from os import getpid
from multiprocessing import Process
import spacy
import nltk
from nltk.corpus import stopwords
nltk.download('wordnet')
nltk.download('omw-1.4')
nltk.download('punkt')
nltk.download('stopwords')
nlp = spacy.load('de_core_news_lg')

def get_raw_data(db):
    """
    loads raw data from database
    :param db:
    :return:
    """
    cursor = db['rawdata'].find({}, {'_id':1, 'url_id':1, 'source_domain':1, 'date_publish':1, 'maintext':1})
    return pd.DataFrame(list(cursor))

def get_sublists(df, no_threads):
    """
    splits dataset into chunks for multiprocessing
    :param df:
    :param no_threads:
    :return:
    """
    n = math.ceil(len(df) / no_threads)
    return [df[i:i + n] for i in range(0, df.shape[0], n)]

def lemmatize_stemming(text, stemmer):
    """
    performs word lemmatization and stemming
    :param text:
    :param stemmer:
    :return:
    """
    return stemmer.stem(nltk.stem.WordNetLemmatizer().lemmatize(text, pos='v'))

# Copies gensim.utils.simple_preprocess function but keeps original words
def tokenize_with_originals(text, deacc=False, min_len=2, max_len=15):
    """
    copy of gensim.utils.simple_preprocess
    Keeps original word alongside preorpcessed version.
    :param text:
    :param deacc:
    :param min_len:
    :param max_len:
    :return:
    """
    result = []
    text = gensim.utils.to_unicode(text, 'utf8', errors='ignore')
    for token in gensim.utils.simple_tokenize(text):
        orig_token = token
        if min_len <= len(token) <= max_len and not token.startswith('_'):
            token = token.lower()
            if deacc:
                token = gensim.utils.deaccent(token)
            result.append(
                [orig_token, token]
            )
    return result

def preprocess(text):
    """
    Tokenize into words, Lemmatize, remove stopwords and small words, remove umlauts and accents
    :param text:
    :return:
    """
    stemmer = nltk.stem.SnowballStemmer("german")
    preprocessed = []
    for original, token in tokenize_with_originals(text, deacc=True, max_len=100):
        if token not in stopwords.words('german') and len(token) > 3:
            stemmed = stemmer.stem(nltk.stem.WordNetLemmatizer().lemmatize(token, pos='v'))
            preprocessed.append({'original':original,'preprocessed': stemmed})
    return pd.DataFrame.from_dict(preprocessed)


def add_tfidf_to_preprocessed(df, preprocessed_keyword = 'preprocessed'):
    """
    determines TF-IDF value and adds it to preprocessed dict
    :param df:
    :param preprocessed_keyword:
    :return:
    """
    tokens = [results['preprocessed'] for results in df[preprocessed_keyword]]

    #create word dictionary
    dictionary = gensim.corpora.Dictionary(tokens)
    # create bag of  words
    bow_corpus = [dictionary.doc2bow(doc) for doc in tokens]
    # create tfidf model
    tfidf = gensim.models.TfidfModel(bow_corpus)
    # get tfidf scores for corpus
    corpus_tfidf = tfidf[bow_corpus]

    #extract and add to preprocessed column for each row
    idx = 0
    tfidf = []
    for doc in corpus_tfidf:
        pp_df = df[preprocessed_keyword][idx]
        pp_df_new = []
        for id, freq in doc:
            res = pp_df[pp_df['preprocessed'] == dictionary[id]]
            for index, r in res.iterrows():
                pp_df_new.append({'original':r['original'], 'preprocessed': r['preprocessed'], 'tfidf_freq': freq})

        df[preprocessed_keyword][idx] = pd.DataFrame.from_dict(pp_df_new)
        idx += 1
    return df

def get_allowed_entities(maintext):
    """
    returns all nound from text
    :param maintext:
    :return:
    """
    doc = nlp(maintext)
    # Only allow Nouns and Prepositions as Entities
    allowed_entities = []
    for token in doc:
        if ((token.head.pos_ == 'NOUN' or token.head.pos_ == 'PROPN') and (str(token.head) not in allowed_entities)):
            allowed_entities.append(str(token.head))
    return allowed_entities

def filter_preprocessed_for_allowed_entities(maintext, preprocessed):
    """
    returns only allowed keywords in preprocessed dict
    :param maintext:
    :param preprocessed:
    :return:
    """
    try:
        return preprocessed[preprocessed['original'].isin(get_allowed_entities(maintext))]
    except:
        return pd.DataFrame([{'original': '', 'preprocessed': '', 'tfidf_freq': ''}])

def add_top_keywords_and_scores(df):
    """
    determines top-3 keywords according to TF-IDF score
    :param df:
    :return:
    """
    df = df.reset_index()
    tfidf_results = pd.DataFrame(columns=['tfidf_top_scores', 'tfidf_top_preprocessed', 'tfidf_top_original'])
    for index, row in df.iterrows():
        try:
            grouped_orig = row['preprocessed'].groupby(by=['original']).max().reset_index()
            grouped = grouped_orig.groupby(by=['preprocessed']).max().reset_index()

            tfidf_top_scores = grouped.nlargest(n=3, columns=['tfidf_freq'])['tfidf_freq'].values
            tfidf_words = grouped.nlargest(n=3, columns=['tfidf_freq'])['preprocessed'].values

            # search for all original words from the stemmed form and return them as list
            original_top_words = []
            for tfidf_word in tfidf_words:
                original_top_words.append(list(grouped_orig[grouped_orig['preprocessed'] == tfidf_word]['original'].values))

            tfidf_results = tfidf_results.append({'tfidf_top_scores': list(tfidf_top_scores), 'tfidf_top_preprocessed': list(tfidf_words), 'tfidf_top_original': list(original_top_words)}, ignore_index=True)
        except:
            df = df.drop([index])
            print('Error occurred while picking top keywords. Skipping. url_id: {}'.format(row['url_id']))

    df = df.drop(columns=['preprocessed'])
    df['tfidf_top_scores'] = tfidf_results['tfidf_top_scores']
    df['tfidf_top_preprocessed'] = tfidf_results['tfidf_top_preprocessed']
    df['tfidf_top_original'] = tfidf_results['tfidf_top_original']

    return df

def get_sentences_with_occurrences(maintext, original_top_words):
    """
    returns all sentences where specified keywords occurr
    :param maintext:
    :param original_top_words:
    :return:
    """
    maintext = maintext.replace('\n', '. ')
    tkn = nltk.tokenize.sent_tokenize(maintext, language='german')
    sentences = [[], [], []]
    lengths = [[], [], []]
    for sentence in tkn:
        keyword_set = original_top_words
        for i in [0, 1, 2]:
            for keyword in keyword_set[i]:
                if keyword in sentence and sentence not in sentences[i]:
                    sentences[i].append(sentence)
                    lengths[i].append(len(sentence))
    return sentences, lengths

def filter_and_save_sentences(sentences, lengths, row):
    """
    Chooses longest sentence where each keyword occurrs; saves tuple to database
    :param sentences:
    :param lengths:
    :param row:
    :return:
    """
    for i in [0, 1, 2]:
        idx = lengths[i].index(max(lengths[i]))
        final_sentence = sentences[i][idx]
        model = {}
        for original in row['tfidf_top_original'][i]:
            if (original in final_sentence):
                match = re.search(re.escape(original), final_sentence)
                model = {'date_publish': row['date_publish'],
                         'source_domain': row['source_domain'],
                         'url_id': row['url_id'],
                         'tfidf_score': row['tfidf_top_scores'][i],
                         'preprocessed_word': row['tfidf_top_preprocessed'][i],
                         'original_word_main': original,
                         'sentence': final_sentence,
                         'main_start_pos': match.start(),
                         'main_end_pos': match.end(),
                         }
        dbconnection = DbConnection()
        db = dbconnection.get_database()
        db.preprocessed.insert_many(pd.DataFrame([model]).to_dict('records'))

def extract_sentences_and_save(df):
    """
    performs sentence extraction and database save
    :param df:
    :return:
    """
    for index, row in df.iterrows():
        try:
            sentences, lengths = get_sentences_with_occurrences(row['maintext'], row['tfidf_top_original'])
            filter_and_save_sentences(sentences, lengths, row)
        except:
            print('Error in url_id: {}. Skipping.'.format(row['url_id']))

def postprocessing_worker(raw_data):
    """
    postprocessing process instance
    :param raw_data:
    :return:
    """
    print('started worker {}'.format(getpid()))
    raw_data['preprocessed'] = raw_data.apply(
        lambda x: filter_preprocessed_for_allowed_entities(x.maintext, x.preprocessed), axis=1)
    print('retrieving top tfidf keywords and scores')
    raw_data = add_top_keywords_and_scores(raw_data)
    extract_sentences_and_save(raw_data)

def run_postprocessing_multithread(raw_data_chunks, no_of_chunks):
    """
    runs multiple instances of postprocessing instance in parallel
    :param raw_data_chunks:
    :param no_of_chunks:
    :return:
    """
    processes = []
    for thread_no in range(0, no_of_chunks):
        process = Process(target=postprocessing_worker,
                          args=(raw_data_chunks[thread_no], ))
        processes.append(process)
        process.start()

    for proc in processes:
        proc.join()

if __name__ == '__main__':
    dbconnection = DbConnection()
    db = dbconnection.get_database()
    raw_data = get_raw_data(db)
    print('found {} articles.'.format(len(raw_data)))
    raw_data['preprocessed'] = raw_data['maintext'].map(preprocess)

    print('calculating ifidf scores')
    raw_data = add_tfidf_to_preprocessed(raw_data)

    print('starting postprocessing multithread')
    no_of_chunks = 1
    raw_data_chunks = list(get_sublists(raw_data, no_of_chunks))
    run_postprocessing_multithread(raw_data_chunks, no_of_chunks)