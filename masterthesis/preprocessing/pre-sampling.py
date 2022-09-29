from masterthesis.db.dbconnection import DbConnection
from masterthesis.utils import get_sublists
import pandas as pd
from textblob_de import TextBlobDE as TextBlob
from multiprocessing import Process
from os import getpid

def get_preprocessed_data(db):
    """
    returns dataFrame of preprocessed data from database
    :param db:
    :return:
    """
    cursor = db['preprocessed'].find({}, {'_id':1, 'date_publish':1, 'source_domain':1, 'url_id':1,'preprocessed_word':1, 'sentence':1})
    return pd.DataFrame(list(cursor))

def find_matches(df, row):
    """
    returns articles a matches, which bare the same keyword on the same day but are published by different news outlets
    :param df:
    :param row:
    :return:
    """
    df.drop([row.name])
    # find all rows with the same keyword
    df = df.loc[df['preprocessed_word'] == row['preprocessed_word']]
    # and date
    df = df.loc[df['date'] == row['date']]
    # but discard all of the same domain
    df = df.loc[~(df['source_domain']==row['source_domain'])]
    return list(df['url_id'].values)

def get_polarity(sentence):
    """
    returns sentiment polarity for sentence
    :param sentence:
    :return:
    """
    blob = TextBlob(sentence)
    return blob.sentiment.polarity

def iterate_df(df, df_split):
    """
    processes a row in dataframe
    :param df:
    :param df_split:
    :return:
    """
    print('started worker {}'.format(getpid()))
    matches = []
    polarity = []
    counter = 1
    leng = len(df_split)
    for index, row in df_split.iterrows():
        print('searching for matches and subjectivity {}/{}'.format(counter, leng))
        matches.append(find_matches(df, row))
        pol = get_polarity(row['sentence'])
        polarity.append(pol)
        counter += 1
    print('process {} done. saving.'.format(getpid()))
    df_split['matches'] = matches
    df_split['polarity'] = polarity
    df_split = df_split.drop(columns=['date_publish', 'source_domain', 'sentence', 'date'])
    dbconnection = DbConnection()
    db = dbconnection.get_database()
    db.presampled.insert_many(df_split.to_dict('records'))

if __name__ == '__main__':
    dbconnection = DbConnection()
    db = dbconnection.get_database()
    preprocessed = get_preprocessed_data(db)

    preprocessed['date'] = pd.to_datetime(preprocessed['date_publish']).dt.date

    no_of_chunks = 1
    raw_data_chunks = list(get_sublists(preprocessed, no_of_chunks))

    processes = []
    for thread_no in range(0, no_of_chunks):
        process = Process(target=iterate_df,
                          args=(preprocessed, raw_data_chunks[thread_no], ))
        processes.append(process)
        process.start()

    for proc in processes:
        proc.join()