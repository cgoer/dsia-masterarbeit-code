from newsplease import NewsPlease
from masterthesis.db.dbconnection import DbConnection
import sys
from multiprocessing import Process
import math

def transform(article, source, source_id):
    """
    transforms news-please data into desired format
    :param article:
    :param source:
    :param source_id:
    :return:
    """
    return {
        'date_download': str(article.date_download),
        'authors': article.authors,
        'date_modify': str(article.date_modify),
        'date_publish': str(article.date_publish),
        'description': article.description,
        'language': article.language,
        'maintext': article.maintext,
        'source_domain': article.source_domain,
        'text': article.text,
        'title': article.title,
        'url':article.url,
        'url_source': source,
        'url_id': source_id,
    }

def get_sublists(d, no_threads):
    """
    returns chunks of dataset for multiprocessing
    :param d:
    :param no_threads:
    :return:
    """
    n = math.ceil(len(d) / no_threads)
    keys = list(d.keys())
    for i in range(0, len(keys), n):
        yield {k: d[k] for k in keys[i: i + n]}

def get_already_crawled_ids(source, db):
    """
    loads all ids of already crawled urls
    :param source:
    :param db:
    :return:
    """
    resultset = []
    for col in db['newspleasedata'].find({'url_source': source},{'url_id'}):
        resultset.append(col['url_id'])
    return resultset

def get_erronous_ids(source, db):
    """
    loads all ids which caused errors previously
    :param source:
    :param db:
    :return:
    """
    resultset = []
    for col in db['newspleaseerrors'].find({'url_source': source}, {'url_id'}):
        resultset.append(col['url_id'])
    return resultset

def get_urls(source, excluded_ids, db):
    """
    returns all urls from database
    :param source:
    :param excluded_ids:
    :param db:
    :return:
    """
    resultset = {}
    for col in db[source].find({}, {'_id': 1, 'url': 1}):
        resultset[col['_id']] = col['url']
    for rmv_id in excluded_ids:
        del resultset[rmv_id]
    return resultset

def write_error_to_db(dbconnection, url_source, url_id, url, error_type, error_value):
    data = {
        'url_source': url_source,
        'url_id': url_id,
        'url': url,
        'error_type': error_type,
        'error_value': error_value
    }
    dbconnection.insert_one('newspleaseerrors', data)

def get_article_data(urls, source):
    """
    calls news-please and returns full article data
    :param urls:
    :param source:
    :return:
    """
    dbconnection = DbConnection()
    counter = 1
    for url_id in urls:
        print('obtaining {} of {}'.format(counter, len(urls)))
        try:
            res = NewsPlease.from_url(urls[url_id])
            if res is not None:
                dbconnection.insert_one('newspleasedata', transform(res, source, url_id))
            else:
                write_error_to_db(dbconnection, source, url_id, urls[url_id], 'NoneResponse',
                                       'Nothing retrieved for this URL.')
        except:
            type, value, traceback = sys.exc_info()
            write_error_to_db(dbconnection, source, url_id, urls[url_id], str(type), str(value))
        counter += 1

if __name__ == '__main__':
    source = 'thenewsapidata'
    allow_multithreading = True
    no_threads = 5
    dbconnection = DbConnection()
    db = dbconnection.get_database()

    ids_to_exclude = get_already_crawled_ids(source, db)
    ids_to_exclude += get_erronous_ids(source, db)
    urls = get_urls(source, ids_to_exclude, db)
    print('Still {} articles to crawl.'.format(len(urls)))

    if len(urls) < 1:
        exit(0)

    if (allow_multithreading == False):
        get_article_data(urls, source)
    else:
        print('Entering Multithreading mode with {} threads.'.format(no_threads))
        processes = []
        url_lists = list(get_sublists(urls, no_threads))
        for thread_no in range(0, no_threads):
            print('Thread started')
            process = Process(target=get_article_data, args=(url_lists[thread_no], source,))
            processes.append(process)
            process.start()

        for proc in processes:
            proc.join()




