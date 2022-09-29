import urllib.parse
import requests
import json
from datetime import timedelta, datetime
import math
from masterthesis.db.dbconnection import DbConnection
from masterthesis.utils import get_project_root

CONFIG_FILE = '{}/raw-data/thenewsapi-conf.txt'.format(get_project_root())

def request_api_or_die(params):
    """
    performs api request. Exits Program if error occurs
    :param params:
    :return:
    """
    resultset = requests.get('https://api.thenewsapi.com/v1/news/all?{}'.format(params))
    if resultset.status_code != 200:
        print(resultset.status_code)
        print(resultset.reason)
        exit(1)
    return json.loads(resultset.content)

def daterange(start_date, end_date):
    """
    creates date range between specified dates
    :param start_date:
    :param end_date:
    :return:
    """
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

def read_config():
    return json.load(open(CONFIG_FILE))

def write_config(data):
    json.dump(data, open(CONFIG_FILE, 'w'))

def write_to_db(data, db):
    for line in data:
        db.insert_one('thenewsapidata', line)

if __name__ == '__main__':
    db = DbConnection()
    config = read_config()
    params = {
        'api_token': config['api_token'],
        'limit': config['limit'],
        'published_on': '',
        'domains': '',
        'page':'',
    }

    # Take last completed date and continue with following day
    start_date = datetime.strptime(config['last_ok_date'], "%Y-%m-%d") + timedelta(days=1)
    end_date = datetime.strptime(config['end_date'], "%Y-%m-%d")
    for single_date in daterange(start_date, end_date):
        date = single_date.strftime("%Y-%m-%d")
        print('------------------------------------------')
        print('Crawling News Info for {}'.format(date))

        params['published_on'] = date
        params['page'] = '1'
        params['domains'] = 't-online.de,focus.de,focus-online.de,bild.de,welt.de,n-tv.de,spiegel.de,rtl.de,frankfurter-allgemeine.de,faz.de,stern.de,rnd.de,sueddeutsche.de,zeit.de,orf.at,krone.at,heute.at,derstandard.at,kurier.at,kleinezeitung.at,srf.ch,20min.ch,blick.ch,nzz.ch,bluewin.ch,watson.ch'
        # Perform first request to check result size
        result = request_api_or_die(urllib.parse.urlencode(params))

        total_results = result['meta']['found']
        pages = math.ceil(total_results/params['limit'])

        print('Found {} Articles on {} pages'.format(total_results, pages))

        if len(result['data']) > 0:
            write_to_db(result['data'], db)

        if pages > 1:
            # Iterate through all other pages
            for page in range(pages):
                if page < 1:
                    continue
                params['page'] = str(page)
                result = request_api_or_die(urllib.parse.urlencode(params))
                if len(result['data']) > 0:
                    write_to_db(result['data'], db)

        print('Done for the day. Writing progress to Config file.')
        config['last_ok_date'] = date
        write_config(config)