from pymongo import MongoClient
import json
from datetime import datetime
import uuid
from masterthesis.utils import get_project_root

class DbConnection:
    _client = None
    _config = None
    _dbName = 'masterthesis-goerner'

    def __init__(self):
        self._config = json.load(open('{}/db/db-conf.txt'.format(get_project_root())))

    def get_client(self):
        """
        Returns mongo Client instance
        :return:
        """
        if self._client is not None:
            return self._client
        self._client = MongoClient('mongodb://{}:{}@{}:{}'.format(
            self._config['db_user'],
            self._config['db_pass'],
            self._config['db_host'],
            self._config['db_port'],
        ))
        return self._client

    def get_database(self):
        """
        Returns mongo client instance connected to database
        :return:
        """
        client = self.get_client()
        return client.get_database(self._dbName)

    def insert_one(self, collection_name: str, data, id=None):
        """
        inserts one tuple to a specified collection. sets tuple-id automatically if not specified
        :param collection_name:
        :param data:
        :param id:
        :return:
        """
        db = self.get_database()
        collection = db[collection_name]
        if id is None or '_id' not in data.keys():
            data['_id'] = uuid.uuid4().hex

        collection.insert_one(data)

    def backup_database(self):
        """
        Performs database backup
        :return:
        """
        db = self.get_database()
        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        for collection in db.list_collection_names():
            print('backing up {}'.format(collection))
            total = db[collection].estimated_document_count()
            print('total: {} entries'.format(total))
            current = 1
            with open('{}/db/backup/{}/{}_{}.json'.format(get_project_root(),self._dbName, collection, timestamp), 'w') as f:
                f.write('[')
                for doc in db[collection].find():
                    json.dump(doc, f, default=str)
                    if current < total:
                        f.write(',')
                    current += 1
                f.write(']')

    def restore_database_from_backup(self, backup_file_name):
        """
        restores colelction from specific file in backup directory
        :param backup_file_name:
        :return:
        """
        db = self.get_database()
        collection_name = backup_file_name.split('_')[0]
        collection = db[collection_name]
        collection.drop()
        collection = db[collection_name]

        for line in open('{}/db/backup/{}/{}'.format(get_project_root(),self._dbName, backup_file_name), 'r'):
            collection.insert_many(json.loads(line))




