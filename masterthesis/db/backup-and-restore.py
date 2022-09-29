from masterthesis.db.dbconnection import DbConnection
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', required=True, help='Backup and restore mode. Options: backup, restore, importcsv')
    parser.add_argument('--source', required=False, help='Needed in restore mode to determine backup file name in backup/<DBNAME> folder')

    args = parser.parse_args()
    if(args.mode == 'backup'):
        print('starting backup')
        DbConnection().backup_database()
        print('backup done')
    elif(args.mode == 'restore'):
        if (args.source is None):
            exit('Please define source!')
        print('starting bkp restore')
        DbConnection().restore_database_from_backup(args.source)
        print('backup restored')
    else:
        exit('invalid mode used.')