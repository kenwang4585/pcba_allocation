from flask_settings import *
from sqlalchemy import create_engine
import os

def create_table():
    engine = create_engine('sqlite:///' + base_dir_db + os.getenv('DB_URI'))
    print('Existing tables:',engine.table_names())

    db.create_all()

    print('Current tables:',engine.table_names())

if __name__=='__main__':
    create_table()