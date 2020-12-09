from flask_settings import *
from sqlalchemy import create_engine
import os



def create_table():
    db.drop_all()  # if any update have to drop all first then can apply the changes
    print('delete done')

    db.create_all()
    print('create done')

    engine = create_engine(os.getenv('DB_URI'))
    print(engine.table_names())

if __name__=='__main__':
    create_table()