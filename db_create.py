from flask_settings import *
from sqlalchemy import create_engine
import os



def create_table():
    a=input('Do you want to drop all tables?(YeS/no)')
    if a=='YeS':
        db.drop_all()  # if any update have to drop all first then can apply the changes
        print('All tables deleted!!!')

    db.create_all()
    print('Tables created:')

    engine = create_engine(os.getenv('DB_URI'))
    print(engine.table_names())

if __name__=='__main__':
    create_table()