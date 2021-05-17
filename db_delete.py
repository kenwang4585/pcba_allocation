from flask_settings import db

def delete_table_data(table_name,id_list):
    '''
    delete a list of records from a defined table
    '''
    # first change the table name to table class
    table_name=table_name.split('_')
    table_name=[x.title() for x in table_name]
    table_name=''.join(table_name)
    table_class=eval(table_name)

    for id in id_list:
        record=table_class.query.get(id)

        db.session.delete(record)
        db.session.commit()


if __name__=='__main__':
    delete_email('email_settings', [''])
    #pass
