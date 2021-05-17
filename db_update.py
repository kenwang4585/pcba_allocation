from flask_settings import *
import pandas as pd

def update_email_data(pcba_org,bu,email,login_user):
    '''
    Update table based on user input
    '''
    records = AllocationSubscription.query.filter_by(Email=email).all()
    for record in records:
        record.PCBA_Org=pcba_org
        record.BU=bu
        record.Email=email
        record.Added_by=login_user
        record.Added_on=pd.Timestamp.now().date()

        db.session.commit()

if __name__=='__main__':

    #update_ml_collected_label('packed_order_with_new_pid')
    start_time=pd.Timestamp.now()
    from db_read import read_table
    df=read_table('packed_order_with_new_pid','ML_COLLECTED IS NULL')
    print(df.shape)
    update_ml_collected_label_batch('packed_order_with_new_pid', df)
    finish_time=pd.Timestamp.now()
    processing_time = round((finish_time - start_time).total_seconds() / 60, 1)
    print(processing_time)
