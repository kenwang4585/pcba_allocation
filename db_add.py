# import cryptography

import pandas as pd
from flask_settings import *


def add_log_summary(user='', location='', user_action='',summary=''):
    '''
    Add the user log to db
    '''

    log = AllocationUserLog(USER_NAME=user,
                    DATE=pd.Timestamp.now().date(),
                    TIME=pd.Timestamp.now().strftime('%H:%M:%S'),
                    LOCATION=location,
                    USER_ACTION=user_action,
                    SUMMARY=summary)


    db.session.add(log)  # can also use add_all() for multiple adding at one time
    db.session.commit()
    #print('User log added')

def add_email_data(pcba_org, bu, email,login_user):
    '''
    Add the user log to db
    '''

    log = AllocationSubscription(PCBA_Org=pcba_org,
                  BU=bu,
                  Email=email,
                  Added_by=login_user,
                  Added_on=pd.Timestamp.now().date(),)

    db.session.add(log)  # can also use add_all() for multiple adding at one time
    db.session.commit()
    #print('User log added')


def add_exceptional_priority_data_from_template(df,login_user):
    '''
    Add exceptional data
    '''
    df.fillna('',inplace=True)

    #df=df[['ORGANIZATION_CODE', 'BUSINESS_UNIT', 'PO_NUMBER','LINE_CREATION_DATE', 'OPTION_NUMBER',
    #   'PRODUCT_ID', 'ORDERED_QUANTITY','LABEL','COMMENTS','REPORT_DATE', 'UPLOAD_BY','ML_COLLECTED']]

    df_data = df.values

    db.session.bulk_insert_mappings(
                                    AllocationExceptionPriority,
                                    [dict(
                                        SO_SS=row[0],
                                        ORG=row[1],
                                        BU=row[2],
                                        Ranking=row[3],
                                        Comments=row[4],
                                        Added_by=login_user,
                                        Added_on=pd.Timestamp.now().date()
                                        )
                                     for row in df_data]
                                    )
    db.session.commit()
    print('data added')

def add_exceptional_sourcing_split_data_from_template(df,login_user):
    '''
    Add exceptional data
    '''
    df.fillna('',inplace=True)

    #df=df[['ORGANIZATION_CODE', 'BUSINESS_UNIT', 'PO_NUMBER','LINE_CREATION_DATE', 'OPTION_NUMBER',
    #   'PRODUCT_ID', 'ORDERED_QUANTITY','LABEL','COMMENTS','REPORT_DATE', 'UPLOAD_BY','ML_COLLECTED']]

    df_data = df.values

    db.session.bulk_insert_mappings(
                                    AllocationExceptionSourcingSplit,
                                    [dict(
                                        DF_site=row[0],
                                        PCBA_site=row[1],
                                        BU=row[2],
                                        PF=row[3],
                                        TAN=row[4],
                                        Split=row[5],
                                        Comments=row[6],
                                        Added_by=login_user,
                                        Added_on=pd.Timestamp.now().date()
                                        )
                                     for row in df_data]
                                    )
    db.session.commit()
    print('data added')

def add_tan_grouping_data_from_template(df,login_user):
    '''
    Add tan grouping data
    '''
    df.fillna('',inplace=True)

    #df=df[['ORGANIZATION_CODE', 'BUSINESS_UNIT', 'PO_NUMBER','LINE_CREATION_DATE', 'OPTION_NUMBER',
    #   'PRODUCT_ID', 'ORDERED_QUANTITY','LABEL','COMMENTS','REPORT_DATE', 'UPLOAD_BY','ML_COLLECTED']]

    df_data = df.values

    db.session.bulk_insert_mappings(
                                    AllocationTanGrouping,
                                    [dict(
                                        Group_name=row[0],
                                        TAN=row[1],
                                        DF=row[2],
                                        Comments=row[3],
                                        Added_by=login_user,
                                        Added_on=pd.Timestamp.now().date()
                                        )
                                     for row in df_data]
                                    )
    db.session.commit()
    print('data added')

def roll_back():
    try:
        db.session.commit()
    except:
        db.session.rollback()

def from_file_add_email_data(df):
    '''
    Add exceptional data
    '''
    df.fillna('',inplace=True)

    df_data = df.values

    print(df_data)

    db.session.bulk_insert_mappings(
                                    AllocationSubscription,
                                    [dict(
                                        Email=row[0].replace('\xa0', '').replace(' ', ''),
                                        PCBA_Org=row[1].replace('\xa0','').replace(' ',''),
                                        BU=row[2].replace('\xa0','').replace(' ',''),

                                        Added_by=row[3].replace('\xa0','').replace(' ',''),
                                        Added_on=row[4].replace('\xa0','').replace(' ','')
                                        )
                                     for row in df_data]
                                    )
    db.session.commit()
    print('data added')


def from_file_add_exceptional_priority_data_from_template(df):
    '''
    Add exceptional data
    '''
    df.fillna('',inplace=True)

    #df=df[['ORGANIZATION_CODE', 'BUSINESS_UNIT', 'PO_NUMBER','LINE_CREATION_DATE', 'OPTION_NUMBER',
    #   'PRODUCT_ID', 'ORDERED_QUANTITY','LABEL','COMMENTS','REPORT_DATE', 'UPLOAD_BY','ML_COLLECTED']]

    df_data = df.values
    print(df_data)

    db.session.bulk_insert_mappings(
                                    AllocationExceptionPriority,
                                    [dict(
                                        SO_SS=row[0].replace('\xa0',''),
                                        ORG=row[1].replace('\xa0',''),
                                        BU=row[2].replace('\xa0',''),
                                        Ranking=float(row[3]),
                                        Comments=row[4].replace('\xa0',''),
                                        Added_by=row[5].replace('\xa0',''),
                                        Added_on=row[6].replace('\xa0',''),
                                        )
                                     for row in df_data]
                                    )
    db.session.commit()
    print('data added')

def from_file_add_exceptional_sourcing_split_data_from_template(df):
    '''
    Add exceptional data
    '''
    df.fillna('',inplace=True)

    #df=df[['ORGANIZATION_CODE', 'BUSINESS_UNIT', 'PO_NUMBER','LINE_CREATION_DATE', 'OPTION_NUMBER',
    #   'PRODUCT_ID', 'ORDERED_QUANTITY','LABEL','COMMENTS','REPORT_DATE', 'UPLOAD_BY','ML_COLLECTED']]

    df_data = df.values

    db.session.bulk_insert_mappings(
                                    AllocationExceptionSourcingSplit,
                                    [dict(
                                        DF_site=row[0].replace('\xa0',''),
                                        PCBA_site=row[1].replace('\xa0',''),
                                        BU=row[2].replace('\xa0',''),
                                        PF=row[3].replace('\xa0',''),
                                        TAN=row[4].replace('\xa0',''),
                                        Split=row[5].replace('\xa0',''),
                                        Comments=row[6].replace('\xa0',''),
                                        Added_by=row[7].replace('\xa0',''),
                                        Added_on=row[8].replace('\xa0',''),
                                        )
                                     for row in df_data]
                                    )
    db.session.commit()
    print('data added')

def from_file_add_tan_grouping_data_from_template(df):
    '''
    Add tan grouping data
    '''
    df.fillna('',inplace=True)

    #df=df[['ORGANIZATION_CODE', 'BUSINESS_UNIT', 'PO_NUMBER','LINE_CREATION_DATE', 'OPTION_NUMBER',
    #   'PRODUCT_ID', 'ORDERED_QUANTITY','LABEL','COMMENTS','REPORT_DATE', 'UPLOAD_BY','ML_COLLECTED']]

    df_data = df.values

    db.session.bulk_insert_mappings(
                                    AllocationTanGrouping,
                                    [dict(
                                        Group_name=row[0].replace('\xa0',''),
                                        TAN=row[1].replace('\xa0',''),
                                        DF=row[2].replace('\xa0',''),
                                        Comments=row[3].replace('\xa0',''),
                                        Added_by=row[4].replace('\xa0',''),
                                        Added_on=row[5].replace('\xa0',''),
                                        )
                                     for row in df_data]
                                    )
    db.session.commit()
    print('data added')



if __name__ == '__main__':
    #add_user_log(user='kwang2', location='Admin', user_action='Visit',
    #             summary='Warning')

    df_priority=pd.read_excel('/Users/wangken/downloads/Exceptional priority SS.xlsx')
    df_split=pd.read_excel('/Users/wangken/downloads/exceptional split.xlsx')
    #df_grouping=pd.read_excel('/Users/wangken/downloads/grouping.xlsx')
    df_email=pd.read_excel('/Users/wangken/downloads/subscription.xlsx')

    #print(df_email)

    #from_file_add_tan_grouping_data_from_template(df_grouping)
    #from_file_add_exceptional_sourcing_split_data_from_template(df_split)
    #from_file_add_exceptional_priority_data_from_template(df_priority)
    #from_file_add_email_data(df_email)
    roll_back()

