from flask import Flask
from flask_wtf.file import FileField, FileRequired,FileAllowed
from flask_wtf import FlaskForm
from wtforms.validators import Email, DataRequired,AnyOf
from wtforms import SubmitField, BooleanField, StringField,IntegerField,SelectField,PasswordField,TextAreaField,RadioField
import os
from flask_sqlalchemy import SQLAlchemy
from werkzeug.middleware.proxy_fix import ProxyFix
from settings import base_dir_db
from sqlalchemy import create_engine

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app) # usethis inorder for request.remote_addr to show the real user IP

app.secret_key = os.getenv('SECRET_KEY', 'secret string')
#app.config['UPLOAD_PATH'] = os.path.join(app.root_path, 'upload_file')

#app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + base_dir_db + os.getenv('DB_URI')
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DB_URI')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
#app.config['MAX_CONTENT_LENGTH']=150*1024*1024

db = SQLAlchemy(app)

# Flask forms
class RunAllocationForm(FlaskForm):
    # 创建各种表单对象
    org=StringField('PCBA Org name:',validators=[DataRequired()],render_kw={'placeholder':'e.g. FOL'})
    bu=StringField("Business units: ",render_kw={'placeholder':'e.g. ERBU/SRGBU; leave blank for all BU'})

    ranking_logic=SelectField('Select order ranking logic:',
                             choices=[('cus_sat','Customer focus: Top priority->OSSD->FCD->Qty->Rev/non-rev'),
                                      ('max_rev','Revenue maximization: N/A')],
                             default='cus_sat',
                             validators=[DataRequired()])
    description = StringField('Description:', render_kw={'placeholder': 'Short description show in output file name, e.g., test'})

    file_3a4 = FileField('Upload 3A4 file (.csv):',validators=[FileRequired()])
    file_supply=FileField('Upload supply file (.xlsx):')


    submit_allocation=SubmitField(' Make Allocation ')



class ResultForm(FlaskForm):
    file_name_delete=StringField('File to delete',render_kw={'placeholder':'put in filename here'}) # for deleting filename created by user self
    submit_delete=SubmitField('   Delete   ')

    file_name_share=StringField('File to share',render_kw={'placeholder':'put in filename here'}) # share by email
    email_msg=TextAreaField(default='Hello, pls find attached the PCBA allocation result.')
    submit_share=SubmitField('   Share    ')


class AdminForm(FlaskForm):
    file_name=StringField(validators=[DataRequired()])
    submit_delete=SubmitField('Delete')

class SubscriptionForm(FlaskForm):
    pcba_org=StringField("*PCBA Org(multiple org separate by '/'):")
    bu=StringField("BU(multiple BU separate by '/')")
    email_to_add=StringField('*Email address:')
    submit_add=SubmitField('   Add email   ')

class ScdxAPIForm(FlaskForm):
    pcba_site_poc = StringField('Download supply data file from SCDx-POC:',
                                render_kw={'placeholder':'put in PCBA org name here'})
    submit_download_supply_poc = SubmitField('Download')  # download from db

    pcba_site_prod = StringField('Download supply data file from SCDx-production:',
                            render_kw={'placeholder':'put in PCBA org name here'})
    submit_download_supply_prod = SubmitField('Download')  # download from db

class ExceptionalPriorityForm(FlaskForm):
    upload_option = RadioField('Upload option:',
                               choices=[('replace_all', 'Remove all under my name and replace with template'),
                                        ('add_update', 'Add new or update existing based on template')],
                               default='add_update',
                               validators=[DataRequired()]
                               )
    file_upload_template = FileField('Upload template (.xlsx):')
    submit_upload_template = SubmitField('Upload')

    submit_show_me = SubmitField('Show my data')
    submit_show_all = SubmitField('Show all data')
    submit_download_me = SubmitField('Download my data')
    submit_download_all = SubmitField('Download all data')

class ExceptionalSourcingSplitForm(FlaskForm):
    file_upload_template = FileField('Upload template (.xlsx):')
    submit_upload_template = SubmitField('Upload')

    submit_show_me = SubmitField('Show my data')
    submit_show_all = SubmitField('Show all data')
    submit_download = SubmitField('Download my data')

class TanGroupingForm(FlaskForm):
    file_upload_template = FileField('Upload template (.xlsx):')
    submit_upload_template = SubmitField('Upload')

    submit_show_me = SubmitField('Show my data')
    submit_show_all = SubmitField('Show all data')
    submit_download = SubmitField('Download my data')

class MpqForm(FlaskForm):
    upload_option = RadioField('Upload option:',
                                choices=[('replace_all', 'Remove all under my name and replace with template'),
                                         ('add_update', 'Add new or update existing based on template')],
                                default='add_update',
                                validators=[DataRequired()]
                                )
    file_upload_template = FileField('Upload template (.xlsx):')
    submit_upload_template = SubmitField('Upload')

    submit_show_me = SubmitField('Show my data')
    submit_show_all = SubmitField('Show all data')
    submit_download = SubmitField('Download my data')


# Database tables

class AllocationUserLog(db.Model):
    '''
    User logs db table
    '''
    id=db.Column(db.Integer,primary_key=True)
    USER_NAME=db.Column(db.String(10))
    DATE=db.Column(db.Date)
    TIME=db.Column(db.String(8))
    LOCATION=db.Column(db.String(25))
    USER_ACTION=db.Column(db.String(35))
    SUMMARY=db.Column(db.Text)

class AllocationSubscription(db.Model):
    '''
    Email setting db table
    '''
    id=db.Column(db.Integer,primary_key=True)
    Email=db.Column(db.String(30))
    PCBA_Org=db.Column(db.String(30))
    BU=db.Column(db.String(30))
    Added_by=db.Column(db.String(10))
    Added_on=db.Column(db.Date)

class AllocationExceptionPriority(db.Model):
    '''
    Exceptional priority SS db table. This is also set under both the PCBA allocation tool and DFPM auto tools as it's commonly used.
    '''
    id=db.Column(db.Integer,primary_key=True)
    SO_SS=db.Column(db.String(30))
    ORG=db.Column(db.String(3))
    BU=db.Column(db.String(12))
    Ranking=db.Column(db.FLOAT())
    Comments=db.Column(db.String(100))
    Added_by=db.Column(db.String(10))
    Added_on=db.Column(db.Date)

class AllocationExceptionSourcingSplit(db.Model):
    '''
    Exceptional Sourcing split db table
    '''
    id=db.Column(db.Integer,primary_key=True)
    DF_site=db.Column(db.String(3))
    PCBA_site=db.Column(db.String(3))
    BU=db.Column(db.String(12))
    PF=db.Column(db.String(12))
    TAN=db.Column(db.String(14))
    Split=db.Column(db.Integer())
    Comments=db.Column(db.String(100))
    Added_by=db.Column(db.String(10))
    Added_on=db.Column(db.Date)

class AllocationTanGrouping(db.Model):
    '''
    Tan grouping db table
    '''
    id=db.Column(db.Integer,primary_key=True)
    Group_name=db.Column(db.String(80))
    TAN=db.Column(db.String(14))
    DF=db.Column(db.String(27))
    Comments=db.Column(db.String(100))
    Added_by=db.Column(db.String(10))
    Added_on=db.Column(db.Date)

class Mpq(db.Model):
    """
    For MPQ data
    """
    id = db.Column(db.Integer, primary_key=True)
    PCBA_ORG = db.Column(db.String(3))
    TAN = db.Column(db.String(14))
    MPQ = db.Column(db.Integer)
    Comments = db.Column(db.String(100))
    Added_by = db.Column(db.String(15))
    Added_on = db.Column(db.Date)
