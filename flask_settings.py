from flask import Flask
from flask_wtf.file import FileField, FileRequired,FileAllowed
from flask_wtf import FlaskForm
from wtforms.validators import Email, DataRequired,input_required
from wtforms import SubmitField, BooleanField, StringField,IntegerField,SelectField,PasswordField,TextAreaField,RadioField
import os
from flask_sqlalchemy import SQLAlchemy
from werkzeug.middleware.proxy_fix import ProxyFix

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app) # usethis inorder for request.remote_addr to show the real user IP

app.secret_key = os.getenv('SECRET_KEY', 'secret string')
#app.config['UPLOAD_PATH'] = os.path.join(app.root_path, 'upload_file')

app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DB_URI') #os.getenv('DB_URI')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

# Flask forms
class UploadForm(FlaskForm):
    # 创建各种表单对象
    org=StringField('PCBA Org code (e.g. FOL):',validators=[DataRequired()],default='FOL')
    bu=StringField('Business units (e.g. PABU/ERBU; leave blank for all BU): ',default='')

    ranking_logic=SelectField('Select order ranking logic:',
                             choices=[('cus_sat','Customer focus: Top priority->OSSD->FCD->Qty->Rev/non-rev'),
                                      ('max_rev','Revenue maximization: N/A')],
                             default='cus_sat',
                             validators=[DataRequired()])

    file_3a4 = FileField('Upload 3A4 file (.csv):',validators=[FileRequired(),FileAllowed(['csv'],'Only CSV file is allowed!')])
    file_supply=FileField('Upload supply file (.xlsx):',validators=[FileRequired(),FileAllowed(['xlsx'],'Oly XLSX file is allowed!')])
    use_blg_server=BooleanField('Use latest backlog loaded onto server')
    use_supply_server = BooleanField('Use latest supply loaded onto server')
    submit_allocation=SubmitField(' Make Allocation ')
    email_option=BooleanField('Notify users by email')


class FileDownloadForm(FlaskForm):
    fname_download_supply = StringField('Download supply data file from SCDx:')
    submit_download_supply=SubmitField('Download') # download from db

    fname_output=StringField('Download the allocation data file:',default='paste the file name here and click the download button')
    submit_download_output=SubmitField('Download')

    fname_uploaded = StringField('Download user input data file:',default='paste the file name here and click the download button')
    submit_download_uploaded=SubmitField('Download')


# Database tables
class UserLog(db.Model):
    '''
    User logs db table
    '''
    id=db.Column(db.Integer,primary_key=True)
    USER_NAME=db.Column(db.String(10))
    DATE=db.Column(db.Date)
    START_TIME=db.Column(db.String(8))
    FINISH_TIME=db.Column(db.String(8))
    TOTAL_TIME=db.Column(db.Float)
    USER_SELECTION=db.Column(db.Text)
    EMAIL_OPTION=db.Column(db.String(10))
    PROGRAM_LOG=db.Column(db.Text)
    PROCESSING_TIME_DETAIL=db.Column(db.Text)
    ERROR_LOG=db.Column(db.Text)
