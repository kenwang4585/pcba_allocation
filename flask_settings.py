from flask import Flask
from flask_wtf.file import FileField, FileRequired
from flask_wtf import FlaskForm
from wtforms.validators import Email, DataRequired,input_required
from wtforms import SubmitField, BooleanField, StringField,IntegerField,SelectField,PasswordField,TextAreaField,RadioField
import os
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
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
                             choices=[('cus_sat','Customer focus: Top priority->OSSD->FCD->Rev/non-rev->Qty'),
                                      ('max_rev','Revenue maximization: N/A')],
                             default='cus_sat',
                             validators=[DataRequired()])

    file_3a4 = FileField('Upload 3A4 file (.csv):',validators=[DataRequired()])
    file_supply=FileField('Upload supply file (.csv/.xlsx):',validators=[DataRequired()])
    submit_allocation=SubmitField(' Make Allocation ')

class FileDownloadForm(FlaskForm):
    fname_output=StringField('Download output data file - input filename to download:')
    submit_download_output=SubmitField('Download')

    fname_uploaded = StringField('Download uploaded data file - input filename to download:')
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
