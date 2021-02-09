# created by Ken wang, Oct, 2020


# add below matplotlib.use('Agg') to avoid this error: Assertion failed: (NSViewIsCurrentlyBuildingLayerTreeForDisplay()
# != currentlyBuildingLayerTree), function NSViewSetCurrentlyBuildingLayerTreeForDisplay
import matplotlib
matplotlib.use('Agg')

import time
from werkzeug.utils import secure_filename
from flask import flash,send_from_directory,render_template, request,redirect,url_for
from flask_settings import *
from functions import *
from pull_supply_data_from_db import collect_scr_oh_transit_from_scdx_poc
from SCDx_API import collect_scr_oh_transit_from_scdx_prod
from settings import *
from sending_email import *
from db_add import add_user_log,add_email_data
from db_read import read_table
from db_update import update_email_data
from db_delete import delete_record
import traceback
import gc


@app.route('/allocation', methods=['GET', 'POST'])
def allocation_run():
    form = UploadForm()
    # as these email valiable are redefined below in email_to_only check, thus have to use global to define here in advance
    # otherwise can't be used. (as we create new vaiables with _ suffix thus no need to set global variable)
    # global backlog_dashboard_emails
    login_user = request.headers.get('Oidc-Claim-Sub')
    login_name=request.headers.get('Oidc-Claim-Fullname')
    if login_user==None:
        login_user = 'kwang2'
        login_name = 'Ken - debug'
        http_scheme = 'http'
    else:
        http_scheme = 'https'

    #print(request.headers)
    #print(request.url)

    if login_user!='' and login_user!='kwang2':
        add_user_log(user=login_user,location='Allocation',user_action='visit',summary='')

    if form.validate_on_submit():
        start_time = pd.Timestamp.now()
        print('start to run: {}'.format(start_time.strftime('%Y-%m-%d %H:%M')))
        log_msg = []
        log_msg.append('\n\n[' + login_user + '] ' + start_time.strftime('%Y-%m-%d %H:%M'))
        #log_msg.append('User info: ' + request.headers.get('User-agent'))

        # 通过条件判断及邮件赋值，开始执行任务
        pcba_site=form.org.data.strip().upper()
        bu=form.bu.data.strip().upper()
        bu_list=bu.split('/')
        f_supply= form.file_supply.data
        f_3a4 = form.file_3a4.data
        ranking_logic=form.ranking_logic.data # This is not shown on the UI - take the default value set

        # check input
        if pcba_site not in pcba_site_list:
            msg = "'{}' is not a PCBA org.".format(pcba_site)
            flash(msg, 'warning')
            print(login_user,msg)
            return redirect(url_for('allocation_run', _external=True, _scheme=http_scheme, viewarg1=1))
        print('{} is correct PCBA site.'.format(pcba_site))

        # check input
        if pcba_site not in f_supply.filename.upper():
            msg = "The supply file used is not a right one to do allocation for {}: {}.".format(pcba_site,f_supply.filename)
            flash(msg, 'warning')
            print(login_user,msg)
            summary = 'pcba_site ({}) and supply file({}) not matching!'.format(pcba_site,f_supply.filename)
            add_user_log(user=login_user, location='Allocation', user_action='Make allocation', summary=summary)

            return redirect(url_for('allocation_run', _external=True, _scheme=http_scheme, viewarg1=1))
        print('{} is correct supply file for this allocation.'.format(f_supply.filename))

        log_msg.append('PCBA_SITE: ' + pcba_site)
        log_msg.append('BU: ' + bu)

        # 检查文件格式
        ext_3a4 = os.path.splitext(f_3a4.filename)[1]
        ext_supply = os.path.splitext(f_supply.filename)[1]
        if ext_3a4 != '.csv':
            msg='3a4 file only accepts CSV formats here!'
            flash(msg, 'warning')
            summary = 'Wong 3a4 formats: {}'.format(ext_3a4)
            add_user_log(user=login_user, location='Allocation', user_action='Make allocation', summary=summary)

            return redirect(url_for('allocation_run', _external=True, _scheme=http_scheme, viewarg1=1))
        if ext_supply != '.xlsx':
            msg='The supply file must be xlsx format which you downloaded from the datasource tab!'
            flash(msg, 'warning')
            summary = 'Wong supply file formats: {}'.format(ext_supply)
            add_user_log(user=login_user, location='Allocation', user_action='Make allocation', summary=summary)

            return redirect(url_for('allocation_run', _external=True, _scheme=http_scheme, viewarg1=1))


        # 存储文件
        #file_path_3a4 = os.path.join(app.config['UPLOAD_PATH'],'3a4.csv')
        #file_path_supply = os.path.join(app.config['UPLOAD_PATH'],'supply.xlsx')
        file_path_3a4 = os.path.join(base_dir_upload, login_user+'_'+secure_filename(f_3a4.filename))
        file_path_supply = os.path.join(base_dir_upload, login_user+'_'+secure_filename(f_supply.filename))
        # save the files to server
        f_3a4.save(file_path_3a4)
        f_supply.save(file_path_supply)

        # check and store file size - after file is saved
        size_3a4=os.path.getsize(file_path_3a4)
        if size_3a4/1024>1:
            size_3a4=str(round(size_3a4/(1024*1024),1)) + 'Mb'
        else:
            size_3a4 = str(int(size_3a4 / 1024)) + 'Kb'
        size_supply=os.path.getsize(file_path_supply)
        if size_supply/1024>1:
            size_supply=str(round(size_supply/(1024*1024),1)) + 'Mb'
        else:
            size_supply = str(int(size_supply / 1024)) + 'Kb'
        log_msg.append('File 3a4: ' + f_3a4.filename + '(size: ' + size_3a4 + ')')
        log_msg.append('File supply: ' + f_supply.filename + '(size: ' + size_supply + ')')

        # check data format
        sheet_name_msg, msg_3a4, msg_3a4_option, msg_transit, msg_oh, msg_scr = check_input_file_format(file_path_3a4, file_path_supply,
                                                                        col_3a4_must_have, col_transit_must_have,
                                                                        col_oh_must_have, col_scr_must_have,
                                                                        'in-transit','df-oh','por')
        if sheet_name_msg!='':
            flash(sheet_name_msg,'warning')
        if msg_3a4!='':
            flash(msg_3a4,'warning')
        if msg_3a4_option!='':
            flash(msg_3a4_option,'warning')
        if msg_transit!='':
            flash(msg_transit,'warning')
        if msg_oh!='':
            flash(msg_oh,'warning')
        if msg_scr!='':
            flash(msg_scr,'warning')

        if sheet_name_msg!='' or msg_3a4!='' or msg_3a4_option!='' or msg_transit!='' or msg_oh!='' or msg_scr!='':
            print('error')
            return redirect(url_for('allocation_run',_external=True,_scheme=http_scheme,viewarg1=1))

       # 判断并定义ranking_col
        if ranking_logic == 'cus_sat':
            ranking_col = ranking_col_cust
        #elif ranking_logic == 'max_rev':
        #    ranking_col = ranking_col_rev

        try:
            # 读取数据
            df_3a4, df_oh, df_transit, df_scr, df_sourcing=read_data(file_path_3a4, file_path_supply)

            # limit BU from 3a4 and df_scr for allocation
            df_3a4, df_scr=limit_bu_from_3a4_and_scr(df_3a4,df_scr,bu_list)
            if df_3a4.shape[0] == 0:
                flash('The 3a4 data is empty, check data source, or check if you put in a BU that does not exist!', 'warning')
                return redirect(url_for('allocation_run',_external=True,_scheme=http_scheme,viewarg1=1))

            if df_scr.shape[0] == 0:
                flash('The SCR data is empty, check data source, or check if you put in a BU that does not exist!', 'warning')
                return redirect(url_for('allocation_run',_external=True,_scheme=http_scheme,viewarg1=1))

            #### main program
            output_filename=pcba_allocation_main_program(df_3a4, df_oh, df_transit, df_scr, df_sourcing, pcba_site, bu_list, ranking_col,login_user)
            flash('Allocation file created for downloading: {} '.format(output_filename), 'success')


            finish_time = pd.Timestamp.now()
            processing_time = round((finish_time - start_time).total_seconds() / 60, 1)
            log_msg.append('Processing time: ' + str(processing_time) + ' min')
            print('Finish run:',finish_time.strftime('%Y-%m-%d %H:%M'))

            # Write the log file
            summary='; '.join(log_msg)
            add_user_log(user=login_user,location='Allocation',user_action='Make allocation',summary=summary)

        except Exception as e:
            try:
                del df_scr, df_3a4, df_oh, df_transit, df_sourcing
                gc.collect()
            except:
                print('')

            traceback.print_exc()
            log_msg.append(str(e))
            flash('Error encountered: {}'.format(str(e)),'warning')
            #Write the log file
            summary = '; '.join(log_msg)
            add_user_log(user=login_user, location='Allocation', user_action='Make allocation', summary=summary)

            # write details to error_log.txt
            log_msg='\n'.join(log_msg)
            with open(os.path.join(base_dir_logs, 'error_log.txt'), 'a+') as file_object:
                file_object.write(log_msg)
            traceback.print_exc(file=open(os.path.join(base_dir_logs, 'error_log.txt'), 'a+'))

        # clear memory
        try:
            del df_scr, df_3a4, df_oh, df_transit, df_sourcing
            gc.collect()
        except:
            print('')

        return redirect(url_for('allocation_run',_external=True,_scheme=http_scheme,viewarg1=1))

    return render_template('allocation_run.html', form=form, user=login_name)

@app.route('/download', methods=['GET', 'POST'])
def allocation_download():
    form = FileDownloadForm()
    login_user = request.headers.get('Oidc-Claim-Sub')
    login_name = request.headers.get('Oidc-Claim-Fullname')

    if login_user == None:
        login_user = 'unknown'
        login_name = 'unknown'
        http_scheme = 'http'
    else:
        http_scheme = 'https'

    if login_user!='' and login_user!='kwang2':
        add_user_log(user=login_user, location='Download', user_action='Visit', summary='')

    # read the files
    output_record_hours=360
    upload_record_hours=240
    df_output=get_file_info_on_drive(base_dir_output,keep_hours=output_record_hours)
    df_upload=get_file_info_on_drive(base_dir_upload,keep_hours=upload_record_hours)

    if form.validate_on_submit():
        start_time=pd.Timestamp.now()
        log_msg = []
        log_msg.append('\n\n[' + login_user + '] ' + start_time.strftime('%Y-%m-%d %H:%M'))

        submit_detete_file=form.submit_delete.data
        submit_share_file=form.submit_share.data

        if submit_detete_file:
            fname = form.file_name_delete.data
            if login_user in fname:
                if fname in df_output.File_name.values:
                    f_path = df_output[df_output.File_name == fname].File_path.values[0]
                    os.remove(f_path)
                    msg = '{} removed!'.format(fname)
                    flash(msg, 'success')
                elif fname in df_upload.File_name.values:
                    f_path = df_upload[df_upload.File_name == fname].File_path.values[0]
                    os.remove(f_path)
                    msg = '{} removed!'.format(fname)
                    flash(msg, 'success')
                else:
                    add_user_log(user=login_user, location='Download', user_action='Delete file',
                                 summary='Fail: {}'.format(fname))
                    msg = 'Error file name: {}'.format(fname)
                    flash(msg, 'warning')
                    return redirect(url_for('allocation_download', _external=True, _scheme=http_scheme, viewarg1=1))
                add_user_log(user=login_user, location='Download', user_action='Delete file',
                             summary='Success: {}'.format(fname))
            else:
                msg='You are not allowed to delete this file created by others: {}'.format(fname)
                flash(msg,'warning')
                return redirect(url_for('allocation_download', _external=True, _scheme=http_scheme, viewarg1=1))

            return redirect(url_for('allocation_download', _external=True, _scheme=http_scheme, viewarg1=1))
        elif submit_share_file:
            fname_share = form.file_name_share.data.strip()
            if fname_share not in df_output.File_name.values:
                msg='This file you put in does not exist on server: {}'.format(fname_share)
                flash(msg,'warning')
                return redirect(url_for('allocation_download', _external=True, _scheme=http_scheme, viewarg1=1))

            if login_user not in fname_share:
                msg = 'You can only share file generated by yourself!'
                flash(msg, 'warning')
                return redirect(url_for('allocation_download', _external=True, _scheme=http_scheme, viewarg1=1))

            email_msg=form.email_msg.data

            email_msg = email_msg.format('Filename: ' + fname_share)

            try:
                send_allocation_result(email_msg, fname_share, login_user,login_name)

                add_user_log(user=login_user, location='Download', user_action='Share file',
                             summary='Success: {}'.format(fname_share))
                if login_user == 'unknown' or login_user == 'kwang2':
                    msg = 'Testing purpose - {} is sent to KW.'.format(fname_share)
                else:
                    msg = '{} is sent to the defined users by email.'.format(fname_share)

            except Exception as e:
                traceback.print_exc()
                msg='Error encountered in sharing result:{}'.format(e)
                flash(msg, 'warning')
                # Write the log file
                add_user_log(user=login_user, location='Download', user_action='Share file', summary=str(e))

                # write details to error_log.txt
                log_msg = '\n'.join(log_msg)
                with open(os.path.join(base_dir_logs, 'error_log.txt'), 'a+') as file_object:
                    file_object.write(log_msg)
                traceback.print_exc(file=open(os.path.join(base_dir_logs, 'error_log.txt'), 'a+'))

            flash(msg, 'success')
            return redirect(url_for('allocation_download', _external=True, _scheme=http_scheme, viewarg1=1))

    return render_template('allocation_download.html',form=form,
                           files_output=df_output.values,
                           output_record_days=int(output_record_hours/24),
                           files_uploaded=df_upload.values,
                           upload_record_days=int(upload_record_hours/24),
                           user=login_name)


# Below did now work out somehow - NOT USED
@app.route('/delete/<path:file_path>',methods=['POST'])
def delete_file(file_path):
    form=AdminForm()

    if form.validate_on_submit():
        os.remove(file_path)
        msg = 'File deleted!'
        flash(msg, 'success')
        return redirect(url_for("allocation_admin"))
    return render_template('allocation_admin.html',form=form)


@app.route('/o/<filename>',methods=['GET'])
def download_file_output(filename):
    f_path=base_dir_output
    print(f_path)
    login_user = request.headers.get('Oidc-Claim-Sub')
    if login_user != None:
        add_user_log(user=login_user, location='Download', user_action='Download file',
                 summary=filename)
    return send_from_directory(f_path, filename=filename, as_attachment=True)

@app.route('/u/<filename>',methods=['GET'])
def download_file_upload(filename):
    f_path=base_dir_upload
    print(f_path)
    login_user = request.headers.get('Oidc-Claim-Sub')
    if login_user != None:
        add_user_log(user=login_user, location='Download', user_action='Download file',
                 summary=filename)
    return send_from_directory(f_path, filename=filename, as_attachment=True)

@app.route('/s/<filename>',methods=['GET'])
def download_file_supply(filename):
    f_path=base_dir_supply
    print(f_path)
    login_user = request.headers.get('Oidc-Claim-Sub')
    if login_user != None:
        add_user_log(user=login_user, location='Download', user_action='Download file',
                 summary=filename)
    return send_from_directory(f_path, filename=filename, as_attachment=True)

@app.route('/l/<filename>',methods=['GET'])
def download_file_logs(filename):
    f_path=base_dir_logs
    print(f_path)
    login_user = request.headers.get('Oidc-Claim-Sub')
    if login_user != None:
        add_user_log(user=login_user, location='Download', user_action='Download file',
                 summary=filename)
    return send_from_directory(f_path, filename=filename, as_attachment=True)

@app.route('/email',methods=['GET','POST'])
def email_settings():
    form = EmailSettingForm()
    login_user=request.headers.get('Oidc-Claim-Sub')
    login_name = request.headers.get('Oidc-Claim-Fullname')
    if login_user == None:
        login_user = 'unknown'
        login_name = 'unknown'
        http_scheme = 'http'
    else:
        http_scheme = 'https'

    if login_user!='' and login_user!='kwang2':
        add_user_log(user=login_user, location='Email settings', user_action='Visit',
                 summary='')

    # read emails
    df_email_detail = read_table('email_settings')
    df_email_detail.sort_values(by=['Identity','BU'],inplace=True)

    if form.validate_on_submit():
        submit_add=form.submit_add.data
        submit_remove=form.submit_remove.data

        if submit_add:
            identity=form.identity.data
            pcba_org=form.pcba_org.data.strip().upper()
            bu=form.bu.data.strip().upper()
            pf = form.pf.data.strip().upper()
            email_to_add=form.email_to_add.data.strip().lower()

            if len(pcba_org)==0 or len(email_to_add)==0:
                msg='PCBA org and email are mandatory fields!'
                flash(msg,'warning')
                return redirect(url_for('email_settings', _external=True, _scheme=http_scheme, viewarg1=1))

            if email_to_add.count('@')>1:
                msg = 'Emails need to be added one by one only!'
                flash(msg, 'warning')
                return redirect(url_for('email_settings', _external=True, _scheme=http_scheme, viewarg1=1))

            if ',' in email_to_add or ';' in email_to_add:
                msg = 'Pls input correct email address without , or ;'
                flash(msg, 'warning')
                return redirect(url_for('email_settings', _external=True, _scheme=http_scheme, viewarg1=1))

            if email_to_add in df_email_detail.Email.values:
                update_email_data(identity, pcba_org, bu, email_to_add, login_user)
                msg='This email already exists! Data has been updated: {}'.format(email_to_add)
                flash(msg,'success')
                return redirect(url_for('email_settings', _external=True, _scheme=http_scheme, viewarg1=1))
            else:
                add_email_data(identity, pcba_org, bu, pf, email_to_add,login_user)
                msg='This email is added: {}'.format(email_to_add)
                flash(msg,'success')
                return redirect(url_for('email_settings', _external=True, _scheme=http_scheme, viewarg1=1))
        elif submit_remove:
            email_to_remove=form.email_to_remove.data.strip().lower()
            if len(email_to_remove)==0:
                msg='Put in the email to remove!'
                flash(msg,'warning')
                return redirect(url_for('email_settings', _external=True, _scheme=http_scheme, viewarg1=1))

            if email_to_remove in df_email_detail.Email.values:
                df_remove = df_email_detail[(df_email_detail.Email == email_to_remove)&(df_email_detail.Added_by==login_user)]
                if df_remove.shape[0]==0:
                    msg = "You can't remove emails added by others!"
                    flash(msg, 'warning')
                    return redirect(url_for('email_settings', _external=True, _scheme=http_scheme, viewarg1=1))

                add_user_log(user=login_user, location='Email settings', user_action='Remove email',
                             summary=email_to_remove)

                id_list=df_email_detail[df_email_detail.Email==email_to_remove].id.to_list()
                delete_record('email_settings', id_list)
                msg='This email has been removed: {}'.format(email_to_remove)
                flash(msg,'success')
                return redirect(url_for('email_settings', _external=True, _scheme=http_scheme, viewarg1=1))
            else:
                msg='This email does not exist: {}'.format(email_to_remove)
                flash(msg,'warning')
                return redirect(url_for('email_settings', _external=True, _scheme=http_scheme, viewarg1=1))

    return render_template('allocation_email_settings.html', form=form,
                           email_details=df_email_detail.values,
                           user=login_name)


@app.route('/admin', methods=['GET','POST'])
def allocation_admin():
    form = AdminForm()
    login_user=request.headers.get('Oidc-Claim-Sub')
    login_name = request.headers.get('Oidc-Claim-Fullname')
    if login_user == None:
        login_user = 'unknown'
        login_name = 'unknown'
        http_scheme = 'http'
    else:
        http_scheme = 'https'

    if login_user!='unknown' and login_user!='kwang2':
        return redirect(url_for('allocation_run',_external=True,_scheme=http_scheme,viewarg1=1))
    if login_user != 'unknown' and login_user != 'kwang2':
        add_user_log(user=login_user, location='Admin', user_action='Visit', summary='why this happens')

    # get file info
    df_output=get_file_info_on_drive(base_dir_output,keep_hours=360)
    df_upload=get_file_info_on_drive(base_dir_upload,keep_hours=240)
    df_supply=get_file_info_on_drive(base_dir_supply,keep_hours=240)
    df_logs=get_file_info_on_drive(base_dir_logs,keep_hours=10000)

    # read logs
    df_log_detail = read_table('user_log')
    df_log_detail.sort_values(by=['DATE','TIME'],ascending=False,inplace=True)

    if form.validate_on_submit():
        fname=form.file_name.data
        if fname in df_output.File_name.values:
            f_path=df_output[df_output.File_name==fname].File_path.values[0]
            os.remove(f_path)
            msg='{} removed!'.format(fname)
            flash(msg,'success')
        elif fname in df_upload.File_name.values:
            f_path = df_upload[df_upload.File_name == fname].File_path.values[0]
            os.remove(f_path)
            msg = '{} removed!'.format(fname)
            flash(msg, 'success')
        elif fname in df_supply.File_name.values:
            f_path = df_supply[df_supply.File_name == fname].File_path.values[0]
            os.remove(f_path)
            msg = '{} removed!'.format(fname)
            flash(msg, 'success')
        else:
            msg = 'Error file name! Ensure it is in output folder,upload folder or supply folder: {}'.format(fname)
            flash(msg, 'warning')
            return redirect(url_for('allocation_admin',_external=True,_scheme=http_scheme,viewarg1=1))

    return render_template('allocation_admin.html',form=form,
                           files_output=df_output.values,
                           files_uploaded=df_upload.values,
                           files_supply=df_supply.values,
                           files_log=df_logs.values,
                           log_details=df_log_detail.values,
                           user=login_name)


@app.route('/document', methods=['GET'])
def document():
    login_user=request.headers.get('Oidc-Claim-Sub')
    login_name = request.headers.get('Oidc-Claim-Fullname')
    if login_user == None:
        login_user = 'unknown'
        login_name = 'unknown'
        http_scheme = 'http'
    else:
        http_scheme = 'https'

    if login_user!='unknown' and login_user!='kwang2':
        return redirect(url_for('allocation_run',_external=True,_scheme=http_scheme,viewarg1=1))

    if login_user != 'unknown' and login_user != 'kwang2':
        add_user_log(user=login_user, location='Document', user_action='Visit', summary='why this happens')

    return render_template('allocation_document.html',
                           user=login_name)

# Below is a dummy one
@app.route('/config',methods=['GET','POST'])
def config():
    form = ConfigForm()
    return render_template('config_control_panel_new.html', form=form)

@app.route('/time')
def index():
    import datetime
    dt = datetime.datetime.utcnow()
    print(request)

    return render_template('test.html',
        dt=dt)

@app.route('/user-guide')
def user_guide():
    login_user = request.headers.get('Oidc-Claim-Sub')
    login_name = request.headers.get('Oidc-Claim-Fullname')
    if login_user == None:
        login_user = 'unknown'
        login_name = 'unknown'

    if login_user != '' and login_user != 'kwang2':
        add_user_log(user=login_user, location='User-guide', user_action='Visit', summary='')

    return render_template('allocation_userguide.html',user=login_name)

@app.route('/datasource',methods=['GET','POST'])
def allocation_datasource():
    form=DataSourceForm()
    login_user = request.headers.get('Oidc-Claim-Sub')
    login_name = request.headers.get('Oidc-Claim-Fullname')
    if login_user == None:
        login_user = 'unknown'
        login_name = 'unknown'
        http_scheme = 'http'
    else:
        http_scheme = 'https'

    if login_user != '' and login_user != 'kwang2':
        add_user_log(user=login_user, location='User-guide', user_action='Visit', summary='')

    if form.validate_on_submit():
        submit_download_scdx_poc=form.submit_download_supply_poc.data
        submit_download_scdx_prod = form.submit_download_supply_prod.data

        if submit_download_scdx_poc:
            pcba_site_poc=form.pcba_site_poc.data.strip().upper()

            log_msg = []
            log_msg.append('\n\n[Download SCDx-POC] - ' + pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'))

            now = pd.Timestamp.now()
            f_path=base_dir_supply
            fname=pcba_site_poc + ' scr_oh_intransit(scdx-poc) ' + now.strftime('%m-%d %Hh%Mm ') + login_user + '.xlsx'
            log_msg.append('Download supply from SCDx-POC')

            if pcba_site_poc not in pcba_site_list:
                msg = "'{}' is not a PCBA org.".format(pcba_site_poc)
                flash(msg, 'warning')
                return redirect(url_for('allocation_datasource',_external=True,_scheme=http_scheme))
            try:
                df_scr, df_oh, df_intransit, df_sourcing = collect_scr_oh_transit_from_scdx_poc(pcba_site_poc)
                data_to_write = {'por': df_scr,
                                 'df-oh': df_oh,
                                 'in-transit': df_intransit,
                                 'sourcing-rule': df_sourcing}

                write_data_to_excel(os.path.join(f_path, fname), data_to_write)
                add_user_log(user=login_user, location='Datasource', user_action='Download SCDx-POC', summary='Success: ' + pcba_site_poc)

                return send_from_directory(f_path, filename=fname, as_attachment=True)
            except Exception as e:
                msg = 'Error downloading supply data from SCDx-POC! ' + str(e)
                flash(msg, 'warning')
                add_user_log(user=login_user, location='Datasource', user_action='Download SCDx-POC', summary='Error: [' + pcba_site_poc + '] ' + str(e))
                return redirect(url_for('allocation_datasource', _external=True, _scheme=http_scheme, viewarg1=1))
        elif submit_download_scdx_prod:
            pcba_site_prod=form.pcba_site_prod.data.strip().upper()

            log_msg = []
            log_msg.append('\n\n[Download SCDx-Production] - ' + pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'))

            now = pd.Timestamp.now()
            f_path=base_dir_supply
            fname=pcba_site_prod + ' scr_oh_intransit(scdx-prod) ' + now.strftime('%m-%d %Hh%Mm ') + login_user + '.xlsx'
            log_msg.append('Download supply from SCDx-POC')

            if pcba_site_prod not in pcba_site_list:
                msg = "'{}' is not a PCBA org.".format(pcba_site_prod)
                flash(msg, 'warning')
                return redirect(url_for('allocation_datasource',_external=True,_scheme=http_scheme))
            try:
                df_scr, df_oh, df_intransit, df_sourcing = collect_scr_oh_transit_from_scdx_prod(pcba_site_prod,'*')
                data_to_write = {'por': df_scr,
                                 'df-oh': df_oh,
                                 'in-transit': df_intransit,
                                 'sourcing-rule': df_sourcing}

                write_data_to_excel(os.path.join(f_path, fname), data_to_write)
                add_user_log(user=login_user, location='Datasource', user_action='Download SCDx-Prod', summary='Success: ' + pcba_site_prod)

                return send_from_directory(f_path, filename=fname, as_attachment=True)
            except Exception as e:
                msg = 'Error downloading supply data from SCDx-Prod! ' + str(e)
                flash(msg, 'warning')
                add_user_log(user=login_user, location='Datasource', user_action='Download SCDx-Prod', summary='Error: [' + pcba_site_prod + '] ' + str(e))
                return redirect(url_for('allocation_datasource', _external=True, _scheme=http_scheme, viewarg1=1))

    return render_template('allocation_datasource.html',user=login_name,form=form)
