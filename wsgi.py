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
from SCDx_POC import collect_scr_oh_transit_from_scdx_poc
from SCDx_PROD_API import collect_scr_oh_transit_from_scdx_prod
from settings import *
from sending_email import *
from db_add import add_user_log,add_email_data
from db_read import read_table
from db_update import update_email_data
from db_delete import delete_email
import traceback
import gc


@app.route('/allocation', methods=['GET', 'POST'])
def allocation_run():
    form = RunAllocationForm()
    # as these email valiable are redefined below in email_to_only check, thus have to use global to define here in advance
    # otherwise can't be used. (as we create new vaiables with _ suffix thus no need to set global variable)
    # global backlog_dashboard_emails
    login_user = request.headers.get('Oidc-Claim-Sub')
    login_name=request.headers.get('Oidc-Claim-Fullname')
    if login_user==None:
        login_user = 'unknown'
        login_name = 'unknown'
        http_scheme = 'http'
    else:
        http_scheme = 'https'

    #print(request.headers)
    #print(request.url)

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
        log_msg.append('PCBA_SITE: ' + pcba_site)
        log_msg.append('BU: ' + bu)

        if f_supply==None:
            msg = 'Pls upload the supply file! Reading directly from SCDx target to live from 5/5.'
            flash(msg, 'warning')
            return render_template('allocation_run.html', form=form, user=login_name)
        else:
            # check input
            if pcba_site not in f_supply.filename.upper():
                msg = "The supply file used is not a right one to do allocation for {}: {}.".format(pcba_site,f_supply.filename)
                flash(msg, 'warning')
                print(login_user,msg)
                summary = 'pcba_site ({}) and supply file({}) not matching!'.format(pcba_site,f_supply.filename)
                add_user_log(user=login_user, location='Allocation', user_action='Make allocation', summary=summary)

                return render_template('allocation_run.html', form=form, user=login_name)
            # save supply file
            file_path_supply = os.path.join(base_dir_upload, login_user + '_' + secure_filename(f_supply.filename))
            f_supply.save(file_path_supply)


        # 检查文件格式
        ext_3a4 = os.path.splitext(f_3a4.filename)[1]
        if ext_3a4 != '.csv':
            msg='3a4 file only accepts CSV formats here!'
            flash(msg, 'warning')
            summary = 'Wong 3a4 formats: {}'.format(ext_3a4)
            add_user_log(user=login_user, location='Allocation', user_action='Make allocation', summary=summary)

            return render_template('allocation_run.html', form=form, user=login_name)

        # 存储3a4
        file_path_3a4 = os.path.join(base_dir_upload, login_user+'_'+secure_filename(f_3a4.filename))
        f_3a4.save(file_path_3a4)

        # check and store file size - after file is saved
        size_3a4=os.path.getsize(file_path_3a4)
        if size_3a4/1024>1:
            size_3a4=str(round(size_3a4/(1024*1024),1)) + 'Mb'
        else:
            size_3a4 = str(int(size_3a4 / 1024)) + 'Kb'
        log_msg.append('File 3a4: ' + f_3a4.filename + '(size: ' + size_3a4 + ')')

        if f_supply!=None:
            size_supply=os.path.getsize(file_path_supply)
            if size_supply/1024>1:
                size_supply=str(round(size_supply/(1024*1024),1)) + 'Mb'
            else:
                size_supply = str(int(size_supply / 1024)) + 'Kb'
            log_msg.append('File supply: ' + f_supply.filename + '(size: ' + size_supply + ')')
        else:
            log_msg.append('File supply: directly download through API')

        # check data format
        msg_3a4, msg_3a4_option = check_3a4_input_file_format(file_path_3a4, col_3a4_must_have)
        if msg_3a4!='':
            flash(msg_3a4,'warning')
            return render_template('allocation_run.html', form=form, user=login_name)
        if msg_3a4_option!='':
            flash(msg_3a4_option,'warning')
            return render_template('allocation_run.html', form=form, user=login_name)

        if f_supply!=None:
            sheet_name_msg, msg_transit, msg_oh, msg_scr=check_supply_input_file_format(file_path_supply,
                                                                                        col_transit_must_have,
                                                                                        col_oh_must_have,
                                                                                        col_scr_must_have,
                                                                                        'in-transit','df-oh','por')
            if sheet_name_msg!='':
                flash(sheet_name_msg,'warning')
                return render_template('allocation_run.html', form=form, user=login_name)
            if msg_transit!='':
                flash(msg_transit,'warning')
                return render_template('allocation_run.html', form=form, user=login_name)
            if msg_oh!='':
                flash(msg_oh,'warning')
                return render_template('allocation_run.html', form=form, user=login_name)
            if msg_scr!='':
                flash(msg_scr,'warning')
                return render_template('allocation_run.html', form=form, user=login_name)

       # 判断并定义ranking_col
        if ranking_logic == 'cus_sat':
            ranking_col = ranking_col_cust
        #elif ranking_logic == 'max_rev':
        #    ranking_col = ranking_col_rev

        try:
            # 读取数据
            df_3a4 = pd.read_csv(file_path_3a4, encoding='ISO-8859-1', parse_dates=['ORIGINAL_FCD_NBD_DATE', 'TARGET_SSD'],
                                 low_memory=False)

            if f_supply!=None:
                df_scr, df_oh, df_transit, df_sourcing=read_supply_data(file_path_supply)
            else:
                df_scr, df_oh, df_transit, df_sourcing=collect_scr_oh_transit_from_scdx_prod(pcba_site,'*')
                df_scr.loc[:, 'date'] = df_scr.date.map(lambda x: x.date())

            # limit BU from 3a4 and df_scr for allocation
            df_3a4, df_scr=limit_bu_from_3a4_and_scr(df_3a4,df_scr,bu_list)
            if df_3a4.shape[0] == 0:
                flash('The 3a4 data is empty, check data source, or check if you put in a BU that does not exist!', 'warning')
                return render_template('allocation_run.html', form=form, user=login_name)

            if df_scr.shape[0] == 0:
                flash('The SCR data is empty, check data source, or check if you put in a BU that does not exist!', 'warning')
                return render_template('allocation_run.html', form=form, user=login_name)

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

@app.route('/result', methods=['GET', 'POST'])
def allocation_result():
    form = ResultForm()
    login_user = request.headers.get('Oidc-Claim-Sub')
    login_name = request.headers.get('Oidc-Claim-Fullname')
    login_title = request.headers.get('Oidc-Claim-Title')

    if login_user == None:
        login_user = 'unknown'
        login_name = 'unknown'
        login_title = 'unknown'
        http_scheme = 'http'
    else:
        http_scheme = 'https'

    if '[C]' in login_title: # for c-workers
        return 'Sorry, you are not authorized to access this.'

    # read the files
    output_record_hours=360
    upload_record_hours=240
    trash_record_hours=240
    df_output=get_file_info_on_drive(base_dir_output,keep_hours=output_record_hours)
    df_upload=get_file_info_on_drive(base_dir_upload,keep_hours=upload_record_hours)
    df_trash = get_file_info_on_drive(base_dir_trash, keep_hours=trash_record_hours)

    if form.validate_on_submit():
        start_time=pd.Timestamp.now()
        log_msg = []
        log_msg.append('\n\n[' + login_user + '] ' + start_time.strftime('%Y-%m-%d %H:%M'))

        fname_share = form.file_name_share.data.strip()
        submit_share_file = form.submit_share.data

        if fname_share=='':
            msg='Pls put in the name of the file you want to share!'
            flash(msg,'warning')
            return redirect(url_for('allocation_result'))

        if submit_share_file:
            if fname_share not in df_output.File_name.values:
                msg='This file you put in does not exist on server: {}'.format(fname_share)
                flash(msg,'warning')
                return redirect(url_for('allocation_result', _external=True, _scheme=http_scheme, viewarg1=1))

            if login_user not in fname_share:
                msg = 'You can only share file generated by yourself!'
                flash(msg, 'warning')
                return redirect(url_for('allocation_result', _external=True, _scheme=http_scheme, viewarg1=1))

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
            return redirect(url_for('allocation_result', _external=True, _scheme=http_scheme, viewarg1=1))

    return render_template('allocation_result.html',form=form,
                           files_output=df_output.values,
                           output_record_days=int(output_record_hours/24),
                           files_uploaded=df_upload.values,
                           upload_record_days=int(upload_record_hours/24),
                           files_trash=df_trash.values,
                           trash_record_days=int(trash_record_hours/24),
                           user=login_name,
                           login_user=login_user)


# Below did now work out somehow - NOT USED
@app.route('/delete/<path:file_path>',methods=['POST'])
def delete_file(file_path):
    form=AdminForm()

    if form.validate_on_submit():
        os.remove(file_path)
        msg = 'File deleted!'
        flash(msg, 'success')
        return redirect(url_for("allocation_admin", _external=True, _scheme=http_scheme, viewarg1=1))
    return render_template('allocation_admin.html',form=form)

@app.route('/o/<login_user>/<filename>',methods=['GET'])
def delete_file_output(login_user,filename):
    if login_user == 'unknown':
        http_scheme = 'http'
    else:
        http_scheme = 'https'

    if login_user in filename:
        os.rename(os.path.join(base_dir_output,filename),os.path.join(base_dir_trash,filename))
        msg='File removed: {}'.format(filename)
        flash(msg,'success')
    else:
        msg='You can only delete file created by yourself!'
        flash(msg,'warning')

    return redirect(url_for("allocation_result", _external=True, _scheme=http_scheme, viewarg1=1))

@app.route('/u/<login_user>/<filename>',methods=['GET'])
def delete_file_upload(login_user,filename):
    if login_user == 'unknown':
        http_scheme = 'http'
    else:
        http_scheme = 'https'

    if login_user in filename:
        os.rename(os.path.join(base_dir_upload,filename),os.path.join(base_dir_trash,filename))
        msg='File removed: {}'.format(filename)
        flash(msg,'success')
    else:
        msg='You can only delete file uploaded by yourself!'
        flash(msg,'warning')

    return redirect(url_for("allocation_result", _external=True, _scheme=http_scheme, viewarg1=1))


@app.route('/e/<login_user>/<added_by>/<email>/<email_id>',methods=['GET'])
def delete_email_record(login_user,added_by,email,email_id):
    if login_user == 'unknown':
        http_scheme = 'http'
    else:
        http_scheme = 'https'

    if login_user in email or login_user in added_by or login_user==super_user:
        id_list=[str(email_id)]
        delete_email('subscription', id_list)
        msg = 'Email deleted: {}'.format(email)
        flash(msg, 'success')
    else:
        msg = 'You can only delete your email or email added by you!'
        flash(msg,'warning')

    return redirect(url_for("subscribe", _external=True, _scheme=http_scheme, viewarg1=1))


@app.route('/recover/<login_user>/<filename>', methods=['GET'])
def recover_file_trash(login_user, filename):
    if login_user == 'unknown':
        http_scheme = 'http'
    else:
        http_scheme = 'https'

    if 'SCR allocation' in filename:
        dest_path=base_dir_output
    else:
        dest_path=base_dir_upload

    if login_user in filename:
        os.rename(os.path.join(base_dir_trash, filename), os.path.join(dest_path, filename))
        msg = 'File put back to original place: {}'.format(filename)
        flash(msg, 'success')
    else:
        msg = 'You can only operate file created by yourself!'
        flash(msg, 'warning')

    return redirect(url_for("allocation_result", _external=True, _scheme=http_scheme, viewarg1=1))

@app.route('/t/<filename>',methods=['GET'])
def download_file_trash(filename):
    f_path=base_dir_trash
    login_user = request.headers.get('Oidc-Claim-Sub')

    add_user_log(user=login_user, location='Download', user_action='Download file',
                 summary=filename)
    return send_from_directory(f_path, filename=filename, as_attachment=True)

@app.route('/o/<filename>',methods=['GET'])
def download_file_output(filename):
    f_path=base_dir_output
    login_user = request.headers.get('Oidc-Claim-Sub')

    add_user_log(user=login_user, location='Download', user_action='Download file',
                 summary=filename)
    return send_from_directory(f_path, filename=filename, as_attachment=True)

@app.route('/u/<filename>',methods=['GET'])
def download_file_upload(filename):
    f_path=base_dir_upload
    login_user = request.headers.get('Oidc-Claim-Sub')

    add_user_log(user=login_user, location='Download', user_action='Download file',
                 summary=filename)
    return send_from_directory(f_path, filename=filename, as_attachment=True)

@app.route('/s/<filename>',methods=['GET'])
def download_file_supply(filename):
    f_path=base_dir_supply
    login_user = request.headers.get('Oidc-Claim-Sub')

    add_user_log(user=login_user, location='Download', user_action='Download file',
                 summary=filename)
    return send_from_directory(f_path, filename=filename, as_attachment=True)

@app.route('/l/<filename>',methods=['GET'])
def download_file_logs(filename):
    f_path=base_dir_logs
    login_user = request.headers.get('Oidc-Claim-Sub')

    add_user_log(user=login_user, location='Download', user_action='Download file',
                 summary=filename)
    return send_from_directory(f_path, filename=filename, as_attachment=True)

@app.route('/subscribe',methods=['GET','POST'])
def subscribe():
    form = SubscriptionForm()
    login_user=request.headers.get('Oidc-Claim-Sub')
    login_name = request.headers.get('Oidc-Claim-Fullname')
    if login_user == None:
        login_user = 'unknown'
        login_name = 'unknown'
        http_scheme = 'http'
    else:
        http_scheme = 'https'

    # read emails
    df_email_detail = read_table('subscription')
    df_email_detail.sort_values(by=['PCBA_Org','BU'],inplace=True)

    if form.validate_on_submit():
        submit_add=form.submit_add.data
        if submit_add:
            pcba_org=form.pcba_org.data.upper().replace(' ','')
            bu=form.bu.data.upper().replace(' ','')
            email_to_add=form.email_to_add.data.lower().replace(' ','').replace(',','').replace(';','')

            if len(pcba_org)==0 or len(email_to_add)==0:
                msg='PCBA org and email are mandatory fields!'
                flash(msg,'warning')
                return render_template('subscribe.html', form=form,
                                       email_details=df_email_detail.values,
                                       user=login_name,
                                       login_user=login_user)

            if email_to_add.count('@')>1:
                msg = 'Input only one email each time!'
                flash(msg, 'warning')
                return render_template('subscribe.html', form=form,
                                       email_details=df_email_detail.values,
                                       user=login_name,
                                       login_user=login_user)

            if 'cisco' not in email_to_add:
                if ('foxconn' in email_to_add and pcba_org not in ['FOL','FJZ']) or \
                    ('jabil' in email_to_add and pcba_org not in ['JPE','JMX']) or \
                    ('fab' in email_to_add and pcba_org not in ['NCB']) or \
                    ('flex' in email_to_add and pcba_org not in ['FDO','FGU']):
                    msg = 'Non-Cisco users can only subscribe to belonged org!'
                    flash(msg, 'warning')
                    return render_template('subscribe.html', form=form,
                                           email_details=df_email_detail.values,
                                           user=login_name,
                                           login_user=login_user)

            if email_to_add in df_email_detail.Email.values:
                update_email_data(pcba_org, bu, email_to_add, login_user)
                msg='This email already exists! Data has been updated: {}'.format(email_to_add)
                flash(msg,'success')
                return redirect(url_for('subscribe', _external=True, _scheme=http_scheme, viewarg1=1))
            else:
                add_email_data(pcba_org, bu, email_to_add,login_user)
                msg='This email is added: {}'.format(email_to_add)
                flash(msg,'success')
                return redirect(url_for('subscribe', _external=True, _scheme=http_scheme, viewarg1=1))

    return render_template('subscribe.html', form=form,
                           email_details=df_email_detail.values,
                           user=login_name,
                           login_user=login_user,
                           subtitle=' - Subscribe')


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

    if login_user!='kwang2':
        add_user_log(user=login_user, location='Admin', user_action='Visit - trying', summary='')
        log_msg='\n\n[' + login_user + '] attempting access ADMIN ' + pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')
        log_msg=log_msg + '\n' + str(request.headers)
        with open(os.path.join(base_dir_logs, 'error_log.txt'), 'a+') as file_object:
            file_object.write(log_msg)

        raise ValueError
        add_user_log(user=login_user, location='Admin', user_action='Visit success', summary='why this happens??')

    # get file info
    df_output=get_file_info_on_drive(base_dir_output,keep_hours=360)
    df_upload=get_file_info_on_drive(base_dir_upload,keep_hours=240)
    df_supply=get_file_info_on_drive(base_dir_supply,keep_hours=240)
    df_trash = get_file_info_on_drive(base_dir_trash, keep_hours=360)
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
        elif fname in df_trash.File_name.values:
            f_path = df_trash[df_trash.File_name == fname].File_path.values[0]
            os.remove(f_path)
            msg = '{} removed!'.format(fname)
            flash(msg, 'success')
        else:
            msg = 'Error file name! Ensure it is in output folder,upload folder or supply folder: {}'.format(fname)
            flash(msg, 'warning')
            return redirect(url_for('allocation_admin',_external=True,_scheme=http_scheme,viewarg1=1))

    return render_template('allocation_admin.html',form=form,
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

    if login_user!='kwang2':
        raise ValueError
        add_user_log(user=login_user, location='Document', user_action='Visit - trying', summary='why this happens??')

    return render_template('allocation_document.html',
                           user=login_name)

@app.route('/user-guide')
def user_guide():
    login_user = request.headers.get('Oidc-Claim-Sub')
    login_name = request.headers.get('Oidc-Claim-Fullname')
    if login_user == None:
        login_user = 'unknown'
        login_name = 'unknown'

    return render_template('allocation_userguide.html',user=login_name, subtitle=' - FAQ')

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

    if form.validate_on_submit():
        submit_download_scdx_poc=form.submit_download_supply_poc.data
        submit_download_scdx_prod = form.submit_download_supply_prod.data

        if submit_download_scdx_poc:
            pcba_site_poc=form.pcba_site_poc.data.strip().upper()

            log_msg = []
            log_msg.append('\n\n[Download SCDx-POC] - ' + login_user + ' ' + pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'))

            now = pd.Timestamp.now()
            f_path=base_dir_supply
            fname=pcba_site_poc + ' scr_oh_intransit(scdx-poc) ' + now.strftime('%m-%d %Hh%Mm ') + login_user + '.xlsx'
            log_msg.append('Download supply from SCDx-POC')

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
                msg = 'Error downloading supply data from SCDx-POC! Maybe SCDx issue, pls try again after a while.'
                flash(msg, 'warning')
                traceback.print_exc()
                add_user_log(user=login_user, location='Datasource', user_action='Download SCDx-POC', summary='Error: [' + pcba_site_poc + '] ' + str(e))

                # write details to error_log.txt
                log_msg = '\n'.join(log_msg)
                with open(os.path.join(base_dir_logs, 'error_log.txt'), 'a+') as file_object:
                    file_object.write(log_msg)
                traceback.print_exc(file=open(os.path.join(base_dir_logs, 'error_log.txt'), 'a+'))

                return redirect(url_for('allocation_datasource', _external=True, _scheme=http_scheme, viewarg1=1))
        elif submit_download_scdx_prod:
            pcba_site_prod=form.pcba_site_prod.data.strip().upper()

            log_msg = []
            log_msg.append('\n\n[Download SCDx-Production] - ' + pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'))

            now = pd.Timestamp.now()
            f_path=base_dir_supply
            fname=pcba_site_prod + ' scr_oh_intransit(scdx-prod) ' + now.strftime('%m-%d %Hh%Mm ') + login_user + '.xlsx'
            log_msg.append('Download supply from SCDx-POC')

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
                msg = 'Error downloading supply data from SCDx-Prod! Pls try again later.'
                flash(msg, 'warning')
                add_user_log(user=login_user, location='Datasource', user_action='Download SCDx-Prod', summary='Error: [' + pcba_site_prod + '] ' + str(e))

                # write details to error_log.txt
                log_msg = '\n'.join(log_msg)
                with open(os.path.join(base_dir_logs, 'error_log.txt'), 'a+') as file_object:
                    file_object.write(log_msg)
                traceback.print_exc(file=open(os.path.join(base_dir_logs, 'error_log.txt'), 'a+'))

                return redirect(url_for('allocation_datasource', _external=True, _scheme=http_scheme, viewarg1=1))

    return render_template('allocation_datasource.html',user=login_name,form=form)


@app.route('/scdx-api',methods=['GET','POST'])
def scdx_api():
    form=ScdxAPIForm()
    login_user = request.headers.get('Oidc-Claim-Sub')
    login_name = request.headers.get('Oidc-Claim-Fullname')
    login_title = request.headers.get('Oidc-Claim-Title')
    if login_user == None:
        login_user = 'unknown'
        login_name = 'unknown'
        login_title = 'unknown'
        http_scheme = 'http'
    else:
        http_scheme = 'https'

    if '[C]' in login_title:  # for c-workers
        return 'Sorry, you are not authorized to access this.'

    if form.validate_on_submit():
        submit_download_scdx_poc=form.submit_download_supply_poc.data
        submit_download_scdx_prod = form.submit_download_supply_prod.data

        if submit_download_scdx_poc:
            pcba_site_poc=form.pcba_site_poc.data.strip().upper()

            if pcba_site_poc=='':
                msg = 'Pls put in a PCBA site name to proceed!'
                flash(msg,'warning')
                return redirect(url_for('scdx_api', _external=True, _scheme=http_scheme, viewarg1=1))

            log_msg = []
            log_msg.append('\n\n[Download SCDx-POC] - ' + login_user + ' ' + pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'))

            now = pd.Timestamp.now()
            f_path=base_dir_supply
            fname=pcba_site_poc + ' scr_oh_intransit(scdx-poc) ' + now.strftime('%m-%d %Hh%Mm ') + login_user + '.xlsx'
            log_msg.append('Download supply from SCDx-POC')

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
                msg = 'Error downloading supply data from SCDx-POC! Maybe SCDx issue, pls try again after a while.'
                flash(msg, 'warning')
                traceback.print_exc()
                add_user_log(user=login_user, location='Datasource', user_action='Download SCDx-POC', summary='Error: [' + pcba_site_poc + '] ' + str(e))

                # write details to error_log.txt
                log_msg = '\n'.join(log_msg)
                with open(os.path.join(base_dir_logs, 'error_log.txt'), 'a+') as file_object:
                    file_object.write(log_msg)
                traceback.print_exc(file=open(os.path.join(base_dir_logs, 'error_log.txt'), 'a+'))

                return redirect(url_for('scdx_api', _external=True, _scheme=http_scheme, viewarg1=1))
        elif submit_download_scdx_prod:
            pcba_site_prod=form.pcba_site_prod.data.strip().upper()

            if pcba_site_prod=='':
                msg = 'Pls put in a PCBA site name to proceed!'
                flash(msg,'warning')
                return redirect(url_for('scdx_api', _external=True, _scheme=http_scheme, viewarg1=1))

            log_msg = []
            log_msg.append('\n\n[Download SCDx-Production] - ' + pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'))

            now = pd.Timestamp.now()
            f_path=base_dir_supply
            fname=pcba_site_prod + ' scr_oh_intransit(scdx-prod) ' + now.strftime('%m-%d %Hh%Mm ') + login_user + '.xlsx'
            log_msg.append('Download supply from SCDx-POC')

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
                msg = 'Error downloading supply data from SCDx-Prod! Pls try again later.'
                flash(msg, 'warning')
                add_user_log(user=login_user, location='Datasource', user_action='Download SCDx-Prod', summary='Error: [' + pcba_site_prod + '] ' + str(e))

                # write details to error_log.txt
                log_msg = '\n'.join(log_msg)
                with open(os.path.join(base_dir_logs, 'error_log.txt'), 'a+') as file_object:
                    file_object.write(log_msg)
                traceback.print_exc(file=open(os.path.join(base_dir_logs, 'error_log.txt'), 'a+'))

                return redirect(url_for('scdx_api', _external=True, _scheme=http_scheme, viewarg1=1))

    return render_template('scdx_api.html',user=login_name,form=form,subtitle=' - SCDx API')
