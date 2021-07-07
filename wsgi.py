# created by Ken wang, Oct, 2020


# add below matplotlib.use('Agg') to avoid this error: Assertion failed: (NSViewIsCurrentlyBuildingLayerTreeForDisplay()
# != currentlyBuildingLayerTree), function NSViewSetCurrentlyBuildingLayerTreeForDisplay
import matplotlib
matplotlib.use('Agg')

from werkzeug.utils import secure_filename
from flask import flash,send_from_directory,render_template, request,redirect,url_for
from functions import *
from SCDx_PROD_API import collect_scr_oh_transit_from_scdx_prod
from settings import *
from sending_email import *
from db_add import *
from db_read import *
from db_update import *
from db_delete import *
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
    if login_user!='kwang2':
        add_log_summary(user=login_user, location='Home-RUN', user_action='Visit', summary='')

    if form.validate_on_submit():
        log_msg_main = []
        start_time = pd.Timestamp.now()
        print('start to run: {}'.format(start_time.strftime('%Y-%m-%d %H:%M')))

        log_msg='\n\n[' + login_user + '] ' + start_time.strftime('%Y-%m-%d %H:%M')
        add_log_details(msg=log_msg)
        #log_msg.append('User info: ' + request.headers.get('User-agent'))

        # 通过条件判断及邮件赋值，开始执行任务
        pcba_site=form.org.data.strip().upper()
        bu=form.bu.data.strip().upper()
        bu_list=bu.split('/')
        f_supply= form.file_supply.data
        f_3a4 = form.file_3a4.data
        ranking_logic=form.ranking_logic.data # This is not shown on the UI - take the default value set
        description = form.description.data.strip()
        log_msg_main.append(pcba_site + ' ' + bu)
        log_msg = '\n' + pcba_site + ' ' + bu
        add_log_details(msg=log_msg)

        # 判断并定义ranking_col
        if ranking_logic == 'cus_sat':
            ranking_col = ranking_col_cust
        # elif ranking_logic == 'max_rev':
        #    ranking_col = ranking_col_rev

        # check input file extension
        ext_correct_3a4 = check_file_extension(f_3a4, extension='.csv')
        if f_supply != None:
            ext_correct_supply = check_file_extension(f_supply, extension='.xlsx')
        else:
            ext_correct_supply=True

        if not np.all([ext_correct_3a4,ext_correct_supply]):
            msg='File type error! Ensure 3a4 is .csv format and supply file is .xlsx format!'
            flash(msg,'warning')
            return render_template('allocation_run.html', form=form, user=login_user)

        # check input
        if f_supply!=None:
            if pcba_site not in f_supply.filename.upper():
                msg = "The supply file used is not a right one to do allocation for {}: {}.".format(pcba_site,f_supply.filename)
                flash(msg, 'warning')
                print(login_user,msg)
                summary = 'pcba_site ({}) and supply file({}) not matching!'.format(pcba_site,f_supply.filename)
                add_log_summary(user=login_user, location='Allocation', user_action='Make allocation', summary=summary)

                return render_template('allocation_run.html', form=form, user=login_user)

        # save the files
        file_path_3a4 = os.path.join(base_dir_upload, login_user + '_' + secure_filename(f_3a4.filename))
        f_3a4.save(file_path_3a4)
        if f_supply!=None:
            file_path_supply = os.path.join(base_dir_upload, login_user + '_' + secure_filename(f_supply.filename))
            f_supply.save(file_path_supply)

        # formally start the program
        try:
            # get files size and log it
            file_size_3a4 = get_file_size(file_path_3a4)
            log_msg_main.append(f_3a4.filename + '(size: ' + file_size_3a4 + ')')
            log_msg = '\nFile 3a4: ' + f_3a4.filename + '(size: ' + file_size_3a4 + ')'
            add_log_details(msg=log_msg)
            if f_supply!=None:
                file_size_supply=get_file_size(file_path_supply)
                log_msg_main.append(f_supply.filename + '(size: ' + file_size_supply + ')')
                log_msg = '\nFile supply: ' + f_supply.filename + '(size: ' + file_size_supply + ')'
                add_log_details(msg=log_msg)
            else:
                log_msg_main.append('Supply file directly download through API')
                log_msg = '\nFile supply: directly download through API'
                add_log_details(msg=log_msg)

            # read 3a4 data and check the columns required
            df_3a4, msg_3a4, msg_3a4_option=read_3a4_and_check_columns(file_path_3a4,col_3a4_must_have)
            if msg_3a4!='':
                flash(msg_3a4,'warning')
                return render_template('allocation_run.html', form=form, user=login_user)
            if msg_3a4_option!='':
                flash(msg_3a4_option,'warning')
                return render_template('allocation_run.html', form=form, user=login_user)

            # read supply data and check the columns required in each
            if f_supply==None:
                df_por, df_oh, df_transit, df_sourcing = collect_scr_oh_transit_from_scdx_prod(pcba_site, '*')
                df_por.loc[:, 'date'] = df_por.date.map(lambda x: x.date())
                # also save the file in case needed afterwards
                fname = pcba_site + ' scr_oh_intransit(scdx-prod - directly retrieved) ' + pd.Timestamp.now().strftime(
                    '%m-%d %Hh%Mm ') + login_user + '.xlsx'
                output_path = os.path.join(base_dir_upload,fname)
                data_to_write = {'por': df_por,
                                 'df-oh': df_oh,
                                 'in-transit': df_transit,
                                 'sourcing-rule': df_sourcing}
                write_data_to_excel(output_path, data_to_write)
            else:
                result=read_supply_file_and_check_columns(file_path_supply, col_scr_must_have, col_oh_must_have, col_transit_must_have,
                                                   col_sourcing_rule_must_have)

                if len(result)>4: #refers to the error msg instead of hte df
                    flash(result,'warning')
                    return render_template('allocation_run.html', form=form, user=login_user)
                else:
                    df_por=result[0]
                    df_oh=result[1]
                    df_transit=result[2]
                    df_sourcing=result[3]
                    df_por, df_oh, df_transit, df_sourcing = patch_make_sure_supply_data_int_format(df_por, df_oh, df_transit,
                                                                                                    df_sourcing)

            # limit BU from 3a4 and df_por for allocation
            df_3a4, df_por=limit_bu_from_3a4_and_scr(df_3a4,df_por,bu_list)
            if df_3a4.shape[0] == 0:
                flash('The 3a4 data is empty, check data source, or check if you put in a BU that does not exist!', 'warning')
                return render_template('allocation_run.html', form=form, user=login_user)

            if df_por.shape[0] == 0:
                flash('The SCR data is empty, check data source, or check if you put in a BU that does not exist!', 'warning')
                return render_template('allocation_run.html', form=form, user=login_user)

            #### main program
            output_filename=pcba_allocation_main_program(df_3a4, df_oh, df_transit, df_por, df_sourcing, pcba_site, bu_list, ranking_col,description,login_user)
            flash('Allocation file created for downloading: {} '.format(output_filename), 'success')

            # Write the log file
            finish_time=pd.Timestamp.now()
            processing_time = round((finish_time - start_time).total_seconds() / 60, 1)
            msg='Total processing time:' + str(processing_time) + ' min'
            log_msg_main.append(msg)
            add_log_details(msg)
            print('\n' + msg + '\n')

            summary='; '.join(log_msg_main)
            add_log_summary(user=login_user,location='Allocation',user_action='Make allocation',summary=summary)

        except Exception as e:
            try:
                del df_por, df_3a4, df_oh, df_transit, df_sourcing
                gc.collect()
            except:
                print('')

            traceback.print_exc()
            log_msg_main.append(str(e))
            flash('Error encountered: {}'.format(str(e)),'warning')
            #Write the log file
            summary = '; '.join(log_msg_main)
            add_log_summary(user=login_user, location='Allocation', user_action='Make allocation - ERROR', summary=summary)

            # write details to log_details.txt
            traceback.print_exc(file=open(os.path.join(base_dir_logs, 'log_details.txt'), 'a+'))

        # clear memory
        try:
            del df_por, df_3a4, df_oh, df_transit, df_sourcing
            gc.collect()
        except:
            print('')

        return redirect(url_for('allocation_run',_external=True,_scheme=http_scheme,viewarg1=1))

    return render_template('allocation_run.html', form=form, user=login_user)

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

                add_log_summary(user=login_user, location='Download', user_action='Share file',
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
                add_log_summary(user=login_user, location='Download', user_action='Share file', summary=str(e))

                # write details to log_details.txt
                log_msg = '\n'.join(log_msg)
                add_log_details(log_msg)
                traceback.print_exc(file=open(os.path.join(base_dir_logs, 'log_details.txt'), 'a+'))

            flash(msg, 'success')
            return redirect(url_for('allocation_result', _external=True, _scheme=http_scheme, viewarg1=1))

    return render_template('allocation_result.html',form=form,
                           files_output=df_output.values,
                           output_record_days=int(output_record_hours/24),
                           files_uploaded=df_upload.values,
                           upload_record_days=int(upload_record_hours/24),
                           files_trash=df_trash.values,
                           trash_record_days=int(trash_record_hours/24),
                           user=login_user,
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
        delete_table_data('allocation_subscription', id_list)
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

    add_log_summary(user=login_user, location='Download', user_action='Download file',
                 summary=filename)
    return send_from_directory(f_path, filename, as_attachment=True)

@app.route('/o/<filename>',methods=['GET'])
def download_file_output(filename):
    f_path=base_dir_output
    login_user = request.headers.get('Oidc-Claim-Sub')

    add_log_summary(user=login_user, location='Download', user_action='Download file',
                 summary=filename)
    return send_from_directory(f_path, filename, as_attachment=True)

@app.route('/u/<filename>',methods=['GET'])
def download_file_upload(filename):
    f_path=base_dir_upload
    login_user = request.headers.get('Oidc-Claim-Sub')

    add_log_summary(user=login_user, location='Download', user_action='Download file',
                 summary=filename)
    return send_from_directory(f_path, filename, as_attachment=True)

@app.route('/s/<filename>',methods=['GET'])
def download_file_supply(filename):
    f_path=base_dir_supply
    login_user = request.headers.get('Oidc-Claim-Sub')

    add_log_summary(user=login_user, location='Download', user_action='Download file',
                 summary=filename)
    return send_from_directory(f_path, filename, as_attachment=True)

@app.route('/l/<filename>',methods=['GET'])
def download_file_logs(filename):
    f_path=base_dir_logs
    login_user = request.headers.get('Oidc-Claim-Sub')

    add_log_summary(user=login_user, location='Download', user_action='Download file',
                 summary=filename)
    return send_from_directory(f_path, filename, as_attachment=True)

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
    df_email_detail = read_table('allocation_subscription')
    #df_email_detail.sort_values(by=['PCBA_Org','BU'],inplace=True)

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
                                       user=login_user,
                                       login_user=login_user)

            if email_to_add.count('@')>1:
                msg = 'Input only one email each time!'
                flash(msg, 'warning')
                return render_template('subscribe.html', form=form,
                                       email_details=df_email_detail.values,
                                       user=login_user,
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
                                           user=login_user,
                                           login_user=login_user)

            if email_to_add in df_email_detail.Email.values:
                update_email_data(pcba_org, bu, email_to_add, login_user)
                msg='This email already exists! Data has been updated: {}'.format(email_to_add)
                flash(msg,'success')
                return redirect(url_for('subscribe', _external=True, _scheme=http_scheme, viewarg1=1))
            else:
                try:
                    add_email_data(pcba_org, bu, email_to_add,login_user)
                except:
                    roll_back()
                    msg = 'Adding email data to database error (Org: {},BU: {},Email: {})! - contact kwang2 if you can not rootcause.'.format(pcba_org,bu,email_to_add)
                    flash(msg, 'warning')
                    add_log_summary(user=login_user, location='subscribe', user_action='Add email - error',summary=msg)
                    #add_log_details(msg='\n' + login_user + '\n' + msg)

                    return redirect(url_for("subscribe", _external=True, _scheme=http_scheme))

                msg='This email is added: {}'.format(email_to_add)
                flash(msg,'success')
                return redirect(url_for('subscribe', _external=True, _scheme=http_scheme, viewarg1=1))

    return render_template('subscribe.html', form=form,
                           email_details=df_email_detail.values,
                           user=login_user,
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

    if login_user!='kwang2' and login_user!='unknown':
        add_log_summary(user=login_user, location='Admin', user_action='Visit - trying', summary='')
        return redirect(url_for('allocation_run',_external=True,_scheme=http_scheme,viewarg1=1))
        add_log_summary(user=login_user, location='Admin', user_action='Visit success', summary='why this happens??')

    # get file info
    df_output=get_file_info_on_drive(base_dir_output,keep_hours=360)
    df_upload=get_file_info_on_drive(base_dir_upload,keep_hours=240)
    df_supply=get_file_info_on_drive(base_dir_supply,keep_hours=240)
    df_trash = get_file_info_on_drive(base_dir_trash, keep_hours=360)
    df_logs=get_file_info_on_drive(base_dir_logs,keep_hours=10000)

    # read logs
    df_log_detail = read_table('allocation_user_log')
    df_log_detail.sort_values(by=['id'],ascending=False,inplace=True)

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
                           user=login_user)


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
        return redirect(url_for("document", _external=True,_scheme=http_scheme))
        add_log_summary(user=login_user, location='Document', user_action='Visit - trying', summary='why this happens??')

    return render_template('allocation_document.html',
                           user=login_user)

@app.route('/user-guide')
def user_guide():
    login_user = request.headers.get('Oidc-Claim-Sub')
    login_name = request.headers.get('Oidc-Claim-Fullname')
    if login_user == None:
        login_user = 'unknown'
        login_name = 'unknown'

    return render_template('allocation_userguide.html',user=login_user, subtitle=' - FAQ')

@app.route('/exceptional_priority',methods=['GET','POST'])
def exceptional_priority():
    form=ExceptionalPriorityForm()
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

    if login_user not in ['kwang2', 'unknown']:
        add_log_summary(user=login_user, location='E-priority', user_action='visit', summary='')

    df_db_data=read_table('allocation_exception_priority')
    df_db_data.sort_values(by='Added_on', ascending=False, inplace=True)
    summary_info = []
    summary_info.append('Total records: {}'.format(df_db_data.shape[0]))
    for user in df_db_data.Added_by.unique():
        summary_info.append('{}: {}'.format(user, df_db_data[df_db_data.Added_by == user].shape[0]))
    summary_info = '; '.join(summary_info)

    if form.validate_on_submit():
        submit_remove_packed = form.submit_remove_packed.data
        submit_upload_template = form.submit_upload_template.data
        submit_show_all=form.submit_show_all.data
        submit_show_me=form.submit_show_me.data
        submit_download_me=form.submit_download_me.data
        bu_org=form.bu_org.data
        submit_show_bu_org=form.submit_show_bu_org.data
        submit_download_bu_org=form.submit_download_bu_org.data

        # define needed columns for the template and 3a4
        col_template = ['SO_SS', 'ORG', 'BU', 'Ranking', 'Comments']
        col_3a4 = ['SO_SS', 'ORGANIZATION_CODE', 'BUSINESS_UNIT', 'ORDER_HOLDS', 'PACKOUT_QUANTITY']

        if submit_remove_packed:
            file_upload_3a4 = form.file_upload_3a4.data
            # confirm file uploaded and save the file
            if file_upload_3a4 == None:
                msg = 'Pls select the 3a4 file to upload!'
                flash(msg, 'warning')
                return redirect(url_for("exceptional_priority", _external=True,_scheme=http_scheme))
            else:
                ext_3a4 = os.path.splitext(file_upload_3a4.filename)[1]
                if ext_3a4 != '.csv':
                    msg = '3a4 file only accepts CSV formats here!'
                    flash(msg, 'warning')
                    return redirect(url_for("exceptional_priority", _external=True,_scheme=http_scheme))

                # save file
                file_path_3a4 = os.path.join(base_dir_upload,
                                                  login_user + '_' + secure_filename(file_upload_3a4.filename))
                file_upload_3a4.save(file_path_3a4)

            # read the file
            df_3a4 = pd.read_csv(file_path_3a4,encoding='iso-8859-1')

            # Limit the needed columns and check 3a4 formats
            try:
                df_3a4=df_3a4[col_3a4]
            except:
                msg = '3a4 format error! Ensure following columns are included: {}. You can use 3a4 view PCBA_ALLOCATION and select the related ORG/BU to download 3a4.'.format(col_3a4)
                flash(msg, 'warning')
                return redirect(url_for("exceptional_priority", _external=True,_scheme=http_scheme))
            # if login_user == super_user, then all records are in scope
            removed_ss=remove_packed_exceptional_priority_ss(df_3a4, login_user)

            msg = '{} SO_SS are removed from the database due to packed/cancelled.'.format(len(removed_ss))
            flash(msg, 'success')
            add_log_summary(user=login_user, location='E-priority', user_action='Remove packed', summary=msg)

            return redirect(url_for("exceptional_priority", _external=True,_scheme=http_scheme))
        elif submit_upload_template:
            file_upload_template = form.file_upload_template.data

            # confirm file uploaded and save the file
            if file_upload_template==None:
                msg='Pls select the file to upload!'
                flash(msg,'warning')
                return redirect(url_for("exceptional_priority", _external=True,_scheme=http_scheme))
            else:
                ext_template = os.path.splitext(file_upload_template.filename)[1]
                if ext_template != '.xlsx':
                    msg = 'The template file only accepts .xlsx formats here!'
                    flash(msg, 'warning')
                    return redirect(url_for("exceptional_priority", _external=True, _scheme=http_scheme))

                # save the file
                file_path_template = os.path.join(base_dir_upload, login_user + '_' + secure_filename(file_upload_template.filename))
                file_upload_template.save(file_path_template)

            # read the file
            df_exceptional_priority=pd.read_excel(file_path_template)

            # identify errors in the template
            df_missing_value = df_exceptional_priority[
                (df_exceptional_priority.SO_SS.isnull()) | (df_exceptional_priority.Ranking.isnull())]
            df_duplicated=df_exceptional_priority[df_exceptional_priority.duplicated('SO_SS')]
            if df_missing_value.shape[0]>0:
                msg='Check your template! {} record will not uploaded due to missing SO_SS or Ranking values!'.format(df_missing_value.shape[0])
                flash(msg,'warning')
            if df_duplicated.shape[0] > 0:
                msg = 'Check your template! Following SO_SS are duplicated and only the last record will be uploaded: {}'.format(
                    df_duplicated.SO_SS.values)
                flash(msg, 'warning')

            # remove the error records
            df_exceptional_priority.drop_duplicates('SO_SS',keep='last',inplace=True)
            df_exceptional_priority=df_exceptional_priority[(df_exceptional_priority.SO_SS.notnull())&(df_exceptional_priority.Ranking.notnull())].copy()

            # limit the needed columns (checking formats)
            try:
                df_exceptional_priority=df_exceptional_priority[col_template]
            except:
                msg = 'Stop - format error! Ensure following columns are included: {}'.format(col_template)
                flash(msg, 'warning')
                return redirect(url_for("exceptional_priority", _external=True,_scheme=http_scheme))

            # remove all data for user and write in new data from the template
            df_db_data_user = df_db_data[df_db_data.Added_by == login_user]
            delete_table_data('allocation_exception_priority', df_db_data_user.id)
            try:
                add_exceptional_priority_data_from_template(df_exceptional_priority,login_user)
            except Exception as e:
                roll_back()
                msg='Adding exceptional priority data to database error (template: {}; error msg: {})! - contact kwang2 if you can not rootcause.'.format(secure_filename(file_upload_template.filename),str(e)[:100])
                flash(msg,'warning')
                add_log_summary(user=login_user, location='E-priority', user_action='Upload template - error', summary=msg)
                #add_log_details(msg='\n' + login_user + '\n' + msg)

                return redirect(url_for("exceptional_priority", _external=True, _scheme=http_scheme))

            # read and display data by user
            df_db_data = read_table('allocation_exception_priority')
            df_db_data=df_db_data[df_db_data.Added_by==login_user]
            df_db_data.sort_values(by='Added_on', ascending=False, inplace=True)

            msg='{} records in db deleted, and replaced with {} records uploaded through the template.'\
                .format(df_db_data_user.shape[0],df_exceptional_priority.shape[0])
            flash(msg,'success')
            add_log_summary(user=login_user, location='E-priority', user_action='Upload template', summary=msg)

            return render_template('allocation_exceptional_priority.html',
                                   db_data_header=df_db_data.columns,
                                   db_data_value=df_db_data.values,
                                   form=form,
                                   user=login_user,
                                   subtitle=' - Exceptional Priority')
        elif submit_show_all:
            df_db_data = read_table('allocation_exception_priority')
            df_db_data.sort_values(by='Added_on', ascending=False, inplace=True)
            summary_info = []
            summary_info.append('Total records: {}'.format(df_db_data.shape[0]))
            for user in df_db_data.Added_by.unique():
                summary_info.append('{}: {}'.format(user, df_db_data[df_db_data.Added_by == user].shape[0]))
            summary_info = '; '.join(summary_info)

            return render_template('allocation_exceptional_priority.html',
                                   db_data_header=df_db_data.columns,
                                   db_data_value=df_db_data.values,
                                   summary_info=summary_info,
                                   form=form,
                                   user=login_user,
                                   subtitle=' - Exceptional Priority')
        elif submit_show_me:
            df_db_data = read_table('allocation_exception_priority')
            df_db_data = df_db_data[df_db_data.Added_by == login_user]
            df_db_data.sort_values(by='Added_on', ascending=False, inplace=True)
            summary_info = []
            summary_info.append('Total records: {}'.format(df_db_data.shape[0]))
            for user in df_db_data.Added_by.unique():
                summary_info.append('{}: {}'.format(user, df_db_data[df_db_data.Added_by == user].shape[0]))
            summary_info = '; '.join(summary_info)

            return render_template('allocation_exceptional_priority.html',
                           db_data_header=df_db_data.columns,
                           db_data_value=df_db_data.values,
                           summary_info=summary_info,
                           form=form,
                           user=login_user,
                           subtitle=' - Exceptional Priority')

        elif submit_download_me:
            df_db_data = read_table('allocation_exception_priority')
            df_db_data = df_db_data[df_db_data.Added_by == login_user][col_template]
            df_db_data.set_index('SO_SS',inplace=True)
            df_db_data.Ranking=df_db_data.Ranking.astype(float)
            f_path=base_dir_supply
            fname='Exceptional priority SS ' + login_user + ' ' + pd.Timestamp.now().strftime('%m-%d') + '.xlsx'

            df_db_data.to_excel(os.path.join(f_path,fname))

            return send_from_directory(f_path, fname, as_attachment=True)

        elif submit_show_bu_org:
            if bu_org=='':
                msg='Pls input BU/ORG'
                flash(msg,'warning')
                return redirect(url_for("exceptional_priority", _external=True,_scheme=http_scheme))

            bu_org=bu_org.strip().split('/')
            bu=bu_org[0].strip().upper()
            if len(bu_org)==1:
                org=''
            else:
                org=bu_org[1].strip().upper()
                if org=='*':
                    org=''

            df_db_data = read_table('allocation_exception_priority')
            df_db_data = df_db_data[df_db_data.BU == bu].copy()
            if org!='':
                df_db_data = df_db_data[df_db_data.ORG == org]
            df_db_data.sort_values(by='Added_on', ascending=False, inplace=True)
            summary_info = []
            summary_info.append('Total records: {}'.format(df_db_data.shape[0]))
            for user in df_db_data.Added_by.unique():
                summary_info.append('{}: {}'.format(user, df_db_data[df_db_data.Added_by == user].shape[0]))
            summary_info = '; '.join(summary_info)

            return render_template('allocation_exceptional_priority.html',
                                   db_data_header=df_db_data.columns,
                                   db_data_value=df_db_data.values,
                                   summary_info=summary_info,
                                   form=form,
                                   user=login_user,
                                   subtitle=' - Exceptional Priority')
        elif submit_download_bu_org:
            if bu_org == '':
                msg = 'Pls input BU/ORG'
                flash(msg, 'warning')
                return redirect(url_for("exceptional_priority", _external=True, _scheme=http_scheme))

            bu_org = bu_org.strip().split('/')
            bu = bu_org[0].strip().upper()
            if len(bu_org) == 1:
                org = ''
            else:
                org = bu_org[1].strip().upper()
                if org == '*':
                    org = ''

            df_db_data = read_table('allocation_exception_priority')
            df_db_data = df_db_data[df_db_data.BU == bu].copy()
            if org != '':
                df_db_data = df_db_data[df_db_data.ORG == org]
            # df_db_data.sort_values(by='Ranking', inplace=True)

            df_db_data.set_index('SO_SS',inplace=True)
            df_db_data.Ranking=df_db_data.Ranking.astype(float)

            f_path=base_dir_supply
            fname='Exceptional priority SS ' + login_user + ' ' + pd.Timestamp.now().strftime('%m-%d') + '.xlsx'

            df_db_data.to_excel(os.path.join(f_path, fname))

            return send_from_directory(f_path, fname, as_attachment=True)

    return render_template('allocation_exceptional_priority.html',
                           db_data_header=df_db_data.columns,
                           db_data_value=df_db_data.values,
                           summary_info=summary_info,
                           form=form,
                           user=login_user,
                           subtitle=' - Exceptional Priority')


@app.route('/exceptional_sourcing_split',methods=['GET','POST'])
def exceptional_sourcing_split():
    form=ExceptionalSourcingSplitForm()
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

    df_db_data=read_table('allocation_exception_sourcing_split')

    if form.validate_on_submit():
        submit_upload_template = form.submit_upload_template.data
        submit_show_all=form.submit_show_all.data
        submit_show_me=form.submit_show_me.data
        submit_download=form.submit_download.data

        # define needed columns for the template
        col_template = ['DF_site', 'PCBA_site', 'BU','PF', 'TAN','Split', 'Comments']

        if submit_upload_template:
            file_upload_template = form.file_upload_template.data

            # confirm file uploaded and save the file
            if file_upload_template==None:
                msg='Pls select the file to upload!'
                flash(msg,'warning')
                return redirect(url_for("exceptional_sourcing_split", _external=True,_scheme=http_scheme))
            else:
                ext_template = os.path.splitext(file_upload_template.filename)[1]
                if ext_template != '.xlsx':
                    msg = 'The template file only accepts .xlsx formats here!'
                    flash(msg, 'warning')
                    return redirect(url_for("exceptional_sourcing_split", _external=True, _scheme=http_scheme))

                file_path_template = os.path.join(base_dir_upload, login_user + '_' + secure_filename(file_upload_template.filename))
                file_upload_template.save(file_path_template)

            # read the file
            df_exceptional_sourcing_split=pd.read_excel(file_path_template)

            # identify errors in the template
            df_missing_value = df_exceptional_sourcing_split[
                (df_exceptional_sourcing_split.DF_site.isnull()) | (df_exceptional_sourcing_split.PCBA_site.isnull()) | (df_exceptional_sourcing_split.TAN.isnull()) | (df_exceptional_sourcing_split.Split.isnull())]
            df_duplicated=df_exceptional_sourcing_split[df_exceptional_sourcing_split.duplicated(['DF_site','PCBA_site','TAN'])]
            if df_missing_value.shape[0]>0:
                msg='Check your template! {} record will not uploaded due to missing DF_site, PCBA_site, TAN or Split values!'.format(df_missing_value.shape[0])
                flash(msg,'warning')
            if df_duplicated.shape[0] > 0:
                msg = 'Check your template! There are {} duplicated rows and only the last record will be uploaded.'.format(
                    df_duplicated.shape[0])
                flash(msg, 'warning')

            # remove the error records
            df_exceptional_sourcing_split.drop_duplicates(['DF_site','PCBA_site','TAN'],keep='last',inplace=True)
            df_exceptional_sourcing_split=df_exceptional_sourcing_split[(df_exceptional_sourcing_split.DF_site.notnull()) & (df_exceptional_sourcing_split.PCBA_site.notnull()) & (df_exceptional_sourcing_split.TAN.notnull()) & (df_exceptional_sourcing_split.Split.notnull())].copy()

            # limit the needed columns (checking formats)
            try:
                df_exceptional_sourcing_split=df_exceptional_sourcing_split[col_template]
                df_exceptional_sourcing_split.Split=df_exceptional_sourcing_split.Split.astype(int)
            except:
                msg = 'Stop - format error! Ensure following columns are included: {}'.format(col_template)
                flash(msg, 'warning')
                return redirect(url_for("exceptional_sourcing_split", _external=True,_scheme=http_scheme))

            # remove all data for user and write in new data from the template
            df_db_data_user = df_db_data[df_db_data.Added_by == login_user]
            delete_table_data('allocation_exception_sourcing_split', df_db_data_user.id)
            add_exceptional_sourcing_split_data_from_template(df_exceptional_sourcing_split,login_user)

            # read and display data by user
            df_db_data = read_table('allocation_exception_sourcing_split')
            df_db_data=df_db_data[df_db_data.Added_by==login_user]

            msg='{} records in db deleted, and replaced with {} records uploaded through the template.'\
                .format(df_db_data_user.shape[0],df_exceptional_sourcing_split.shape[0])
            flash(msg,'success')
            add_log_summary(user=login_user, location='E-sourcing split', user_action='Upload template', summary=msg)

            return render_template('exceptional_sourcing_split.html',
                                   db_data_header=df_db_data.columns,
                                   db_data_value=df_db_data.values,
                                   form=form,
                                   user=login_user,
                                   subtitle=' - Exceptional Sourcing Split')
        elif submit_show_all:
            df_db_data = read_table('allocation_exception_sourcing_split')

            return render_template('exceptional_sourcing_split.html',
                                   db_data_header=df_db_data.columns,
                                   db_data_value=df_db_data.values,
                                   form=form,
                                   user=login_user,
                                   subtitle=' - Exceptional Sourcing Split')
        elif submit_show_me:
            df_db_data = read_table('allocation_exception_sourcing_split')
            df_db_data = df_db_data[df_db_data.Added_by == login_user]

            return render_template('exceptional_sourcing_split.html',
                                   db_data_header=df_db_data.columns,
                                   db_data_value=df_db_data.values,
                                   form=form,
                                   user=login_user,
                                   subtitle=' - Exceptional Sourcing Split')

        elif submit_download:
            df_db_data = read_table('allocation_exception_sourcing_split')
            df_db_data = df_db_data[df_db_data.Added_by == login_user][col_template]
            df_db_data.set_index('DF_site',inplace=True)
            df_db_data.Split=df_db_data.Split.astype(int)
            f_path=base_dir_supply
            fname='Exceptional sourcing split ' + login_user + ' ' + pd.Timestamp.now().strftime('%m-%d') + '.xlsx'

            df_db_data.to_excel(os.path.join(f_path,fname))

            return send_from_directory(f_path, fname, as_attachment=True)

    return render_template('exceptional_sourcing_split.html',
                           db_data_header=df_db_data.columns,
                           db_data_value=df_db_data.values,
                           form=form,
                           user=login_user,
                           subtitle=' - Exceptional Sourcing Split')


@app.route('/tan_grouping',methods=['GET','POST'])
def tan_grouping():
    form=TanGroupingForm()
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

    df_db_data=read_table('allocation_tan_grouping')
    if form.validate_on_submit():
        submit_upload_template = form.submit_upload_template.data
        submit_show_all=form.submit_show_all.data
        submit_show_me=form.submit_show_me.data
        submit_download=form.submit_download.data

        # define needed columns for the template
        col_template = ['Group_name', 'TAN', 'DF','Comments']

        if submit_upload_template:
            file_upload_template = form.file_upload_template.data

            # confirm file uploaded and save the file
            if file_upload_template==None:
                msg='Pls select the file to upload!'
                flash(msg,'warning')
                return redirect(url_for("tan_grouping", _external=True,_scheme=http_scheme))
            else:
                ext_template = os.path.splitext(file_upload_template.filename)[1]
                if ext_template != '.xlsx':
                    msg = 'The template file only accepts .xlsx formats here!'
                    flash(msg, 'warning')
                    return redirect(url_for("tan_grouping", _external=True, _scheme=http_scheme))

                # save the file
                file_path_template = os.path.join(base_dir_upload, login_user + '_' + secure_filename(file_upload_template.filename))
                file_upload_template.save(file_path_template)

            # read the file
            df_tan_grouping=pd.read_excel(file_path_template)

            # identify errors in the template
            df_missing_value = df_tan_grouping[
                (df_tan_grouping.Group_name.isnull()) | (df_tan_grouping.TAN.isnull()) | (df_tan_grouping.DF.isnull())]
            df_duplicated=df_tan_grouping[df_tan_grouping.duplicated(['Group_name','TAN','DF'])]
            if df_missing_value.shape[0]>0:
                msg='Check your template! {} record will not uploaded due to missing Group_name, TAN or DF values!'.format(df_missing_value.shape[0])
                flash(msg,'warning')
            if df_duplicated.shape[0] > 0:
                msg = 'Check your template! There are {} duplicated rows and only the last record will be uploaded.'.format(
                    df_duplicated.shape[0])
                flash(msg, 'warning')

            # remove the error records
            df_tan_grouping.drop_duplicates(['Group_name','TAN','DF'],keep='last',inplace=True)
            df_tan_grouping=df_tan_grouping[(df_tan_grouping.Group_name.notnull()) & (df_tan_grouping.TAN.notnull()) & (df_tan_grouping.DF.notnull())].copy()

            # limit the needed columns (checking formats)
            try:
                df_tan_grouping=df_tan_grouping[col_template]
            except:
                msg = 'Stop - format error! Ensure following columns are included: {}'.format(col_template)
                flash(msg, 'warning')
                return redirect(url_for("tan_grouping", _external=True,_scheme=http_scheme))

            # remove all data for user and write in new data from the template
            df_db_data_user = df_db_data[df_db_data.Added_by == login_user]
            delete_table_data('allocation_tan_grouping', df_db_data_user.id)
            try:
                add_tan_grouping_data_from_template(df_tan_grouping,login_user)
            except Exception as e:
                roll_back()
                msg='Adding TAN grouping data to database error (template: {}; error message: {})! - contact kwang2 if you can not rootcause.'.format(secure_filename(file_upload_template.filename),str(e)[:200])
                flash(msg,'warning')
                add_log_summary(user=login_user, location='TAN grouping', user_action='Upload - error', summary=msg)
                #add_log_details(msg='\n' + login_user + '\n' + msg)
                return redirect(url_for("tan_grouping", _external=True, _scheme=http_scheme))

            # read and display data by user
            df_db_data = read_table('allocation_tan_grouping')
            df_db_data=df_db_data[df_db_data.Added_by==login_user]

            msg='{} records in db deleted, and replaced with {} records uploaded through the template.'\
                .format(df_db_data_user.shape[0],df_tan_grouping.shape[0])
            flash(msg,'success')
            add_log_summary(user=login_user, location='TAN grouping', user_action='Upload template', summary=msg)

            return render_template('tan_grouping.html',
                                   db_data_header=df_db_data.columns,
                                   db_data_value=df_db_data.values,
                                   form=form,
                                   user=login_user,
                                   subtitle=' - TAN Grouping')
        elif submit_show_all:
            df_db_data = read_table('allocation_tan_grouping')

            return render_template('tan_grouping.html',
                                   db_data_header=df_db_data.columns,
                                   db_data_value=df_db_data.values,
                                   form=form,
                                   user=login_user,
                                   subtitle=' - TAN Grouping')
        elif submit_show_me:
            df_db_data = read_table('allocation_tan_grouping')
            df_db_data = df_db_data[df_db_data.Added_by == login_user]

            return render_template('tan_grouping.html',
                                   db_data_header=df_db_data.columns,
                                   db_data_value=df_db_data.values,
                                   form=form,
                                   user=login_user,
                                   subtitle=' - TAN Grouping')

        elif submit_download:
            df_db_data = read_table('allocation_tan_grouping')
            df_db_data = df_db_data[df_db_data.Added_by == login_user][col_template]
            df_db_data.set_index('Group_name',inplace=True)
            f_path=base_dir_supply
            fname='TAN grouping ' + login_user + ' ' + pd.Timestamp.now().strftime('%m-%d') + '.xlsx'

            df_db_data.to_excel(os.path.join(f_path,fname))

            return send_from_directory(f_path, fname, as_attachment=True)

    return render_template('tan_grouping.html',
                           db_data_header=df_db_data.columns,
                           db_data_value=df_db_data.values,
                           form=form,
                           user=login_user,
                           subtitle=' - TAN Grouping')


@app.route('/mpq',methods=['GET','POST'])
def mpq():
    form=MpqForm()
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

    df_db_data=read_table('mpq')
    if form.validate_on_submit():
        upload_option = form.upload_option.data
        submit_upload_template = form.submit_upload_template.data
        submit_show_all=form.submit_show_all.data
        submit_show_me=form.submit_show_me.data
        submit_download=form.submit_download.data

        # define needed columns for the template
        col_template = ['PCBA_ORG', 'TAN', 'MPQ','Comments']

        if submit_upload_template:
            file_upload_template = form.file_upload_template.data

            # confirm file uploaded and save the file
            if file_upload_template == None:
                msg = 'Pls select the file to upload!'
                flash(msg, 'warning')
                return redirect(url_for("mpq", _external=True, _scheme=http_scheme))
            else:
                ext_template = os.path.splitext(file_upload_template.filename)[1]
                if ext_template != '.xlsx':
                    msg = 'The template file only accepts .xlsx formats here!'
                    flash(msg, 'warning')
                    return redirect(url_for("mpq", _external=True, _scheme=http_scheme))

            # save the file
            file_path_template = os.path.join(base_dir_upload, login_user + '_' + secure_filename(file_upload_template.filename))
            file_upload_template.save(file_path_template)

            # read the file
            df_mpq=pd.read_excel(file_path_template)
            col = df_mpq.columns
            col = [c.strip() for c in col]
            df_mpq.columns=col

            # identify errors in the template
            df_missing_value = df_mpq[
                (df_mpq.PCBA_ORG.isnull()) | (df_mpq.TAN.isnull()) | (df_mpq.MPQ.isnull())]
            df_duplicated=df_mpq[df_mpq.duplicated(['PCBA_ORG', 'TAN'])]
            if df_missing_value.shape[0]>0:
                msg='Check your template! {} record will not be uploaded due to missing Group_name, TAN or DF values!'.format(df_missing_value.shape[0])
                flash(msg,'warning')
            if df_duplicated.shape[0] > 0:
                msg = 'Check your template! There are {} duplicated rows and only the last record will be uploaded.'.format(
                    df_duplicated.shape[0])
                flash(msg, 'warning')

            # remove the error records
            df_mpq.drop_duplicates(['PCBA_ORG','TAN'],keep='last',inplace=True)
            df_mpq=df_mpq[(df_mpq.PCBA_ORG.notnull()) & (df_mpq.TAN.notnull()) & (df_mpq.MPQ > 1)].copy()

            # limit the needed columns (checking formats)
            try:
                df_mpq=df_mpq[col_template]
            except:
                msg = 'Stop - format error! Ensure following columns are included: {}'.format(col_template)
                flash(msg, 'warning')
                return redirect(url_for("mpq", _external=True,_scheme=http_scheme))

            df_db_data_user = df_db_data[df_db_data.Added_by == login_user]
            if upload_option == 'replace_all':
                # remove all data for user
                delete_table_data('mpq', df_db_data_user.id)
            elif upload_option == 'add_update':
                # remove existing org_tan data for user
                df_mpq.loc[:, 'org_tan'] = df_mpq.PCBA_ORG + df_mpq.TAN
                df_db_data_user.loc[:, 'org_tan'] = df_db_data_user.PCBA_ORG + df_db_data_user.TAN

                df_db_data_user_existing = df_db_data_user[df_db_data_user.org_tan.isin(df_mpq.org_tan)]
                delete_table_data('mpq', df_db_data_user_existing.id)

            # add in all data in the template
            try:
                add_tan_mpq_from_template(df_mpq,login_user)
            except Exception as e:
                roll_back()
                msg='Adding TAN MPQ data to database error (template: {}; error message: {})! - contact kwang2 if you can not rootcause.'.format(secure_filename(file_upload_template.filename),str(e)[:200])
                flash(msg,'warning')
                add_log_summary(user=login_user, location='MPQ', user_action='Upload - error', summary=msg)
                #add_log_details(msg='\n' + login_user + '\n' + msg)
                return redirect(url_for("mpq", _external=True, _scheme=http_scheme))

            # read and display data by user
            df_db_data = read_table('mpq')
            df_db_data = df_db_data[df_db_data.Added_by == login_user]

            msg = '{} records added or updated based on the template.'.format(df_mpq.shape[0])
            flash(msg, 'success')
            add_log_summary(user=login_user, location='MPQ', user_action='Upload template', summary=msg)

            return render_template('allocation_mpq.html',
                                   db_data_header=df_db_data.columns,
                                   db_data_value=df_db_data.values,
                                   form=form,
                                   user=login_user,
                                   subtitle=' - TAN MPQ')
        elif submit_show_all:
            df_db_data = read_table('mpq')

            return render_template('allocation_mpq.html',
                                   db_data_header=df_db_data.columns,
                                   db_data_value=df_db_data.values,
                                   form=form,
                                   user=login_user,
                                   subtitle=' - TAN MPQ')
        elif submit_show_me:
            df_db_data = read_table('mpq')
            df_db_data = df_db_data[df_db_data.Added_by == login_user]

            return render_template('allocation_mpq.html',
                                   db_data_header=df_db_data.columns,
                                   db_data_value=df_db_data.values,
                                   form=form,
                                   user=login_user,
                                   subtitle=' - TAN MPQ')

        elif submit_download:
            df_db_data = read_table('mpq')
            df_db_data = df_db_data[df_db_data.Added_by == login_user][col_template]
            df_db_data.set_index('PCBA_ORG',inplace=True)
            f_path=base_dir_supply
            fname='TAN MPQ ' + login_user + ' ' + pd.Timestamp.now().strftime('%m-%d') + '.xlsx'

            df_db_data.to_excel(os.path.join(f_path,fname))

            return send_from_directory(f_path, fname, as_attachment=True)

    return render_template('allocation_mpq.html',
                           db_data_header=df_db_data.columns,
                           db_data_value=df_db_data.values,
                           form=form,
                           user=login_user,
                           subtitle=' - TAN MPQ')


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
        submit_download_scdx_prod = form.submit_download_supply_prod.data

        if submit_download_scdx_prod:
            pcba_site_prod=form.pcba_site_prod.data.strip().upper()

            if pcba_site_prod=='':
                msg = 'Pls put in a PCBA site name to proceed!'
                flash(msg,'warning')
                return redirect(url_for('scdx_api', _external=True, _scheme=http_scheme, viewarg1=1))

            msg='\n\n[Download SCDx-Production] - ' + login_user + ' ' + pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
            add_log_details(msg=msg)

            now = pd.Timestamp.now()
            f_path=base_dir_supply
            fname=pcba_site_prod + ' scr_oh_intransit(scdx-prod) ' + now.strftime('%m-%d %Hh%Mm ') + login_user + '.xlsx'

            try:
                df_por, df_oh, df_intransit, df_sourcing = collect_scr_oh_transit_from_scdx_prod(pcba_site_prod,'*')

                data_to_write = {'por': df_por,
                                 'df-oh': df_oh,
                                 'in-transit': df_intransit,
                                 'sourcing-rule': df_sourcing}

                write_data_to_excel(os.path.join(f_path, fname), data_to_write)

                add_log_summary(user=login_user, location='SCDx-API', user_action='Download SCDx-Prod', summary=fname)
                add_log_details(msg=' ' + fname)
                print(f_path)
                print(fname)

                return send_from_directory(f_path, fname, as_attachment=True)
            except Exception as e:
                msg = 'Error downloading supply data from SCDx-Prod! Check and ensure you put in the right PCBA org name, or wait and try again later.'
                flash(msg, 'warning')
                flash(str(e),'warning')
                add_log_summary(user=login_user, location='SCDx-API', user_action='Download SCDx-Prod', summary='Error: [' + pcba_site_prod + '] ' + str(e))
                add_log_details(msg='\n' + pcba_site_prod + '\n')
                traceback.print_exc(file=open(os.path.join(base_dir_logs, 'log_details.txt'), 'a+'))

                return redirect(url_for('scdx_api', _external=True, _scheme=http_scheme, viewarg1=1))

    return render_template('scdx_api.html',user=login_user,form=form,subtitle=' - SCDx API')
