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
from pull_supply_data_from_db import collect_scr_oh_transit_from_scdx
from settings import *
from sending_email import *
from db_add import add_user_log
from db_read import read_table
#from db_delete import delete_record
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
        login_user=''
        login_name=''

    if login_user!='' and login_user!='kwang2':
        add_user_log(user=login_user,location='Allocation',user_action='visit',summary='')

    if form.validate_on_submit():
        start_time = pd.Timestamp.now()
        print('start to run: {}'.format(start_time.strftime('%Y-%m-%d %H:%M')))
        log_msg = []
        log_msg.append('\n\n[' + login_user + '] ' + start_time.strftime('%Y-%m-%d %H:%M'))
        #log_msg.append('User info: ' + request.headers.get('User-agent'))

        # 通过条件判断及邮件赋值，开始执行任务
        pcba_site=form.org.data
        bu=form.bu.data
        bu_list=bu.strip().upper().split('/')
        email_option=form.email_option.data
        f_3a4 = form.file_3a4.data
        f_supply= form.file_supply.data
        ranking_logic=form.ranking_logic.data # This is not shown on the UI - take the default value set

        log_msg.append('PCBA_SITE: ' + pcba_site)
        log_msg.append('BU: ' + bu.strip().upper())
        log_msg.append('Email option: ' + email_option)

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
                                                                        sheet_transit,sheet_oh,sheet_scr)
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
            return redirect(url_for('allocation_run',_external=True,_scheme='https',viewarg1=1))

       # 判断并定义ranking_col
        if ranking_logic == 'cus_sat':
            ranking_col = ranking_col_cust
        #elif ranking_logic == 'max_rev':
        #    ranking_col = ranking_col_rev

        try:
            # 读取数据
            module='Reading input data'
            df_3a4, df_oh, df_transit, df_scr, df_sourcing=read_data(file_path_3a4, file_path_supply, sheet_scr, sheet_oh, sheet_transit, sheet_sourcing)

            # limit BU from 3a4 and df_scr for allocation
            df_3a4, df_scr=limit_bu_from_3a4_and_scr(df_3a4,df_scr,bu_list)
            if df_3a4.shape[0] == 0:
                flash('The 3a4 data is empty, check data source, or check if you put in a BU that does not exist!', 'warning')
                return redirect(url_for('allocation_run',_external=True,_scheme='https',viewarg1=1))

            if df_scr.shape[0] == 0:
                flash('The SCR data is empty, check data source, or check if you put in a BU that does not exist!', 'warning')
                return redirect(url_for('allocation_run',_external=True,_scheme='https',viewarg1=1))

            #### main program
            module='Main program for allocation'
            output_filename=pcba_allocation_main_program(df_3a4, df_oh, df_transit, df_scr, df_sourcing, pcba_site, bu_list, ranking_col,login_user)
            flash('Allocation file created for downloading: {} '.format(output_filename), 'success')

            # send result by email
            module = 'send_allocation_result'
            msg=send_allocation_result(email_option,output_filename,secure_filename(f_3a4.filename),secure_filename(f_supply.filename),
                                       size_3a4,size_supply,bu_list,pcba_site,login_user)
            flash(msg, 'success')

            finish_time = pd.Timestamp.now()
            processing_time = round((finish_time - start_time).total_seconds() / 60, 1)
            log_msg.append('Processing time: ' + str(processing_time) + ' min')
            print('Finish run:',finish_time.strftime('%Y-%m-%d %H:%M'))

            # Write the log file
            summary='; '.join(log_msg)
            add_user_log(user=login_user,location='Allocation',user_action='Make allocation',summary=summary)

        except Exception as e:
            try:
                del df_scr, df_3a4, df_oh, df_transit
                gc.collect()
            except:
                print('')

            print(module,': ', e)
            traceback.print_exc()
            log_msg.append(str(e))
            flash('Error encountered in module : {} - {}'.format(module,e),'warning')
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
            del df_scr, df_3a4, df_oh, df_transit
            gc.collect()
        except:
            print('')

        return redirect(url_for('allocation_run',_external=True,_scheme='https',viewarg1=1))

    return render_template('allocation_run.html', form=form, user=login_name)

@app.route('/download', methods=['GET', 'POST'])
def allocation_download():
    form = FileDownloadForm()
    login_user = request.headers.get('Oidc-Claim-Sub')
    login_name = request.headers.get('Oidc-Claim-Fullname')

    if login_user == None:
        login_user = ''
        login_name = ''

    if login_user!='' and login_user!='kwang2':
        add_user_log(user=login_user, location='Download', user_action='Visit', summary='')

    # output files
    df_output=get_file_info_on_drive(base_dir_output)
    df_upload=get_file_info_on_drive(base_dir_upload)

    if form.validate_on_submit():
        log_msg = []
        log_msg.append('\n\n[Download/delete file] - ' + pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'))

        pcba_site=form.pcba_site.data.strip().upper()

        now = pd.Timestamp.now()
        f_path=base_dir_supply
        fname=pcba_site + ' SCR_OH_Intransit ' + now.strftime('%m-%d %Hh%Mm ') + login_user + '.xlsx'
        log_msg.append('Download supply from DB')

        if pcba_site not in ['FOL', 'FDO', 'JPE', 'FJZ','NCB','FJZ','JMX','FGU']:
            msg = "'{}' is not a PCBA org.".format(pcba_site)
            flash(msg, 'warning')
            return redirect(url_for('allocation_download',_external=True,_scheme='https',viewarg1=1))
        try:
            df_scr, df_oh, df_intransit, df_sourcing_rule = collect_scr_oh_transit_from_scdx(pcba_site)
            data_to_write = {'scr': df_scr,
                             'oh': df_oh,
                             'in-transit': df_intransit,
                             'sourcing_rule': df_sourcing_rule}

            write_data_to_excel(os.path.join(f_path, fname), data_to_write)
            add_user_log(user=login_user, location='Download', user_action='Download SCDx', summary='Success: ' + pcba_site)

            return send_from_directory(f_path, filename=fname, as_attachment=True)
        except Exception as e:
            msg = 'Error downloading supply data from database! ' + str(e)
            flash(msg, 'warning')
            add_user_log(user=login_user, location='Download', user_action='Download SCDx', summary='Error: [' + pcba_site + '] ' + e)

    return render_template('allocation_download.html',form=form,
                           files_output=df_output.values,
                           files_uploaded=df_upload.values,
                           user=login_name)


# Below did now work out somehow - NOT USED
@app.route('/delete/<path:file_path>',methods=['POST'])
def delete_file(file_path):
    form=AdminForm()
    password=form.password.data

    if form.validate_on_submit():
        print('enter')
        if password==os.getenv('xxxxx'):
            msg='Not authorized!'
            flash(msg,'warning')
            return redirect(url_for("allocation_admin"))

        os.remove(file_path)
        msg = 'File deleted!'
        flash(msg, 'warning')

    return redirect(url_for("allocation_admin"))


@app.route('/<path:file_path>',methods=['GET'])
def download_file(file_path):
    f_path,fname = os.path.split(file_path)
    f_path='/' + f_path

    login_user = request.headers.get('Oidc-Claim-Sub')
    login_name = request.headers.get('Oidc-Claim-Fullname')
    if login_user == None:
        login_user = ''
        login_name = ''
    add_user_log(user=login_user, location='Download', user_action='Download file', summary=fname)

    if fname=='favicon.ico':
        log_msg='\n\nSomehow fname==favicon... filepath: ' + file_path
        with open(os.path.join(base_dir_logs, 'error_log.txt'), 'a+') as file_object:
            file_object.write(log_msg)

    return send_from_directory(f_path, filename=fname, as_attachment=True)

@app.route('/admin', methods=['GET','POST'])
def allocation_admin():
    form = AdminForm()
    login_user=request.headers.get('Oidc-Claim-Sub')
    login_name = request.headers.get('Oidc-Claim-Fullname')
    if login_user == None:
        login_user = ''
        login_name = ''

    if login_user!='' and login_user!='kwang2':
        add_user_log(user=login_user, location='Admin', user_action='Visit', summary='Warning')
        return redirect(url_for('allocation_run',_external=True,_scheme='https',viewarg1=1))

    # get file info
    df_output=get_file_info_on_drive(base_dir_output)
    df_upload=get_file_info_on_drive(base_dir_upload)
    df_supply=get_file_info_on_drive(base_dir_supply)
    df_logs=get_file_info_on_drive(base_dir_logs)

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
            return redirect(url_for('allocation_admin',_external=True,_scheme='https',viewarg1=1))

    return render_template('allocation_admin.html',form=form,
                           files_output=df_output.values,
                           files_uploaded=df_upload.values,
                           files_supply=df_supply.values,
                           files_log=df_logs.values,
                           log_details=df_log_detail.values,
                           user=login_name)

# Below is a dummy one
@app.route('/config',methods=['GET','POST'])
def config():
    form = ConfigForm()
    return render_template('config_control_panel_new.html', form=form)