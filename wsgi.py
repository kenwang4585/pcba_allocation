# created by Ken wang, Oct, 2020


# add below matplotlib.use('Agg') to avoid this error: Assertion failed: (NSViewIsCurrentlyBuildingLayerTreeForDisplay()
# != currentlyBuildingLayerTree), function NSViewSetCurrentlyBuildingLayerTreeForDisplay
import matplotlib
matplotlib.use('Agg')

import time
from werkzeug.utils import secure_filename
from flask import flash,send_from_directory,render_template, request
from flask_settings import *
from functions import *
from pull_supply_data_from_db import collect_scr_oh_transit_from_scdx
from settings import *
#from db_add import add_user_log
#from db_read import read_table
#from db_delete import delete_record
import traceback
import gc


@app.route('/pcba_allocation', methods=['GET', 'POST'])
def allocation_run():
    form = UploadForm()
    # as these email valiable are redefined below in email_to_only check, thus have to use global to define here in advance
    # otherwise can't be used. (as we create new vaiables with _ suffix thus no need to set global variable)
    # global backlog_dashboard_emails

    if form.validate_on_submit():
        log_msg = []
        log_msg.append('\n\n[Making allocation] - ' + pd.Timestamp.now().strftime('%Y-%m-%d'))
        log_msg.append('User info: ' + request.headers.get('User-agent'))

        start_time=pd.Timestamp.now().strftime('%H:%M:%S')
        print('Start time:', start_time)
        log_msg.append('Start time: ' + start_time)

        # 通过条件判断及邮件赋值，开始执行任务
        pcba_site=form.org.data
        bu=form.bu.data
        bu_list=bu.strip().upper().split('/')
        log_msg.append('PCBA_SITE: ' + pcba_site)
        log_msg.append('BU: ' + bu.strip().upper())

        f_3a4 = form.file_3a4.data
        f_supply= form.file_supply.data
        ranking_logic=form.ranking_logic.data

        # 存储文件 - will save again with Org name in file name later
        #file_path_3a4 = os.path.join(app.config['UPLOAD_PATH'],'3a4.csv')
        #file_path_supply = os.path.join(app.config['UPLOAD_PATH'],'supply.xlsx')
        file_path_3a4 = os.path.join(base_dir_upload, secure_filename(f_3a4.filename))
        file_path_supply = os.path.join(base_dir_upload, secure_filename(f_supply.filename))
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
            return render_template('allocation_run.html', form=form)

       # 判断并定义ranking_col
        if ranking_logic == 'cus_sat':
            ranking_col = ranking_col_cust
        elif ranking_logic == 'max_rev':
            ranking_col = ranking_col_rev

        try:
            # 读取数据
            module='Reading input data'
            df_3a4, df_oh, df_transit, df_scr=read_data(file_path_3a4, file_path_supply, sheet_scr, sheet_oh, sheet_transit)

            # limit BU from 3a4 and df_scr for allocation
            df_3a4, df_scr=limit_bu_from_3a4_and_scr(df_3a4,df_scr,bu_list)
            if df_3a4.shape[0] == 0:
                flash('The 3a4 data is empty, check data source, or check if you put in a BU that does not exist!', 'warning')
                return render_template('allocation_run.html', form=form)

            if df_scr.shape[0] == 0:
                flash('The SCR data is empty, check data source, or check if you put in a BU that does not exist!', 'warning')
                return render_template('allocation_run.html', form=form)

            #### main program
            module='Main program for allocation'
            output_filename=pcba_allocation_main_program(df_3a4, df_oh, df_transit, df_scr, pcba_site, bu_list, ranking_col)

            flash('SCR allocation file created: {}! You can download accordingly.'.format(output_filename), 'success')
            finish_time=pd.Timestamp.now().strftime('%H:%M:%S')
            print('Finish run:',finish_time)
            log_msg.append('Finish time: ' + finish_time)

            # Write the log file
            log_msg='\n'.join(log_msg)
            with open(os.path.join(base_dir_output, 'log.txt'), 'a+') as file_object:
                file_object.write(log_msg)

        except Exception as e:
            try:
                del df_scr, df_3a4, df_oh, df_transit
                gc.collect()
            except:
                pass

            print(module,': ', e)
            traceback.print_exc()
            flash('Error encountered in module : {} - {}'.format(module,e),'warning')
            #Write the log file
            log_msg.append('ERROR!!!!' + pd.Timestamp.now().strftime('%H:%M:%S'))
            log_msg='\n'.join(log_msg)
            with open(os.path.join(base_dir_output, 'log.txt'), 'a+') as file_object:
                file_object.write(log_msg)
            traceback.print_exc(file=open(os.path.join(base_dir_output, 'log.txt'), 'a+'))

        # clear memory
        try:
            del df_scr, df_3a4, df_oh, df_transit
            gc.collect()
        except:
            pass

        return render_template('allocation_run.html', form=form)

    return render_template('allocation_run.html', form=form)

@app.route('/download', methods=['GET', 'POST'])
def allocation_download():
    form = FileDownloadForm()

    # output files
    file_list = os.listdir(base_dir_output)
    files = []
    creation_time = []
    file_size = []
    for file in file_list:
        if file[:1] != '.' and file[:1] != '~' and file!='log.txt':
            c_time = os.stat(os.path.join(base_dir_output, file)).st_ctime
            c_time = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(c_time))
            file_s = os.path.getsize(os.path.join(base_dir_output, file))
            if file_s > 1024 * 1024:
                file_s = str(round(file_s/(1024*1024),1)) + 'M'
            else:
                file_s = str(int(file_s / 1024)) + 'K'

            files.append(file)
            creation_time.append(c_time)
            file_size.append(file_s)
    df_output=pd.DataFrame({'File_name':files,'Creation_time':creation_time, 'File_size':file_size})
    df_output.sort_values(by='Creation_time',ascending=False,inplace=True)
    #files.sort(key=lambda x:x[-17:-5]) # 排序
    #files_output=files[::-1]

    # files upload
    file_list = os.listdir(base_dir_upload)
    files = []
    creation_time = []
    file_size = []
    for file in file_list:
        if file[:1] != '.' and file[:1] != '~':
            c_time = os.stat(os.path.join(base_dir_upload, file)).st_ctime
            c_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(c_time))
            file_s = os.path.getsize(os.path.join(base_dir_upload, file))
            if file_s > 1024 * 1024:
                file_s = str(round(file_s / (1024 * 1024), 1)) + 'M'
            else:
                file_s = str(int(file_s / 1024)) + 'K'

            files.append(file)
            creation_time.append(c_time)
            file_size.append(file_s)

    df_upload = pd.DataFrame({'File_name': files, 'Creation_time': creation_time, 'File_size': file_size})
    df_upload.sort_values(by='Creation_time', ascending=False, inplace=True)

    if form.validate_on_submit():
        log_msg = []
        log_msg.append('\n\n[Download/delete file] - ' + pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'))
        log_msg.append('User info: ' + request.headers.get('User-agent'))

        submit_download_supply=form.submit_download_supply.data
        submit_download_output=form.submit_download_output.data
        submit_download_uploaded=form.submit_download_uploaded.data

        pcba_site=form.pcba_site.data.strip().upper()
        fname_output = form.fname_output.data.strip()
        fname_uploaded=form.fname_uploaded.data.strip()

        if pcba_site not in ['FOL','FDO','JPE','FJZ']:
            msg="'{}' seems not a PCBA org??".format(pcba_site)
            flash(msg, 'warning')
            return render_template('allocation_download.html', form=form,
                                   data_header=['File_name', 'Creation_time', 'File_size'],
                                   files_output=df_output.values,
                                   files_uploaded=df_upload.values)
        """
        try:
            start_time = pd.Timestamp.now().strftime('%H:%M')
            add_user_log('', start_time, 0, start_time, 'Download 3A4', '',
                         'File name:{}'.format(fname), '', '')
        except:
            print('Adding user log error - user downloading: {}'.format(fname))
        """
        if submit_download_output:
            f_path=base_dir_output
            fname=fname_output
            log_msg.append('File name: ' + fname)
            download_supply_from_db=False
        elif submit_download_uploaded:
            f_path=base_dir_upload
            fname=fname_uploaded
            log_msg.append('File name: ' + fname)
            download_supply_from_db = False
        elif submit_download_supply:
            now = pd.Timestamp.now()
            f_path=base_dir_supply
            fname=pcba_site + ' SCR_OH_Intransit ' + now.strftime('%m-%d %H%M') + '.xlsx'
            log_msg.append('Download supply from DB')
            download_supply_from_db = True

        # Write the log file
        log_msg = '\n'.join(log_msg)
        with open(os.path.join(base_dir_output, 'log.txt'), 'a+') as file_object:
            file_object.write(log_msg)
        print(log_msg)

        # download/delete file
        if download_supply_from_db==False:
            try:
                if fname[-7:] == '-delete': # 删除文件
                    os.remove(os.path.join(f_path, fname[:-7]))
                    flash('This file has been delted: {}'.format(fname[:-7]),'success')
                    return render_template('allocation_download.html', form=form,
                                               data_header=['File_name', 'Creation_time', 'File_size'],
                                               files_output=df_output.values,
                                               files_uploaded=df_upload.values)
                else:
                    return send_from_directory(f_path, filename=fname, as_attachment=True)
            except Exception as e:
                msg = 'File not found! Check filename you input! ' + str(e)
                flash(msg, 'warning')
        else:
            try:
                df_scr, df_oh, df_intransit, df_sourcing_rule = collect_scr_oh_transit_from_scdx(pcba_site)
                data_to_write = {'scr': df_scr,
                                 'oh': df_oh,
                                 'in-transit': df_intransit,
                                 'sourcing_rule':df_sourcing_rule}

                write_data_to_excel(os.path.join(f_path,fname), data_to_write)
                return send_from_directory(f_path, filename=fname, as_attachment=True)
            except Exception as e:
                msg = 'Error downloading supply data from database! ' + str(e)
                flash(msg, 'warning')

    return render_template('allocation_download.html',form=form,
                           data_header=['File_name','Creation_time','File_size'],
                           files_output=df_output.values,
                           files_uploaded=df_upload.values)


@app.route('/about', methods=['GET'])
def allocation_about():
    ip = request.remote_addr
    print(ip)
    #print(request.headers)
    return render_template('allocation_about.html')

# Below is a dummy one
@app.route('/config',methods=['GET','POST'])
def config():
    form = ConfigForm()
    return render_template('config_control_panel_new.html', form=form)