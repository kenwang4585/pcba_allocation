# created by Ken wang, Oct, 2020


# add below matplotlib.use('Agg') to avoid this error: Assertion failed: (NSViewIsCurrentlyBuildingLayerTreeForDisplay()
# != currentlyBuildingLayerTree), function NSViewSetCurrentlyBuildingLayerTreeForDisplay
import matplotlib
matplotlib.use('Agg')

import time
#from werkzeug.utils import secure_filename
from flask import flash,send_from_directory,render_template
from flask_settings import *
from functions import *
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
    program_log = []
    user_selection = []
    time_details=[]




    if form.validate_on_submit():
        print('Start run:',(pd.Timestamp.now()).strftime('%Y-%m-%d %H:%M:%S'))
        start_time_=pd.Timestamp.now()
        # 通过条件判断及邮件赋值，开始执行任务
        pcba_site=form.org.data

        bu=form.bu.data
        bu_list=bu.strip().upper().split('/')

        f_3a4 = form.file_3a4.data
        f_supply= form.file_supply.data
        ranking_logic=form.ranking_logic.data


        # 存储文件 - will save again with Org name in file name later
        #file_path_3a4 = os.path.join(app.config['UPLOAD_PATH'],'3a4.csv')
        #file_path_supply = os.path.join(app.config['UPLOAD_PATH'],'supply.xlsx')
        file_path_3a4 = os.path.join(base_dir_upload, '3a4.csv')
        file_path_supply = os.path.join(base_dir_upload, 'supply.xlsx')

        # save the files to server
        f_3a4.save(file_path_3a4)
        f_supply.save(file_path_supply)

        # check data format
        sheet_name_msg, msg_3a4, msg_transit, msg_oh, msg_scr = check_input_file_format(file_path_3a4, file_path_supply,
                                                                        col_3a4_must_have, col_transit_must_have,
                                                                        col_oh_must_have, col_scr_must_have,
                                                                        sheet_transit,sheet_oh,sheet_scr)
        if sheet_name_msg!='':
            flash(sheet_name_msg,'warning')
        if msg_3a4!='':
            flash(msg_3a4,'warning')
        if msg_transit!='':
            flash(msg_transit,'warning')
        if msg_oh!='':
            flash(msg_oh,'warning')
        if msg_scr!='':
            flash(msg_scr,'warning')

        if sheet_name_msg!='' or msg_3a4!='' or msg_transit!='' or msg_oh!='' or msg_scr!='':
            return render_template('allocation_run.html', form=form)

        if ranking_logic=='cus_sat':
            ranking_col=['priority_rank', 'ORIGINAL_FCD_NBD_DATE', 'CURRENT_FCD_NBD_DATE','rev_non_rev_rank','C_UNSTAGED_QTY', 'SO_SS','PO_NUMBER']
        elif ranking_logic=='max_rev':
            ranking_col = ['priority_rank', 'ss_rev_rank', 'ORIGINAL_FCD_NBD_DATE', 'CURRENT_FCD_NBD_DATE','rev_non_rev_rank', 'SO_SS', 'PO_NUMBER']


        try:
            # check file format by reading headers
            """
            module='checking file format'
            missing_3a4_col, missing_supply_col = check_input_file_format(supply_source,file_path_supply,file_path_3a4)
            if len(missing_3a4_col) + len(missing_supply_col) >0:
                flash('Check your files! Missing columns in the file you updated, you might have uploaded wrong files. 3A4: {}. Kinaxis supply: {}'.format(missing_3a4_col,missing_supply_col),'warning')
                return render_template('ctb_run.html', form=form)
            """
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
            print('Finish run:',(pd.Timestamp.now()).strftime('%Y-%m-%d %H:%M:%S'))
        except Exception as e:
            try:
                del df_scr, df_3a4, df_oh, df_transit
                gc.collect()
            except:
                pass

            print(module,': ', e)
            traceback.print_exc()
            flash('Error encountered in module : {} - {}'.format(module,e),'warning')
            #summarize time
            error_log_file = os.path.join(base_dir_output, 'error log.txt')

            error_heading = '\n['+ pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S') + ']\n'
            with open(error_log_file, 'a+') as file_object:
                file_object.write(error_heading)
            traceback.print_exc(file=open(error_log_file, 'a+'))

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

    now = time.time()
    # output files
    file_list = os.listdir(base_dir_output)
    files = []
    for file in file_list:
        if file[:1] != '.' and file[:1] != '~':
            #m_time = os.stat(os.path.join(f_path, file)).st_mtime
            files.append(file)
    files.sort(key=lambda x:x[-17:-5]) # 排序
    files_output=files[::-1]

    # files upload
    file_list = os.listdir(base_dir_upload)
    files = []
    for file in file_list:
        if file[:1] != '.' and file[:1] != '~':
            # m_time = os.stat(os.path.join(f_path, file)).st_mtime
            #c_time=time.ctime(os.path.getctime(file))
            files.append(file)


    files_uploaded = files


    if form.validate_on_submit():
        submit_download_output=form.submit_download_output.data
        submit_download_uploaded=form.submit_download_uploaded.data

        fname_output = form.fname_output.data.strip()
        fname_uploaded=form.fname_uploaded.data.strip()

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
        elif submit_download_uploaded:
            f_path=base_dir_upload
            fname=fname_uploaded

        try:
            return send_from_directory(f_path, filename=fname, as_attachment=True)
        except Exception as e:
            msg = 'File not found! Check filename you input! ' + str(e)
            flash(msg, 'warning')

    return render_template('allocation_download.html',form=form,files_output=files_output,files_uploaded=files_uploaded)


@app.route('/about', methods=['GET'])
def allocation_about():
    return render_template('allocation_about.html')

