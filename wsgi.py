# created by Ken wang, Oct, 2020


# add below matplotlib.use('Agg') to avoid this error: Assertion failed: (NSViewIsCurrentlyBuildingLayerTreeForDisplay()
# != currentlyBuildingLayerTree), function NSViewSetCurrentlyBuildingLayerTreeForDisplay
import matplotlib
matplotlib.use('Agg')

import time
from werkzeug.utils import secure_filename
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
        start_time_=pd.Timestamp.now()
        # 通过条件判断及邮件赋值，开始执行任务
        org=form.org.data
        org_list=org.strip().upper().split('/')

        bu=form.bu.data
        bu_list=bu.strip().upper().split('/')

        class_code_exclusion=form.class_code_exclusion.data
        class_code_exclusion=class_code_exclusion.strip().split('/')
        class_code_exclusion=[x+'-' for x in class_code_exclusion]

        customer=form.customer.data
        customer_list=customer.strip().upper().split('/')

        supply_source=form.supply_source.data
        f_3a4 = form.file_3a4.data
        f_supply= form.file_supply.data
        f_exception=form.file_exception.data
        ranking_logic=form.ranking_logic.data

        # 存储文件 - will save again with Org name in file name later
        #file_path_3a4 = os.path.join(app.config['UPLOAD_PATH'],'3a4.csv')
        #file_path_supply = os.path.join(app.config['UPLOAD_PATH'],'supply.xlsx')
        file_path_3a4 = os.path.join(base_dir_upload, '3a4.csv')
        file_path_supply = os.path.join(base_dir_upload, 'supply.xlsx')

        f_3a4.save(file_path_3a4)
        f_supply.save(file_path_supply)
        if f_exception:
            file_path_exception = os.path.join(app.config['UPLOAD_PATH'], 'exception.xlsx')
            f_exception.save(file_path_exception)
        else:
            file_path_exception=None

        if supply_source=='CM':
            fname_path_ct2r = os.path.join(app.config['UPLOAD_PATH'], 'CPN CT2R.xlsx')
        else:
            fname_path_ct2r=''

        if ranking_logic=='cus_sat':
            ranking_col=['priority_rank', 'ORIGINAL_FCD_NBD_DATE', 'CURRENT_FCD_NBD_DATE','rev_non_rev_rank','C_UNSTAGED_QTY', 'SO_SS','PO_NUMBER']
        elif ranking_logic=='max_rev':
            ranking_col = ['priority_rank', 'ss_rev_rank', 'ORIGINAL_FCD_NBD_DATE', 'CURRENT_FCD_NBD_DATE','rev_non_rev_rank', 'SO_SS', 'PO_NUMBER']



        try:
            # check file format by reading headers
            module='checking file format'
            missing_3a4_col, missing_supply_col = check_input_file_format(supply_source,file_path_supply,file_path_3a4)
            if len(missing_3a4_col) + len(missing_supply_col) >0:
                flash('Check your files! Missing columns in the file you updated, you might have uploaded wrong files. 3A4: {}. Kinaxis supply: {}'.format(missing_3a4_col,missing_supply_col),'warning')
                return render_template('ctb_run.html', form=form)

            # 读取3a4,选择相关的org/bu,并添加exception
            module='read_3a4_and_limit_org_bu_and_add_exception'
            df_3a4 = read_3a4_and_limit_org_bu_and_add_exception(file_path_3a4, bu_list, org_list,
                                                             fname_exception=file_path_exception)

            # 读取supply及相关数据并处理
            module = 'read_supply_and_process'
            df_supply = read_supply_and_process(supply_source, file_path_supply, fname_path_ct2r,class_code_exclusion)

            # Rank backlog，allocate supply, and make the summaries
            module='main_program_all'
            output_filename=main_program_all(df_3a4, org_list,bu_list,customer_list,ranking_col, df_supply, qend_list, output_col)
            flash('CTB file created:{}! You can download accordingly.'.format(output_filename), 'success')
        except Exception as e:
            try:
                del df_supply, df_3a4
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
            del df_supply,df_3a4
            gc.collect()
        except:
            pass

        return render_template('allocation_run.html', form=form)

    return render_template('allocation_run.html', form=form)

@app.route('/download', methods=['GET', 'POST'])
def allocation_download():

    return render_template('allocation_download.html')


@app.route('/about', methods=['GET', 'POST'])
def allocation_about():
    return render_template('allocation_about.html')

