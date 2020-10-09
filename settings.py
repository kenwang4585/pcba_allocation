import os
import getpass

# use below or from user selelction on UI
f_3a4='backlog3a4-pcba_allocation.csv'
f_supply='test_SCR+OH+Intransit_0924.xlsx'
sheet_scr='scr'
sheet_transit='in-transit'
sheet_oh='OH & transit-time'
pcba_site='FOL'

ranking_col=['priority_rank', 'ossd_offset', 'fcd_offset','rev_non_rev_rank','C_UNSTAGED_QTY', 'SO_SS','PO_NUMBER']

# backlog offset by transit pad will not consider ocean ship - assuming ocean is to cocver fcst demand but not backlog demand
transit_time={'FOL':{'FOC':2,'other':7},
            'JPE':{'JPE':1,'other':7}
           }


if getpass.getuser()=='ubuntu': # if it's on crate server
    base_dir_output = '/home/ubuntu/output_file'
    base_dir_upload='/home/ubuntu/upload_file'
else:
    base_dir_output = os.path.join(os.getcwd(),'output_file')
    base_dir_upload = os.path.join(os.getcwd(),'upload_file')

