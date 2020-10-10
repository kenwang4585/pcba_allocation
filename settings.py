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
transit_time={'FOL':{'FOC':1,
                     'FTX':7,
                     'FCZ':9,
                     'FJZ':10,
                     'SJZ':10,
                     'JMX':9,
                     'FGU':9,
                     'JPE':4,
                     'JPI':5,
                     'FSJ':6,
                     'FE':9,
                     'other':7},
            'JPE':{'JPE':1,'other':7}
           }
# within below days transit ETA considered as OH
close_eta_cutoff_criteria=15
# Far ETA eligible to backward fulfill PO: offset OSSD by deducting below days
eta_backward_offset_days=15




if getpass.getuser()=='ubuntu': # if it's on crate server
    base_dir_output = '/home/ubuntu/output_file'
    base_dir_upload='/home/ubuntu/upload_file'
else:
    base_dir_output = os.path.join(os.getcwd(),'output_file')
    base_dir_upload = os.path.join(os.getcwd(),'upload_file')

