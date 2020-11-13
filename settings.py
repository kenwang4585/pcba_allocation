import os
import getpass

# use below or from user selelction on UI
f_3a4='backlog3a4-pcba_allocation.csv'
f_supply='test_SCR+OH+Intransit_0924.xlsx'
sheet_scr='scr'
sheet_transit='in-transit'
sheet_oh='oh'
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
                     'FVE':9,
                     'FDO':4,
                     'SHK':2,
                     'other':7},
                'JPE':{'JPE':1,
                       'other':7}
                }
# within below days transit ETA considered as OH
close_eta_cutoff_criteria=15
# Far ETA eligible to backward fulfill PO: offset OSSD by deducting below days
eta_backward_offset_days=10




if getpass.getuser()=='ubuntu': # if it's on crate server
    base_dir_output = '/home/ubuntu/output_file'
    base_dir_upload='/home/ubuntu/upload_file'
    base_dir_supply='/home/ubuntu/supply_file'
else:
    base_dir_output = os.path.join(os.getcwd(),'output_file')
    base_dir_upload = os.path.join(os.getcwd(),'upload_file')
    base_dir_supply = os.path.join(os.getcwd(), 'supply_file')


output_col_3a4=['ORGANIZATION_CODE', 'BUSINESS_UNIT', 'PRODUCT_FAMILY', 'SO_SS', 'PO_NUMBER','distinct_po_filter', 'PRODUCT_ID',
           'TAN','BOM_PN','ADDRESSABLE_FLAG','priority_cat','priority_rank','ss_overall_rank','ORDER_HOLDS','END_CUSTOMER_NAME','SHIP_TO_CUSTOMER_NAME',
           'ORIGINAL_FCD_NBD_DATE','ossd_offset','CURRENT_FCD_NBD_DATE','fcd_offset', 'TARGET_SSD','LT_TARGET_FCD','C_UNSTAGED_DOLLARS',
            'ss_unstg_rev',
            'ORDERED_QUANTITY','C_UNSTAGED_QTY','PACKOUT_QUANTITY','FLB_TAN']

col_3a4_must_have=['ORGANIZATION_CODE', 'BUSINESS_UNIT', 'PRODUCT_FAMILY', 'SO_SS', 'PO_NUMBER','PRODUCT_ID',
           'TAN','ADDRESSABLE_FLAG','ORDER_HOLDS','END_CUSTOMER_NAME','SHIP_TO_CUSTOMER_NAME',
           'CURRENT_FCD_NBD_DATE','ORIGINAL_FCD_NBD_DATE', 'TARGET_SSD','LT_TARGET_FCD','C_UNSTAGED_DOLLARS',
            'ORDERED_QUANTITY','C_UNSTAGED_QTY','PACKOUT_QUANTITY','FLB_TAN']
col_transit_must_have=['planningOrg','TAN','BU','ETA_date','In-transit_quantity']
col_oh_must_have=['planningOrg','TAN','BU','OH']
col_scr_must_have=['planningOrg','TAN','BU','SCRDate','SCRQuantity']

# Revenue rank
ranking_col_cust = ['priority_rank', 'ossd_offset', 'fcd_offset',
                           'C_UNSTAGED_QTY', 'rev_non_rev_rank', 'SO_SS', 'PO_NUMBER']
ranking_col_rev = ['priority_rank', 'ss_rev_rank', 'ossd_offset', 'fcd_offset',
                           'rev_non_rev_rank', 'SO_SS', 'PO_NUMBER']
