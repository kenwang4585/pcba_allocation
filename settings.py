import os
import getpass

# use below or from user selelction on UI
pcba_site_list=['FOL', 'FDO', 'JPE', 'FJZ','NCB','FJZ','JMX','FGU']

super_user='kwang2'


# within below days transit ETA considered as OH
close_eta_cutoff_criteria=15
# Far ETA eligible to backward fulfill PO: offset OSSD by deducting below days
eta_backward_offset_days=10


if getpass.getuser()=='ubuntu': # if it's on crate server
    base_dir_output = '/home/ubuntu/output_file'
    base_dir_upload='/home/ubuntu/upload_file'
    base_dir_supply='/home/ubuntu/supply_file'
    base_dir_logs = '/home/ubuntu/logs'
    base_dir_trash = '/home/ubuntu/trash_file'
    base_dir_share=os.path.join(os.getcwd(), 'share_file')
else:
    base_dir_output = os.path.join(os.getcwd(),'output_file')
    base_dir_upload = os.path.join(os.getcwd(),'upload_file')
    base_dir_supply = os.path.join(os.getcwd(), 'supply_file')
    base_dir_logs = os.path.join(os.getcwd(), 'logs')
    base_dir_trash= os.path.join(os.getcwd(), 'trash_file')
    base_dir_share=os.path.join(os.getcwd(), 'share_file')

output_col_3a4=['ORGANIZATION_CODE', 'BUSINESS_UNIT', 'PRODUCT_FAMILY', 'SO_SS', 'PO_NUMBER','distinct_po_filter', 'PRODUCT_ID',
           'BOM_PN','ADDRESSABLE_FLAG','priority_cat','priority_rank','ss_overall_rank','ORDER_HOLDS','END_CUSTOMER_NAME','SHIP_TO_CUSTOMER_NAME',
           'ORIGINAL_FCD_NBD_DATE','ossd_offset', 'CURRENT_FCD_NBD_DATE','C_UNSTAGED_DOLLARS',
            'ss_unstg_rev','REVENUE_NON_REVENUE',
            'ORDERED_QUANTITY','C_UNSTAGED_QTY','SPLIT','C_UNSTAGED_QTY_SPLIT','PACKOUT_QUANTITY','FLB_TAN','PROGRAM']

col_3a4_must_have=['ORGANIZATION_CODE', 'BUSINESS_UNIT', 'PRODUCT_FAMILY', 'SO_SS', 'PO_NUMBER','PRODUCT_ID',
           'TAN','ADDRESSABLE_FLAG','ORDER_HOLDS','END_CUSTOMER_NAME','SHIP_TO_CUSTOMER_NAME',
           'CURRENT_FCD_NBD_DATE','ORIGINAL_FCD_NBD_DATE', 'TARGET_SSD','LT_TARGET_FCD','PROGRAM','C_UNSTAGED_DOLLARS',
            'ORDERED_QUANTITY','C_UNSTAGED_QTY','PACKOUT_QUANTITY','FLB_TAN','REVENUE_NON_REVENUE']

col_transit_must_have=['DF_site','TAN','BU','ETA_date','In-transit_quantity']
col_oh_must_have=['DF_site','TAN','BU','OH']
col_scr_must_have=['planningOrg','TAN','BU','date','quantity']

# rank sequences
ranking_col_cust = ['priority_rank', 'ossd_offset', 'PROGRAM',
                           'C_UNSTAGED_QTY', 'rev_non_rev_rank', 'SO_SS', 'PO_NUMBER']

# email options
cisco_recipients=['manfan@cisco.com',
                  'kwang2@cisco.com',
                  'clarwang@cisco.com',
                  'seyang@cisco.com',
                  'jimmyzh@cisco.com',
                  'lilixiao@cisco.com',
                  'wibo@cisco.com',
                  'brigwang@cisco.com',
                  'pspforum@cisco.com',
                  'irlee@cisco.com',
                  'adwilkin@cisco.com',
                  'scn_pmm@cisco.com',
                  ]

