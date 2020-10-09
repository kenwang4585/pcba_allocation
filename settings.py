f_3a4='backlog3a4-pcba_allocation.csv'
f_supply='test_SCR+OH+Intransit_0924.xlsx'
sheet_scr='scr'
sheet_transit='in-transit'
sheet_oh='OH & transit-time'

output_filename=''

ranking_col=['priority_rank', 'ossd_offset', 'fcd_offset','rev_non_rev_rank','C_UNSTAGED_QTY', 'SO_SS','PO_NUMBER']

# backlog offset by transit pad will not consider ocean ship - assuming ocean is to cocver fcst demand but not backlog demand
transit_time={'FOL':{'FOC':2,'other':7},
            'JPE':{'JPE':1,'other':7}
           }

pcba_site='FOL'