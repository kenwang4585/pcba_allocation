# created by Ken wang, Oct, 2020

import pandas as pd
import re
import numpy as np
import math
from settings import *
from smartsheet_handler import SmartSheetClient

def read_backlog_priority_from_smartsheet():
    '''
    Read backlog priorities from smartsheet
    :return:
    '''
    # 从smartsheet读取backlog
    token = os.getenv('ALLOCATION_TOKEN')
    sheet_id = os.getenv('PRIORITY_ID')
    proxies = None  # for proxy server
    smartsheet_client = SmartSheetClient(token, proxies)
    df_smart = smartsheet_client.get_sheet_as_df(sheet_id, add_row_id=True, add_att_id=False)

    df_smart.drop_duplicates('SO_SS', keep='last', inplace=True)
    df_smart = df_smart[(df_smart.SO_SS.notnull()) & (df_smart.Ranking.notnull())]
    ss_priority = {}
    for row in df_smart.itertuples():
        try: # in case error input of non-num ranking
            ss_priority[row.SO_SS] = float(row.Ranking)
        except:
            print('{} has a wrong ranking#: {}.'.format(row.SO_SS,row.Ranking) )

    return ss_priority

def read_tan_group_mapping_from_smartsheet():
    '''
    Read TAN group mapping from smartsheet; change to version during processing.
    :return:
    '''
    # 从smartsheet读取backlog
    token = os.getenv('ALLOCATION_TOKEN')
    sheet_id = os.getenv('TAN_GROUP_ID')
    proxies = None  # for proxy server
    smartsheet_client = SmartSheetClient(token, proxies)
    df_smart = smartsheet_client.get_sheet_as_df(sheet_id, add_row_id=True, add_att_id=False)

    df_smart = df_smart[(df_smart.Group_name.notnull()) & (df_smart.TAN.notnull())]

    #  chagne to versionless
    df_smart=change_pn_to_versionless(df_smart, pn_col='TAN')

    tan_group = {}
    for row in df_smart.itertuples():
        tan_group[row.TAN] = row.Group_name

    return tan_group


def read_exceptional_intransit_from_smartsheet():
    '''
    Read the exceptional in transit data from smartsheet for TAN shipping to other partner (like FOL to FGU)
    which is missing from the SCDx .
    :return:
    '''
    # 从smartsheet读取backlog
    token = os.getenv('ALLOCATION_TOKEN')
    sheet_id = os.getenv('INTRANSIT_ID')
    proxies = None  # for proxy server
    smartsheet_client = SmartSheetClient(token, proxies)
    df_smart = smartsheet_client.get_sheet_as_df(sheet_id, add_row_id=True, add_att_id=False)

    df_smart = df_smart[(df_smart.planningOrg.notnull()) & (df_smart.TAN.notnull())
                        & (df_smart.ETA_date.notnull()) & (df_smart['In-transit_quantity'].notnull())]

    df_smart.loc[:,'ETA_date']=df_smart.ETA_date.astype('datetime64[ns]')
    df_smart.loc[:, 'In-transit_quantity'] = df_smart['In-transit_quantity'].astype(int)

    return df_smart



def change_supply_to_versionless_and_addup_supply(df, org_col='planningOrg', pn_col='TAN'):
    """
    Change PN in supply or OH df into versionless. Add up the qty into the versionless PN.
    :param df: the supply df or oh df
    :param pn_col: name of the PN col. In Cm supply file it's PN, in Kinaxis file it's TAN.
    :return:
    """

    regex = re.compile(r'\d{2,3}-\d{4,7}')

    df.reset_index(inplace=True)

    # convert to versionless and add temp col
    df.loc[:, pn_col] = df[pn_col].map(lambda x: regex.search(x).group())
    df.loc[:, 'org_pn'] = df[org_col] + '_' + df[pn_col]

    # add up the duplicate PN (due to multiple versions)
    df.sort_values(by=['org_pn'], inplace=True)
    dup_pn = df[df.duplicated(['org_pn'])]['org_pn'].unique()
    df_sum = pd.DataFrame(columns=df.columns)

    df_sum.set_index([org_col, pn_col, 'org_pn'], inplace=True)
    df.set_index([org_col, pn_col, 'org_pn'], inplace=True)

    for org_pn in dup_pn:
        # print(df_supply[df_supply.PN==pn].sum(axis=1).sum())
        df_sum.loc[(org_pn[:3], org_pn[4:], org_pn), :] = df.loc[(org_pn[:3], org_pn[4:], org_pn), :].sum(axis=0)

    df.reset_index(inplace=True)
    df.set_index('org_pn', inplace=True)
    df.drop(dup_pn, axis=0, inplace=True)
    df.reset_index(inplace=True)
    df.set_index([org_col, pn_col, 'org_pn'], inplace=True)
    # print(df.columns)
    # df.drop(['level_0','index'],axis=1,inplace=True)
    df = pd.concat([df, df_sum])
    df.reset_index(inplace=True)
    df.drop(['org_pn'], axis=1, inplace=True)
    df.set_index([org_col, pn_col], inplace=True)

    return df


def change_pn_to_versionless(df, pn_col='TAN'):
    """
    Change PN to versionless.
    :param df: the supply df or oh df
    :param pn_col: name of the PN col. In Cm supply file it's PN, in Kinaxis file it's TAN.
    :return:
    """

    regex = re.compile(r'\d{2,3}-\d{4,7}')

    df.reset_index(inplace=True)

    # convert to versionless and add temp col
    df.loc[:, pn_col] = df[pn_col].map(lambda x: regex.search(x).group())

    return df



def change_pn_to_group_number(df,tan_group,pn_col='TAN'):
    """
    Change PN to group for those with a group mapping
    """

    df.loc[:,pn_col]=df[pn_col].map(lambda x: tan_group[x] if x in tan_group.keys() else x)

    return df



def add_up_supply_by_pn(df,org_col='planningOrg', pn_col='TAN'):
    """
    Add up qty (in pivoted format) for supply, OH, transit, etc.
    Param df: the datafram
    Param corg_col: col for org
    Param pn_col: col for PN
    """
    df.loc[:, 'org_pn'] = df[org_col] + '_' + df[pn_col]

    # add up the duplicate PN (due to multiple versions)
    df.sort_values(by=['org_pn'], inplace=True)
    dup_pn = df[df.duplicated(['org_pn'])]['org_pn'].unique()
    df_sum = pd.DataFrame(columns=df.columns)

    df_sum.set_index([org_col, pn_col, 'org_pn'], inplace=True)
    df.set_index([org_col, pn_col, 'org_pn'], inplace=True)

    for org_pn in dup_pn:
        # print(df_supply[df_supply.PN==pn].sum(axis=1).sum())
        df_sum.loc[(org_pn[:3], org_pn[4:], org_pn), :] = df.loc[(org_pn[:3], org_pn[4:], org_pn), :].sum(axis=0)

    df.reset_index(inplace=True)
    df.set_index('org_pn', inplace=True)
    df.drop(dup_pn, axis=0, inplace=True)
    df.reset_index(inplace=True)
    df.set_index([org_col, pn_col, 'org_pn'], inplace=True)
    # print(df.columns)
    # df.drop(['level_0','index'],axis=1,inplace=True)
    df = pd.concat([df, df_sum])
    df.reset_index(inplace=True)
    df.drop(['org_pn'], axis=1, inplace=True)
    df.set_index([org_col, pn_col], inplace=True)

    return df



def update_date_with_transit_pad(x, y, transit_time, pcba_site):
    """
    offset transit time to a given date column
    """
    if x in transit_time[pcba_site].keys():
        return y - pd.Timedelta(days=transit_time[pcba_site][x])
    else:
        return y - pd.Timedelta(days=transit_time[pcba_site]['other'])


def generate_df_order_bom_from_flb_tan_col(df_3a4, supply_dic_tan, tan_group):
    """
    Generate the BOM usage file from the FLB_TAN col
    :param df_3a4:
    :return:
    """
    regex_pn = re.compile(r'\d{2,3}-\d{4,7}')
    regex_usage = re.compile(r'\([0-9.]+\)')

    df_flb_tan = df_3a4[df_3a4.FLB_TAN.notnull()][['PO_NUMBER', 'PRODUCT_ID', 'ORDERED_QUANTITY', 'FLB_TAN']].copy()
    # df_flb_tan.drop_duplicates(['PRODUCT_ID'], keep='first', inplace=True)

    po_list = []
    pn_list = []
    usage_list = []
    for row in df_flb_tan.itertuples(index=False):
        po = row.PO_NUMBER
        flb_tan = row.FLB_TAN
        # order_qty = row.ORDERED_QUANTITY
        flb_tan = flb_tan.split('|')

        for item in flb_tan:
            try:
                pn = regex_pn.search(item).group()
                usage = regex_usage.search(item).group()
                usage = float(usage[1:-1])

                if pn in tan_group.keys():
                    pn = tan_group[pn]
                    po_list.append(po)
                    pn_list.append(pn)
                    usage_list.append(usage)
                elif pn in supply_dic_tan.keys():
                    po_list.append(po)
                    pn_list.append(pn)
                    usage_list.append(usage)

            except:
                pass
                # print(po_list)

    # print(po_list)
    df_order_bom_from_flb = pd.DataFrame({'PO_NUMBER': po_list, 'BOM_PN': pn_list, 'BOM_PN_QTY': usage_list})

    return df_order_bom_from_flb


def update_order_bom_to_3a4(df_3a4, df_order_bom):
    """
    Add PN into 3a4 based on BOM
    :param df_3a4:
    :param df_bom:
    :return: df_3a4, df_missing_bom_pid
    """
    # add the BOM PN through merge method
    df_3a4 = pd.merge(df_3a4, df_order_bom, left_on='PO_NUMBER', right_on='PO_NUMBER', how='left')

    """
    # PID missing BOM data
    missing_bom_pid = df_3a4[df_3a4.TAN.notnull() & df_3a4.PN.isnull()].PRODUCT_ID.unique()
    df_missing_bom_pid = pd.DataFrame({'Missing BOM PID': missing_bom_pid})

    # 对于BOM missing 的采用3a4中已有的TAN
    df_3a4.loc[:, 'PN'] = np.where(df_3a4.TAN.notnull() & df_3a4.PN.isnull(),
                                   df_3a4.TAN,
                                   df_3a4.PN)
    """
    # correct the quantity by multiplying BOM Qty
    df_3a4.loc[:, 'C_UNSTAGED_QTY'] = df_3a4.C_UNSTAGED_QTY * (df_3a4.BOM_PN_QTY / df_3a4.ORDERED_QUANTITY)
    df_3a4.loc[:, 'ORDERED_QUANTITY'] = df_3a4.BOM_PN_QTY

    # add indicator for distinct PO filtering
    df_3a4.loc[:, 'distinct_po_filter'] = np.where(~df_3a4.duplicated('PO_NUMBER'),
                                                   'YES',
                                                   '')

    return df_3a4


def created_supply_dict_per_scr(df_scr):
    """
    create supply dict based on scr
    supply_dic_tan={'800-42373':[{'2/10':25},{'2/12':4},{'2/15':10},{'2/22':20},{'3/1':10},{'3/5':15}],
               '800-42925':[{'2/12':4},{'2/13':3},{'2/15':12},{'2/23':25},{'3/1':8},{'3/6':10}]}
    """
    supply_dic_tan = {}

    for tan in df_scr.index:
        date_qty_list = []
        for date in df_scr.columns:
            date_qty = {date: df_scr.loc[tan, date]}
            if not math.isnan(df_scr.loc[tan, date]):  # 判断数值是否为空
                if df_scr.loc[tan, date] > 0:  # 不取0值
                    date_qty_list.append(date_qty)
        supply_dic_tan[tan] = date_qty_list

    """ Below version spends much logner time to create the dict. use above instead with index simplified.
    for org_tan in df_scr.index:
        org=org_tan[0]
        tan=org_tan[1]
        date_qty_list=[]
        for date in df_scr.columns:
            date_qty={date:df_scr.loc[(org,tan),date]}
            if not math.isnan(df_scr.loc[(org,tan),date]): # 判断数值是否为空
                if df_scr.loc[(org,tan),date]>0: # 不取0值
                    date_qty_list.append(date_qty)
    """

    return supply_dic_tan


def created_oh_dict_per_df_oh(df_oh, pcba_site):
    """
    (Also used for transit eta close dict)create OH dict based on DF OH (excluding PCBA site and only consider OH>0 items)
    oh_dic_tan={(FOC,'800-42373'):25,(FJZ,'800-42925'):100}
    """
    df_oh = df_oh[(df_oh.OH > 0)]
    df_oh.reset_index(inplace=True)
    oh_dic_tan = {}
    for row in df_oh.itertuples(index=False):
        org = row.planningOrg
        tan = row.TAN
        oh = row.OH

        if org != pcba_site:
            oh_dic_tan[(org, tan)] = oh

    return oh_dic_tan


def create_transit_dict_per_df_transit(df_transit):
    """
    Create transit dict based on df_transit_eta_late.
    transit_dict_tan={(FOC,'800-42373'):(15,2020-10-20)}
    """
    transit_dic_tan = {}

    for org_tan in df_transit.index:
        date_qty_list = []
        for date in df_transit.columns:
            if not math.isnan(df_transit.loc[(org_tan[0], org_tan[1]), date]):  # 判断数值是否为空
                if df_transit.loc[(org_tan[0], org_tan[1]), date] > 0:  # 不取0值
                    date_qty = {date: df_transit.loc[(org_tan[0], org_tan[1]), date]}
                    date_qty_list.append(date_qty)
        if len(date_qty_list) > 0:
            transit_dic_tan[(org_tan[0], org_tan[1])] = date_qty_list

    return transit_dic_tan


def create_blg_dict_per_sorted_3a4_and_selected_tan(df_3a4, tan):
    """
    create backlog dict for selected tan list from the sorted 3a4 df (considered order prioity and rank)
    blg_ic_tan={'800-42373':{'FJZ':(5,'1234567-1','2020-10-20')}}
    """
    blg_dic_tan = {}
    dfx = df_3a4[(df_3a4.PACKOUT_QUANTITY != 'Packout Completed') & (df_3a4.ADDRESSABLE_FLAG != 'PO_CANCELLED')]
    for pn in tan:
        dfm = dfx[dfx.BOM_PN == pn]
        org_qty_po = []
        for org, qty, po, ossd in zip(dfm.ORGANIZATION_CODE, dfm.C_UNSTAGED_QTY, dfm.PO_NUMBER, dfm.ORIGINAL_FCD_NBD_DATE): # use ORIGINAL_FCD_NBD_DATE instead of ossd_ofset
            if qty > 0:
                org_qty_po.append({org: (qty, po, ossd.date())})

        blg_dic_tan[pn] = org_qty_po

    return blg_dic_tan


def allocate_supply_per_supply_and_blg_dic(supply_dic_tan, blg_dic_tan):
    """
    allocate supply based on supply dict and backlog dict
    supply dict is aranged in date order; backlog dict is aranged based on priority to fulfill

    examples:
        blg_dic_tan={'800-42373-01': [{'FJZ': (5, '110077267-1')},{'FJZ': (23, '110011089-4')},...]}
        supply_dic_tan={'800-42373-01':[{'2/10':25},{'2/12':4},{'2/15':10},{'2/22':20},{'3/1':10},{'3/5':15}],
                             '800-42925-01':[{'2/12':4},{'2/13':3},{'2/15':12},{'2/23':25},{'3/1':8},{'3/6':10}]}
    """
    supply_dic_tan_allocated = {}

    for tan in supply_dic_tan.keys():
        supply_list_tan = supply_dic_tan[tan]  # 每一个tan对应的supply list

        if tan in blg_dic_tan.keys():  #
            blg_list_tan = blg_dic_tan[tan]

            # 对supply list中每一个值进行分配给一个或多个订单
            for date_qty in supply_list_tan:
                # print(date_qty)
                supply_date = list(date_qty.keys())[0]
                supply_qty = list(date_qty.values())[0]
                allocation = []  # 每一个supply的分配结果

                allocated_po = []  # 已经分配给对应数量的po
                # 对每一个po进行数量分配
                for po in blg_list_tan:
                    # print(po)
                    po_qty = list(po.values())[0][0]
                    po_org = list(po.keys())[0]
                    po_number = list(po.values())[0][1]

                    # print(po_qty)
                    # print(supply_qty)
                    if po_qty < supply_qty:  # po数量小于supply数量：po被全额满足；supply数量被减掉；已分配的po被记录 （后面跳转到下一个po）
                        allocation.append((po_org, po_qty))
                        supply_qty = supply_qty - po_qty
                        allocated_po.append(po)
                    elif po_qty == supply_qty:  # po数量等于supply数量：po被全额满足；已分配的po被记录；跳出本次po循环(进到下一个supply循环)
                        allocation.append((po_org, po_qty))
                        allocated_po.append(po)
                        break
                    else:  # po数量大于supply数量：po被部分（=supply qty）满足；po数量被改小；跳出本次po循环(进到下一个supply循环)
                        allocation.append((po_org, supply_qty))
                        new_po_qty = po_qty - supply_qty
                        ind = blg_list_tan.index(po)
                        blg_list_tan[ind] = {po_org: (new_po_qty, po_number)}
                        break

                # print(allocated_po)
                # 把已经被分配的po从列表中删除
                for po in allocated_po:
                    # print(po)
                    blg_list_tan.remove(po)  # double check this one of removing PO whether correct or not
                    blg_dic_tan[tan] = blg_list_tan

                # 把supply列表中对应的supply改变成分配的结果
                ind = supply_list_tan.index(date_qty)
                supply_date = list(date_qty.keys())[0]
                supply_qty = list(date_qty.values())[0]
                supply_list_tan[ind] = {supply_date: (supply_qty, allocation)}

        # 生成新的allocated supply dict
        supply_dic_tan_allocated[tan] = supply_list_tan

    return supply_dic_tan_allocated


def aggregate_allocation_for_each_date(a, date_supply_agg):
    """
    # 根据日期及org汇合每个日期dict下的数量
    a={'2/12': (4, [('FCZ', 1), ('FJZ', 1), ('FJZ', 1), ('FJZ', 1)])}
    date_supply_agg={}
    此函数在aggregate_supply_dic_tan_allocated中引用
    """
    date = list(a.keys())[0]
    supply = list(a.values())[0][1]
    supply_total_qty = list(a.values())[0][0]

    orgs = []
    for org_supply in supply:
        if org_supply[0] not in orgs:
            orgs.append(org_supply[0])

    allocation_agg = []
    for org in orgs:
        qty = 0
        for org_supply in supply:
            if org_supply[0] == org:
                qty += org_supply[1]

        allocation_agg.append((org, qty))

    date_supply_agg = {}
    date_supply_agg[date] = (supply_total_qty, allocation_agg)

    return date_supply_agg


def aggregate_supply_dic_tan_allocated(supply_dic_tan_allocated):
    """
    针对每一个tan按照每一个日期将分配的数量按照org汇总(引用函数aggregate_allocation_for_each_date)
    """
    supply_dic_tan_allocated_agg = {}
    for tan, tan_supply in supply_dic_tan_allocated.items():
        tan_supply_list = []
        for date_supply in tan_supply:
            date_supply_agg = {}
            date_supply_agg = aggregate_allocation_for_each_date(date_supply, date_supply_agg)
            tan_supply_list.append(date_supply_agg)

        supply_dic_tan_allocated_agg[tan] = tan_supply_list

    return supply_dic_tan_allocated_agg


def fulfill_backlog_by_oh(oh_dic_tan, blg_dic_tan):
    """
    Fulfill the backlog per DF site based on the DF site OH; deduct the backlog qty accordingly.
    examples:
        blg_dic_tan={'800-42373': [{'FJZ': (5, '110077267-1','2020-4-1')},{'FJZ': (23, '110011089-4','2020-4-4')},...]}
        oh_dic_tan={('FJZ',800-42373'):25,('FCZ',800-42925'):10}
    return: blg_dic_tan
    """
    for org_tan, qty in oh_dic_tan.items():
        oh_org = org_tan[0]
        oh_tan = org_tan[1]
        oh_qty = qty

        if oh_tan in blg_dic_tan.keys():  # blg_dic_tan只包含scr中的tan，oh_tan可能不在其中，如不在，不予考虑
            blg_dic_tan_list = blg_dic_tan[oh_tan]  # 对应tan下的内容
            blg_dic_tan_list_copy = blg_dic_tan_list.copy()

            # 按顺序对每一个po进行数量分配
            for org_po in blg_dic_tan_list:
                po_org = list(org_po.keys())[0]
                po_qty = list(org_po.values())[0][0]
                po_number = list(org_po.values())[0][1]
                po_ossd = list(org_po.values())[0][2]

                if po_org == oh_org:
                    po_qty_new = po_qty - oh_qty
                    oh_qty_new = oh_qty - po_qty
                    if po_qty_new <= 0:  # po已被oh cover完，移除po
                        blg_dic_tan_list_copy.remove(org_po)
                        oh_qty = oh_qty_new
                    else:  # oh consumed
                        index = blg_dic_tan_list_copy.index(org_po)
                        blg_dic_tan_list_copy[index] = {po_org: (po_qty_new, po_number, po_ossd)}

                        break
            # 更新blg_dic_tan
            blg_dic_tan[oh_tan] = blg_dic_tan_list_copy

    return blg_dic_tan


def add_allocation_to_scr(df_scr, df_3a4, supply_dic_tan_allocated_agg, pcba_site):
    """
    Add up the allocation results to scr and create the final output file
    """
    pcba_site_temp = 'A-' + pcba_site
    df_scr.loc[:, 'ORG'] = pcba_site_temp
    df_scr.reset_index(inplace=True)
    df_scr.set_index(['TAN', 'ORG'], inplace=True)

    # Add in orgs based on 3a4
    df_3a4_p = df_3a4.pivot_table(index=['BOM_PN', 'ORGANIZATION_CODE'], values='PO_NUMBER', aggfunc=len)
    df_3a4_p.reset_index(inplace=True)

    for row in df_3a4_p.itertuples():
        tan = row.BOM_PN
        org = row.ORGANIZATION_CODE

        if tan in df_scr.index:
            df_scr.loc[(tan, org), 'count'] = row.PO_NUMBER
    df_scr.drop('count', axis=1, inplace=True)

    # add in allocated qty
    for tan in supply_dic_tan_allocated_agg.keys():
        for date_supply in supply_dic_tan_allocated_agg[tan]:
            date = list(date_supply.keys())[0]
            org_qty = list(date_supply.values())[0][1]
            for x in org_qty:
                df_scr.loc[(tan, x[0]), date] = x[1]

    df_scr.reset_index(inplace=True)
    df_scr.sort_values(by=['TAN', 'ORG'], ascending=True, inplace=True)
    df_scr.loc[:, 'ORG'] = df_scr.ORG.map(lambda x: pcba_site if 'A-' in x else x)
    df_scr.set_index(['TAN', 'ORG'], inplace=True)

    return df_scr


def extract_bu_from_scr(df_scr,tan_group):
    """
    Versionless the PN and extract the BU info from original scr before pivoting
    """
    regex_pn = re.compile(r'\d{2,3}-\d{4,7}')

    tan_bu = {}
    for row in df_scr.itertuples(index=False):
        tan = regex_pn.search(row.TAN).group()
        if tan in tan_group.keys():#替换成group
            tan=tan_group[tan]
        tan_bu[tan] = row.BU

    return tan_bu



def ss_ranking_overall_new(df_3a4, ss_priority, ranking_col, order_col='SO_SS', new_col='ss_overall_rank'):
    """
    根据priority_cat,OSSD,FCD, REVENUE_NON_REVENUE,C_UNSTAGED_QTY,按照ranking_col的顺序对SS进行排序。最后放MFG_HOLD订单.
    :param df_3a4:
    :param ss_priority: manual priority set for backlog through smartsheet
    :param ranking_col:e.g. ['priority_rank', 'ORIGINAL_FCD_NBD_DATE', 'CURRENT_FCD_NBD_DATE','rev_non_rev_rank',
                        'C_UNSTAGED_QTY', 'SO_SS','PO_NUMBER']
    :param order_col:'SO_SS'
    :param new_col:'ss_overall_rank'
    :return: df_3a4
    """

    # Below create a rev_rank for reference -  currently not used in overall ranking
    ### change non-rev orders unstaged $ to 0
    df_3a4.loc[:, 'C_UNSTAGED_DOLLARS'] = np.where(df_3a4.REVENUE_NON_REVENUE == 'NO',
                                                   0,
                                                   df_3a4.C_UNSTAGED_DOLLARS)

    #### 生成ss_unstg_rev - 在这里不参与排序
    # 计算ss_unstg_rev
    ss_unstg_rev = {}
    df_rev = df_3a4.pivot_table(index='SO_SS', values='C_UNSTAGED_DOLLARS', aggfunc=sum)
    for ss, rev in zip(df_rev.index, df_rev.values):
        ss_unstg_rev[ss] = rev[0]
    df_3a4.loc[:, 'ss_unstg_rev'] = df_3a4.SO_SS.map(lambda x: ss_unstg_rev[x])

    """
    # 计算po_rev_unit - non revenue change to 0
    df_3a4.loc[:, 'po_rev_unit'] = np.where(df_3a4.REVENUE_NON_REVENUE == 'YES',
                                            df_3a4.SOL_REVENUE / df_3a4.ORDERED_QUANTITY,
                                            0)

    # 计算ss_rev_unit: 通过po_rev_unit汇总
    ss_rev_unit = {}
    dfx_rev = df_3a4.pivot_table(index='SO_SS', values='po_rev_unit', aggfunc=sum)
    for ss, rev in zip(dfx_rev.index, dfx_rev.values):
        ss_rev_unit[ss] = rev[0]
    df_3a4.loc[:, 'ss_rev_unit'] = df_3a4.SO_SS.map(lambda x: int(ss_rev_unit[x]))
    """

    # create rank#
    rank = {}
    order_list = df_3a4.sort_values(by='ss_unstg_rev', ascending=False).SO_SS.unique()
    for order, rk in zip(order_list, range(1, len(order_list) + 1)):
        rank[order] = rk
    df_3a4.loc[:, 'ss_rev_rank'] = df_3a4.SO_SS.map(lambda x: rank[x])

    # below creates overall ranking col
    ### Step1: 重新定义priority order及排序
    df_3a4.loc[:, 'priority_cat'] = np.where(df_3a4.SECONDARY_PRIORITY.isin(['PR1', 'PR2', 'PR3']),
                                             df_3a4.SECONDARY_PRIORITY,
                                             np.where(df_3a4.FINAL_ACTION_SUMMARY == 'TOP 100',
                                                      'TOP 100',
                                                      np.where(
                                                          df_3a4.FINAL_ACTION_SUMMARY == 'LEVEL 4 ESCALATION PRESENT',
                                                          'L4',
                                                          np.where(df_3a4.BUP_RANK.notnull(),
                                                                   'BUP',
                                                                   None)
                                                      )
                                                      )
                                             )
    #### Update below DO/DX orders to PR1 due to current PR1/2/3 not updated when order change to DPAS from others
    df_3a4.loc[:, 'priority_cat'] = np.where(
        (df_3a4.DPAS_RATING.isin(['DO', 'DX', 'TAA-DO', 'TAA-DX'])) & (df_3a4.priority_cat.isnull()),
        'PR1',
        df_3a4.priority_cat)
    #### Give them a rank
    df_3a4.loc[:, 'priority_rank'] = np.where(df_3a4.priority_cat == 'PR1',
                                              1,
                                              np.where(df_3a4.priority_cat == 'PR2',
                                                       2,
                                                       np.where(df_3a4.priority_cat == 'PR3',
                                                                3,
                                                                np.where(df_3a4.priority_cat == 'TOP 100',
                                                                         4,
                                                                         np.where(df_3a4.priority_cat == 'L4',
                                                                                  5,
                                                                                  np.where(df_3a4.priority_cat == 'BUP',
                                                                                           6,
                                                                                           None)
                                                                                  )
                                                                         )
                                                                )
                                                       )
                                              )

    ##### Step2: Give revenue/non-revenue a rank
    df_3a4.loc[:, 'rev_non_rev_rank'] = np.where(df_3a4.REVENUE_NON_REVENUE == 'YES', 0, 1)

    #### Step3: Integrate ranking from smartsheet
    df_3a4.loc[:, 'priority_rank'] = np.where(df_3a4.SO_SS.isin(ss_priority.keys()),
                                              df_3a4.SO_SS.map(lambda x: ss_priority.get(x)),
                                              df_3a4.priority_rank)

    #df_3a4.loc[:,'priority_rank']=df_3a4.SO_SS.map(lambda x: ss_priority.get(x))
    #df_3a4.loc[:, 'priority_rank_temp'] = df_3a4.SO_SS.map(lambda x: ss_priority.get(x))

    ##### Step4: sort the SS per ranking columns and Put MFG hold orders at the back
    df_3a4.sort_values(by=ranking_col, ascending=True, inplace=True)
    # Put MFG hold orders at the back - the 3a4 here has no option so can also use mfg_hold directly alternatively
    df_hold = df_3a4[df_3a4.ADDRESSABLE_FLAG == 'MFG_HOLD'].copy()
    df_3a4 = df_3a4[df_3a4.ADDRESSABLE_FLAG != 'MFG_HOLD'].copy()
    df_3a4 = pd.concat([df_3a4, df_hold], sort=False)

    ##### Step5: create rank# and put in 3a4
    rank = {}
    order_list = df_3a4[order_col].unique()
    for order, rk in zip(order_list, range(1, len(order_list) + 1)):
        rank[order] = rk
    df_3a4.loc[:, new_col] = df_3a4[order_col].map(lambda x: rank[x])

    return df_3a4

def write_data_to_excel(output_file,data_to_write):
    '''
    Write the df into excel files as different sheets
    :param fname: fname of the output excel
    :param data_to_write: a dict that contains {sheet_name:df}
    :return: None
    '''

    # engine='xlsxwriter' is used to avoid illegal character which lead to failure of saving the file
    writer = pd.ExcelWriter(output_file, engine='xlsxwriter')

    for sheet_name, df in data_to_write.items():
        df.to_excel(writer, sheet_name=sheet_name)

    writer.save()

def write_excel_output_file(bu_list,df_scr,df_3a4,df_transit):
    # save the scr output file and 3a4 to excel
    #dt = (pd.Timestamp.now() + pd.Timedelta(hours=8)).strftime('%m-%d %Hh%Mm')  # convert from server time to local
    dt = pd.Timestamp.now().strftime('%m-%d %Hh%Mm')

    if bu_list != ['']:
        bu = ' '.join(bu_list)
        output_filename = pcba_site + ' SCR allocation (' + bu + ') ' + dt + '.xlsx'
    else:
        output_filename = pcba_site + ' SCR allocation (all BU) ' + dt + '.xlsx'
    output_path = os.path.join(base_dir_output, output_filename)

    df_3a4 = df_3a4[df_3a4.BOM_PN.notnull()][output_col_3a4].copy()
    df_3a4.set_index(['ORGANIZATION_CODE'], inplace=True)
    df_transit.set_index(['planningOrg', 'TAN', 'In-transit'], inplace=True)
    df_transit.reset_index(inplace=True)
    df_transit.set_index(['planningOrg'], inplace=True)
    df_transit.rename(columns={'In-transit':'Total'},inplace=True)
    data_to_write = {'pcba_allocation': df_scr,
                     '3a4_backlog': df_3a4,
                     'in-transit': df_transit}

    write_data_to_excel(output_path, data_to_write)

    return output_filename

def fulfill_backlog_by_transit_eta_late(transit_dic_tan, blg_dic_tan):
    """
    Fulfill the backlog per DF site based on the DF site transit that is ETA far out; deduct the backlog qty accordingly.
    examples:
        blg_dic_tan={'800-42373': [{'FJZ': (5, '110077267-1','2020-4-1')},{'FJZ': (23, '110011089-4','2020-4-4')},...]}
        transit_dic_tan={('FJZ',800-42373'):[{'2020-10-27':25},{'2020-10-29':10}]}
    return: blg_dic_tan
    """

    for org_tan, date_qty_list in transit_dic_tan.items():
        date_qty_list_copy = date_qty_list.copy()
        transit_org = org_tan[0]
        transit_tan = org_tan[1]
        for date_qty in date_qty_list:
            transit_eta = list(date_qty.keys())[0]
            transit_qty = list(date_qty.values())[0]
            # backward offset the eta so to cover more ossd earlier than ETA
            eta_backward_offset = transit_eta - pd.Timedelta(days=eta_backward_offset_days)

            if transit_tan in blg_dic_tan.keys():  # blg_dic_tan只包含scr中的tan，oh_tan可能不在其中，如不在，不予考虑
                blg_dic_tan_list = blg_dic_tan[transit_tan]  # 对应tan下的内容
                blg_dic_tan_list_copy = blg_dic_tan_list.copy()
                # 按顺序对每一个po进行数量分配
                for org_po in blg_dic_tan_list:
                    po_org = list(org_po.keys())[0]
                    po_qty = list(org_po.values())[0][0]
                    po_number = list(org_po.values())[0][1]
                    po_ossd = list(org_po.values())[0][2]

                    if po_org == transit_org:
                        if eta_backward_offset <= po_ossd or pd.isnull(po_ossd):  # 考虑ossd,forward consumption
                            po_qty_new = po_qty - transit_qty

                            if po_qty_new < 0:  # po已被transit cover完，移除po;更新transit qty
                                blg_dic_tan_list_copy.remove(org_po)
                                index = date_qty_list_copy.index({transit_eta: transit_qty})
                                transit_qty = transit_qty - po_qty
                                date_qty_list_copy[index] = {transit_eta: transit_qty}
                            elif po_qty_new > 0:  # transit consumed by PO, 更新PO qty;移除transit
                                index = blg_dic_tan_list_copy.index(org_po)
                                blg_dic_tan_list_copy[index] = {po_org: (po_qty_new, po_number, po_ossd)}
                                date_qty_list_copy.remove({transit_eta: transit_qty})
                                break
                            else:  # po = transit, both are consumed
                                blg_dic_tan_list_copy.remove(org_po)
                                date_qty_list_copy.remove({transit_eta: transit_qty})
                                break

                # 更新blg_dic_tan
                blg_dic_tan[transit_tan] = blg_dic_tan_list_copy

        # 更新transit_dic_tan
        transit_dic_tan[org_tan] = date_qty_list_copy

    return blg_dic_tan, transit_dic_tan


def read_data(f_3a4,f_supply,sheet_scr,sheet_oh,sheet_transit):
    """
    Read source data from excel files
    :param f_3a4:
    :param f_supply:
    :param sheet_scr:
    :param sheet_oh:
    :param sheet_transit:
    :return:
    """
    # read 3a4
    df_3a4 = pd.read_csv(f_3a4, encoding='ISO-8859-1', parse_dates=['CURRENT_FCD_NBD_DATE', 'ORIGINAL_FCD_NBD_DATE','TARGET_SSD'],
                         low_memory=False)

    # read scr
    df_scr = pd.read_excel(f_supply, sheet_name=sheet_scr)

    # read oh this includes PCBA SM, will be removed when creating DF OH dict
    df_oh = pd.read_excel(f_supply, sheet_name=sheet_oh)

    # read in-transit
    df_transit = pd.read_excel(f_supply, sheet_name=sheet_transit)

    return df_3a4, df_oh, df_transit, df_scr

def limit_bu_from_3a4_and_scr(df_3a4,df_scr,bu_list):
    """
    Limit BU based on user input for allocation
    """
    if bu_list!=['']:
        df_3a4=df_3a4[df_3a4.BUSINESS_UNIT.isin(bu_list)]
        df_scr=df_scr[df_scr.BU.isin(bu_list)]

    return df_3a4, df_scr


def check_input_file_format(file_path_3a4,file_path_supply,col_3a4_must_have,col_transit_must_have,col_oh_must_have,col_scr_must_have,sheet_transit,sheet_oh,sheet_scr):
    """
    Check if the input files contain the right columns
    """
    sheet_name_msg,msg_3a4, msg_3a4_option, msg_transit, msg_oh, msg_scr = '', '', '', '','',''
    df_3a4=pd.read_csv(file_path_3a4,nrows=2,encoding='iso-8859-1')
    try:
        df_transit=pd.read_excel(file_path_supply,sheet_name=sheet_transit,nrows=2)
        df_oh=pd.read_excel(file_path_supply,sheet_name=sheet_oh,nrows=2)
        df_scr=pd.read_excel(file_path_supply,sheet_name=sheet_scr,nrows=2)

        # 检查文件是否包含需要的列：
        if not np.all(np.in1d(col_3a4_must_have, df_3a4.columns)):
            msg_3a4='3A4 file format error! Following required columns not found in 3a4 data: {}'.format(
                str(np.setdiff1d(col_3a4_must_have, df_3a4.columns)))
        if 'OPTION_NUMBER' in df_3a4.columns:
            msg_3a4_option='3A4 file format error! Pls download 3A4 without option PIDs!'

        if not np.all(np.in1d(col_transit_must_have, df_transit.columns)):
            msg_transit = 'In-transit data format error! Following required columns not found in transit data: {}'.format(
                str(np.setdiff1d(col_transit_must_have, df_transit.columns)))

        if not np.all(np.in1d(col_oh_must_have, df_oh.columns)):
            msg_oh = 'OH data format error! Following required columns not found in OH data: {}'.format(
                str(np.setdiff1d(col_oh_must_have, df_oh.columns)))

        if not np.all(np.in1d(col_scr_must_have, df_scr.columns)):
            msg_scr = 'SCR data format error! Following required columns not found in SCR data: {}'.format(
                str(np.setdiff1d(col_scr_must_have, df_scr.columns)))
    except:
        print('sheet name error!')
        sheet_name_msg = 'Supply file format error! Ensure the correct sheet names are: {}, {}, {}'.format(sheet_transit,sheet_oh,sheet_scr)

    return sheet_name_msg, msg_3a4, msg_3a4_option, msg_transit,msg_oh,msg_scr

def redefine_addressable_flag_main_pip_version(df_3a4):
    '''
    Updated on Oct 27, 2020 to leveraging existing addressable definition of Y, and redefine the NO to MFG_HOLD,
    UNSCHEDULED,PACKED,PO_CANCELLED,NON_REVENUE
    :param df_3a4:
    :return:
    '''

    # Convert YES to ADDRESSABLE
    df_3a4.loc[:, 'ADDRESSABLE_FLAG'] = np.where(df_3a4.ADDRESSABLE_FLAG=='YES',
                                                 'ADDRESSABLE',
                                                 df_3a4.ADDRESSABLE_FLAG)


    # 如果没有LT_TARGET_FCD/TARGET_SSD/CURRENT_FCD_NBD_DATE,则作如下处理 - 可能是没有schedule或缺失Target LT date or Target SSD
    df_3a4.loc[:, 'ADDRESSABLE_FLAG'] = np.where(df_3a4.CURRENT_FCD_NBD_DATE.isnull(),
                                                'UNSCHEDULED',
                                                df_3a4.ADDRESSABLE_FLAG)

    # Non_revenue orders
    df_3a4.loc[:, 'ADDRESSABLE_FLAG'] = np.where(df_3a4.REVENUE_NON_REVENUE=='NO',
                                                  'NON_REVENUE',
                                                df_3a4.ADDRESSABLE_FLAG)


    #  mfg-hold
    df_3a4.loc[:, 'ADDRESSABLE_FLAG'] = np.where(df_3a4.MFG_HOLD=='Y',
                                                 'MFG_HOLD',
                                                 df_3a4.ADDRESSABLE_FLAG)

    # redefine cancellation order to PO_CANCELLED - put this after MFG_HOLD so it won't get replaced by MFG_HOLD
    df_3a4.loc[:, 'ADDRESSABLE_FLAG'] = np.where(
        (df_3a4.ORDER_HOLDS.str.contains('Cancellation', case=False)) & (df_3a4.ORDER_HOLDS.notnull()),
        'PO_CANCELLED',
        df_3a4.ADDRESSABLE_FLAG)

    #df_3a4[(df_3a4.OPTION_NUMBER==0)&(df_3a4.ORGANIZATION_CODE=='FOC')][['PO_NUMBER','ADDRESSABLE_FLAG_redefined','ADDRESSABLE_FLAG','MFG_HOLD','ORDER_HOLDS','TARGET_SSD','LT_TARGET_FCD','CURRENT_FCD_NBD_DATE']].to_excel('3a4 processed-2.xlsx',index=False)
    # OTHER non-addressable
    df_3a4.loc[:, 'ADDRESSABLE_FLAG']=np.where(df_3a4.ADDRESSABLE_FLAG=='NO',
                                               'NOT_ADDRESSABLE',
                                               df_3a4.ADDRESSABLE_FLAG)

    return df_3a4



def calculate_x_days_target_ssd_backlog(df_3a4,days=7):
    """
    Calculate the backlog per target SSD by 7/14/21 days as needed.
    Return a dict {(tan,org,bu}:qty}
    """
    days_cutoff=pd.Timestamp.today()+pd.Timedelta(days=days)

    dfx=df_3a4[(df_3a4.TARGET_SSD<=days_cutoff)&(df_3a4.PACKOUT_QUANTITY!='Packout Completed')]
    dfx_p=dfx.pivot_table(index=['BOM_PN','ORGANIZATION_CODE'],values='C_UNSTAGED_QTY',aggfunc=sum)
    dfx_p.reset_index(inplace=True)
    backlog_target_ssd={}
    for row in dfx_p.itertuples():
        key=(row.BOM_PN,row.ORGANIZATION_CODE)
        value=row.C_UNSTAGED_QTY
        backlog_target_ssd[key]=value

    return backlog_target_ssd


def calculate_x_weeks_allocation(df_scr,pcba_site, wk='wk1'):
    """
    Summaryize the supply allocation by week. wk1 includes current wk and pastdue.
    """
    today = pd.Timestamp.now().date()
    today_name = pd.Timestamp.today().day_name()

    if today_name == 'Monday':
        sun_cutoff = 6 + (int(wk[2:])-1) * 7
    elif today_name == 'Tuesday':
        sun_cutoff = 5 + (int(wk[2:])-1) * 7
    elif today_name == 'Wednesday':
        sun_cutoff = 4 + (int(wk[2:])-1) * 7
    elif today_name == 'Thursday':
        sun_cutoff = 3 + (int(wk[2:])-1) * 7
    elif today_name == 'Friday':
        sun_cutoff = 2 + (int(wk[2:])-1) * 7
    elif today_name == 'Saturday':
        sun_cutoff = 1 + (int(wk[2:])-1) * 7
    elif today_name == 'Sunday':
        sun_cutoff = 0 + (int(wk[2:])-1) * 7
    # Sunday of the cutoff week
    supply_cut_off = today + pd.Timedelta(sun_cutoff, 'd')

    # find out the col of cutoff :-10 with buffer to ensure too exclude all the newly added col which are not dates
    date_col=df_scr.iloc[:,:-10].columns.astype('datetime64[ns]').tolist()
    for date in date_col:
        if date>supply_cut_off:
            ind=date_col.index(date)-1
            break
    if ind==None:
        print('Error in program to find the right cut off date in scr')
    dfx=df_scr.iloc[:,:ind].copy()
    dfx.loc[:,'total']=dfx.sum(axis=1)

    tan_allocation_wk={}
    for row in dfx.itertuples(index=True):
        if row.Index[1]!=pcba_site:
            key=(row.Index[0],row.Index[1])
            value=row.total
            tan_allocation_wk[key]=value

    return tan_allocation_wk
    
def process_final_allocated_output(df_scr, tan_bu, df_3a4, df_oh, df_transit, pcba_site):
    """
    Add back the BU, backlog,oh, intransit info into the final SCR with allocation result; and add the related columns based on calculations.
    """
    df_scr.reset_index(inplace=True)

    # add BU info
    df_scr.loc[:, 'BU'] = df_scr.TAN.map(lambda x: tan_bu[x])



    # add backlog qty
    df_3a4_p = df_3a4.pivot_table(index=['ORGANIZATION_CODE', 'BOM_PN'], values='C_UNSTAGED_QTY', aggfunc=sum)
    df_3a4_p.columns = ['Unpacked_blg']
    df_3a4_p.reset_index(inplace=True)
    df_scr = pd.merge(df_scr, df_3a4_p, left_on=['ORG', 'TAN'], right_on=['ORGANIZATION_CODE', 'BOM_PN'], how='left')

    # add df OH
    df_oh.columns = ['OH']
    df_oh.reset_index(inplace=True)
    df_oh = df_oh[df_oh.planningOrg != pcba_site]
    df_scr = pd.merge(df_scr, df_oh, left_on=['ORG', 'TAN'], right_on=['planningOrg', 'TAN'], how='left')
    # drop the unneeded columns introduced by merge
    df_scr.drop(['ORGANIZATION_CODE', 'BOM_PN', 'planningOrg'], axis=1, inplace=True)
    # df_scr.rename(columns={'TAN_x':'TAN'},inplace=True)

    # add df transit
    df_transit.loc[:, 'In-transit'] = df_transit.sum(axis=1)
    df_transit.reset_index(inplace=True)
    df_scr = pd.merge(df_scr, df_transit[['planningOrg', 'TAN', 'In-transit']], left_on=['ORG', 'TAN'],
                      right_on=['planningOrg', 'TAN'], how='left')
    # drop the unneeded columns introduced by merge
    df_scr.drop(['planningOrg'], axis=1, inplace=True)

   # ADD temp col oh+transit to calculate gap before
    df_scr.loc[:, 'oh+transit'] = df_scr.OH.fillna(0) + df_scr['In-transit'].fillna(0)
    df_scr['oh+transit'].fillna(0, inplace=True)
    df_scr.loc[:, 'Gap_before'] = np.where(df_scr.ORG != pcba_site,
                                           df_scr['oh+transit'] - df_scr.Unpacked_blg,
                                           None)
    df_scr.drop('oh+transit', axis=1, inplace=True)

    # calculate and add in total allocation and recovery date
    df_scr.loc[:, 'Allocation'] = np.where(df_scr.ORG != pcba_site,
                                           df_scr.iloc[:, 2:-5].sum(axis=1),  # [3:-5] refer to the right data columns
                                           None)

    df_scr.loc[:, 'Gap_after'] = np.where(df_scr.ORG != pcba_site,
                                          df_scr.Gap_before + df_scr.Allocation,
                                          None)

    df_scr.loc[:, 'Blg_recovery'] = np.where((df_scr.ORG != pcba_site),
                                         np.where(df_scr.Gap_before >= 0,
                                                  'No gap',
                                                  np.where(df_scr.Gap_after < 0,
                                                           'No recovery',
                                                           'TBD')),
                                         None)


    # add in 7/14/21 days target_ssd backlog and wk0/wk1/wk2 allocation qty
    df_scr.set_index(['TAN','ORG'],inplace=True)
    backlog_target_ssd_7=calculate_x_days_target_ssd_backlog(df_3a4, days=7)
    backlog_target_ssd_14=calculate_x_days_target_ssd_backlog(df_3a4, days=14)
    backlog_target_ssd_21=calculate_x_days_target_ssd_backlog(df_3a4, days=21)
    tan_allocation_wk1 = calculate_x_weeks_allocation(df_scr, pcba_site, wk='wk1')
    tan_allocation_wk2 = calculate_x_weeks_allocation(df_scr, pcba_site, wk='wk2')
    tan_allocation_wk3 = calculate_x_weeks_allocation(df_scr, pcba_site, wk='wk3')

    df_scr.loc[:,'Target_SSD_7']=df_scr.index.map(lambda x: backlog_target_ssd_7[x] if x in backlog_target_ssd_7.keys() else None )
    df_scr.loc[:, 'Alloc_by_wk1'] = df_scr.index.map(lambda x: tan_allocation_wk1[x] if x in tan_allocation_wk1.keys() else None)
    df_scr.loc[:,'Delta_1']=df_scr.OH.fillna(0) + df_scr['In-transit'].fillna(0) + df_scr.Alloc_by_wk1 - df_scr.Target_SSD_7
    df_scr.loc[:,'Target_SSD_14']=df_scr.index.map(lambda x: backlog_target_ssd_14[x] if x in backlog_target_ssd_14.keys() else None )
    df_scr.loc[:, 'Alloc_by_wk2'] = df_scr.index.map(lambda x: tan_allocation_wk2[x] if x in tan_allocation_wk2.keys() else None)
    df_scr.loc[:, 'Delta_2'] = df_scr.OH.fillna(0) + df_scr['In-transit'].fillna(0) + df_scr.Alloc_by_wk2 - df_scr.Target_SSD_14
    df_scr.loc[:,'Target_SSD_21']=df_scr.index.map(lambda x: backlog_target_ssd_21[x] if x in backlog_target_ssd_21.keys() else None )
    df_scr.loc[:, 'Alloc_by_wk3'] = df_scr.index.map(lambda x: tan_allocation_wk3[x] if x in tan_allocation_wk3.keys() else None)
    df_scr.loc[:, 'Delta_3'] = df_scr.OH.fillna(0) + df_scr['In-transit'].fillna(0) + df_scr.Alloc_by_wk3 - df_scr.Target_SSD_21

    # update with the correct Blg_recovery date for TBD
    df_scr.reset_index(inplace=True)
    df_scr.set_index(
        ['TAN', 'ORG', 'BU', 'Unpacked_blg', 'OH', 'In-transit', 'Gap_before', 'Allocation', 'Gap_after', 'Blg_recovery'],
        inplace=True)
    df_scr.reset_index(inplace=True)
    dfx = df_scr[(df_scr.Blg_recovery == 'TBD') & (df_scr.ORG != pcba_site)]
    dfx=dfx.iloc[:,:-9].copy() # -9 to excluded the last added columns
    dfx.set_index(['TAN', 'ORG'], inplace=True)
    df_scr.set_index(['TAN', 'ORG'], inplace=True)

    for ind in dfx.index:
        dfy = dfx.loc[ind, :]
        dfy = dfy[dfy.notnull()]

        last_allocation_date = dfy.index[-1]
        df_scr.loc[ind, 'Blg_recovery'] = last_allocation_date

    # set final index
    df_scr.reset_index(inplace=True)
    df_scr.set_index(
        ['TAN', 'ORG', 'BU', 'Unpacked_blg', 'OH', 'In-transit', 'Gap_before', 'Allocation', 'Gap_after', 'Blg_recovery'],
        inplace=True)

    return df_scr



def pcba_allocation_main_program(df_3a4, df_oh, df_transit, df_scr,pcba_site,bu_list,ranking_col):
    """
    Main program to process the data and PCBA allocation.
    :param df_3a4:
    :param df_oh:
    :param df_transit:
    :param df_scr:
    :param pcba_site:
    :param ranking_col:
    :param output_filename:
    :return: None
    """
    # Read TAN group mapping from smartsheet
    tan_group = read_tan_group_mapping_from_smartsheet()

    # extract BU info for TAN from SCR for final report processing use
    tan_bu = extract_bu_from_scr(df_scr,tan_group)

    # Pivot df_scr 并处理日期格式; change to versionless
    df_scr = df_scr.pivot_table(index=['planningOrg', 'TAN'], columns='SCRDate', values='SCRQuantity', aggfunc=sum)
    df_scr.columns = df_scr.columns.map(lambda x: x.date())
    df_scr=change_pn_to_versionless(df_scr,pn_col='TAN')

    # change TAN to group number based on group mapping
    df_scr=change_pn_to_group_number(df_scr,tan_group,pn_col='TAN')

    # Add up supply based on pivoted df
    df_scr=add_up_supply_by_pn(df_scr,org_col='planningOrg',pn_col='TAN')

    # simplify the index will make it much faster to get the dict - drop org and can add back later since know it's pcba_site
    df_scr.reset_index(inplace=True)
    df_scr.drop('planningOrg', axis=1, inplace=True)
    df_scr.set_index('TAN', inplace=True)
    supply_dic_tan = created_supply_dict_per_scr(df_scr)

    # Offset 3A4 OSSD and FCD by transit time
    df_3a4.loc[:, 'fcd_offset'] = df_3a4[['ORGANIZATION_CODE', 'CURRENT_FCD_NBD_DATE']].apply(
        lambda x: update_date_with_transit_pad(x.ORGANIZATION_CODE, x.CURRENT_FCD_NBD_DATE, transit_time, pcba_site),
        axis=1)
    df_3a4.loc[:, 'ossd_offset'] = df_3a4.apply(
        lambda x: update_date_with_transit_pad(x.ORGANIZATION_CODE, x.ORIGINAL_FCD_NBD_DATE, transit_time, pcba_site),
        axis=1)

    # redefine addressable flag
    df_3a4=redefine_addressable_flag_main_pip_version(df_3a4)
    # remove cancelled/packed orders - remove the record from 3a4 (in creating blg dict it's double removed - together with packed orders)
    df_3a4 = df_3a4[(df_3a4.ADDRESSABLE_FLAG != 'PO_CANCELLED')&(df_3a4.PACKOUT_QUANTITY!='Packout Completed')].copy()

    # read smartsheet for exceptional priority ss
    ss_priority=read_backlog_priority_from_smartsheet()
    # Rank the orders
    df_3a4 = ss_ranking_overall_new(df_3a4, ss_priority, ranking_col, order_col='SO_SS', new_col='ss_overall_rank')

    # (do below after ranking) Process 3a4 BOM base on FLB_TAN col. BOM_PN refers to tan_group if exist, or SCR Tan if not.
    df_bom = generate_df_order_bom_from_flb_tan_col(df_3a4, supply_dic_tan,tan_group)
    df_3a4 = update_order_bom_to_3a4(df_3a4, df_bom)

    # create backlog dict for Tan exists in SCR
    blg_dic_tan = create_blg_dict_per_sorted_3a4_and_selected_tan(df_3a4, supply_dic_tan.keys())

    # pivot df_oh and versionless the TAN
    df_oh = df_oh.pivot_table(index=['planningOrg', 'TAN'], values='OH', aggfunc=sum)
    df_oh=change_pn_to_versionless(df_oh,pn_col='TAN')

    # change TAN to group number based on group mapping
    df_oh=change_pn_to_group_number(df_oh,tan_group,pn_col='TAN')

    # add up supply by versionless TAN
    df_oh=add_up_supply_by_pn(df_oh,org_col='planningOrg',pn_col='TAN')

    # 生成OH dict；
    oh_dic_tan = created_oh_dict_per_df_oh(df_oh, pcba_site)

    # Oh to fulfill backlog per site. update blg_dic_tan accordingly
    blg_dic_tan = fulfill_backlog_by_oh(oh_dic_tan, blg_dic_tan)

    # read exceptional intransit from smartsheet and concat with df_transit
    df_smart_intransit = read_exceptional_intransit_from_smartsheet()
    df_transit=pd.concat([df_transit,df_smart_intransit],sort=False)

    # pivot df_transit and change to versionless
    df_transit = df_transit.pivot_table(index=['planningOrg', 'TAN'], columns='ETA_date', values='In-transit_quantity',
                                        aggfunc=sum)
    df_transit.columns = df_transit.columns.map(lambda x: x.date())
    df_transit=change_pn_to_versionless(df_transit,pn_col='TAN')

    # change TAN to group number based on group mapping
    df_transit=change_pn_to_group_number(df_transit,tan_group,pn_col='TAN')

    # add up supply by versionless TAN
    df_transit=add_up_supply_by_pn(df_transit,org_col='planningOrg',pn_col='TAN')

    # split df_transit by threshhold of 15 days
    close_eta_cutoff = pd.Timestamp.today().date() + pd.Timedelta(days=close_eta_cutoff_criteria)
    col = df_transit.columns
    df_transit_eta_early = df_transit.loc[:, col <= close_eta_cutoff].copy()
    df_transit_eta_early.loc[:, 'OH'] = df_transit_eta_early.sum(axis=1)  # sum up as OH so can use the oh dict function
    df_transit_eta_late = df_transit.loc[:, col > close_eta_cutoff].copy()

    # 生成transit dict - for ETA close data use the OH dict function instead
    transit_dic_tan_eta_early = created_oh_dict_per_df_oh(df_transit_eta_early, pcba_site)
    transit_dic_tan_eta_late = create_transit_dict_per_df_transit(df_transit_eta_late)

    # 按照org将in-transit分配给自己的订单（forward consumption considering ETA per OSSD - ETA consider backward offset）
    # 并更新blg_dic_tan
    blg_dic_tan = fulfill_backlog_by_oh(transit_dic_tan_eta_early, blg_dic_tan)
    blg_dic_tan, transit_dic_tan_eta_late = fulfill_backlog_by_transit_eta_late(transit_dic_tan_eta_late, blg_dic_tan)

    # Allocate SCR and 生成allocated supply dict
    supply_dic_tan_allocated = allocate_supply_per_supply_and_blg_dic(supply_dic_tan, blg_dic_tan)

    # TODO: (enhancement) if transit_dic_tan not consumed, come back to judge if the allocation is needed or not(per ETA) - if not, take it back and allocate to others
    # currently using 7days (or 14days?) backward fulfillment for late ETA which partially covered this already.

    # 生成聚合的allocated supply dict
    supply_dic_tan_allocated_agg = aggregate_supply_dic_tan_allocated(supply_dic_tan_allocated)

    # 在df_scr中加入allocation结果
    df_scr = add_allocation_to_scr(df_scr, df_3a4, supply_dic_tan_allocated_agg, pcba_site)

    # 把以下信息加回scr: BU, backlog, OH, intransit; 并做相应的计算处理
    df_scr = process_final_allocated_output(df_scr, tan_bu, df_3a4, df_oh, df_transit, pcba_site)

    # 存储文件
    output_filename = write_excel_output_file(bu_list, df_scr, df_3a4, df_transit)

    return output_filename

