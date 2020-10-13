# created by Ken wang, Oct, 2020

import pandas as pd
import re
import numpy as np
import math
from settings import *


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


def update_date_with_transit_pad(x, y, transit_time, pcba_site):
    """
    offset transit time to a given date column
    """
    if x in transit_time[pcba_site].keys():
        return y - pd.Timedelta(days=transit_time[pcba_site][x])
    else:
        return y - pd.Timedelta(days=transit_time[pcba_site]['other'])


def generate_df_order_bom_from_flb_tan_col(df_3a4, pcba):
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

                if pn in pcba:
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
    for pn in tan:
        dfm = df_3a4[df_3a4.BOM_PN == pn]
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


def extract_bu_from_scr(df_scr):
    """
    Versionless the PN and extract the BU info from original scr before pivoting
    """
    regex_pn = re.compile(r'\d{2,3}-\d{4,7}')

    tan_bu = {}
    for row in df_scr.itertuples(index=False):
        tan = regex_pn.search(row.TAN).group()
        tan_bu[tan] = row.BU

    return tan_bu


def process_final_allocated_output(df_scr, tan_bu, df_3a4, df_oh, df_transit, pcba_site):
    """
    Add back the BU, backlog,oh, intransit info into the final SCR with allocation result; and add the related columns based on calculations.
    """
    df_scr.reset_index(inplace=True)

    # add BU info
    df_scr.loc[:, 'BU'] = df_scr.TAN.map(lambda x: tan_bu[x])

    # add backlog qty
    df_3a4_p = df_3a4.pivot_table(index=['ORGANIZATION_CODE', 'BOM_PN'], values='C_UNSTAGED_QTY', aggfunc=sum)
    df_3a4_p.columns = ['Backlog']
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

    # ADD THE gap col and recovery date
    df_scr.loc[:, 'oh+transit'] = df_scr.OH.fillna(0) + df_scr['In-transit'].fillna(0)
    df_scr['oh+transit'].fillna(0, inplace=True)
    df_scr.loc[:, 'Gap_before'] = np.where(df_scr.ORG != pcba_site,
                                           df_scr['oh+transit'] - df_scr.Backlog,
                                           None)
    df_scr.drop('oh+transit', axis=1, inplace=True)

    df_scr.loc[:, 'Allocation'] = np.where(df_scr.ORG != pcba_site,
                                           df_scr.iloc[:, 3:-5].sum(axis=1),  # [3:-5] refer to the right data columns
                                           None)

    df_scr.loc[:, 'Gap_after'] = np.where(df_scr.ORG != pcba_site,
                                          df_scr.Gap_before + df_scr.Allocation,
                                          None)

    df_scr.loc[:, 'Recovery'] = np.where((df_scr.ORG != pcba_site),
                                         np.where(df_scr.Gap_before >= 0,
                                                  'No gap',
                                                  np.where(df_scr.Gap_after < 0,
                                                           'No recovery',
                                                           'TBD')),
                                         None)

    # update with the correct recovery date for TBD
    df_scr.set_index(
        ['TAN', 'ORG', 'BU', 'Backlog', 'OH', 'In-transit', 'Gap_before', 'Allocation', 'Gap_after', 'Recovery'],
        inplace=True)
    df_scr.reset_index(inplace=True)
    dfx = df_scr[(df_scr.Recovery == 'TBD') & (df_scr.ORG != pcba_site)]
    dfx.set_index(['TAN', 'ORG'], inplace=True)
    df_scr.set_index(['TAN', 'ORG'], inplace=True)

    for ind in dfx.index:
        dfy = dfx.loc[ind, :]
        dfy = dfy[dfy.notnull()]

        last_allocation_date = dfy.index[-1]
        df_scr.loc[ind, 'Recovery'] = last_allocation_date

    df_scr.reset_index(inplace=True)
    df_scr.set_index(
        ['TAN', 'ORG', 'BU', 'Backlog', 'OH', 'In-transit', 'Gap_before', 'Allocation', 'Gap_after', 'Recovery'],
        inplace=True)

    return df_scr


def ss_ranking_overall_new(df_3a4, ranking_col, order_col='SO_SS', new_col='ss_overall_rank'):
    """
    根据priority_cat,OSSD,FCD, REVENUE_NON_REVENUE,C_UNSTAGED_QTY,按照ranking_col的顺序对SS进行排序。最后放MFG_HOLD订单.
    :param df_3a4:
    :param ranking_col:e.g. ['priority_rank', 'ORIGINAL_FCD_NBD_DATE', 'CURRENT_FCD_NBD_DATE','rev_non_rev_rank',
                        'C_UNSTAGED_QTY', 'SO_SS','PO_NUMBER']
    :param order_col:'SO_SS'
    :param new_col:'ss_overall_rank'
    :return: df_3a4
    """
    # removed cancelled orders - this part is different from summary_3a4 automation
    df_3a4.loc[:, 'cancelled'] = np.where(df_3a4.ORDER_HOLDS.notnull(),
                                          np.where(df_3a4.ORDER_HOLDS.str.contains('cancel', case=False),
                                                   'YES',
                                                   'NO'),
                                          'NO')
    df_3a4 = df_3a4[df_3a4.cancelled != 'YES'].copy()

    # Below create a rev_rank for reference -  currently not used in overall ranking
    ### change non-rev orders unstaged $ to 0
    df_3a4.loc[:, 'C_UNSTAGED_DOLLARS'] = np.where(df_3a4.REVENUE_NON_REVENUE == 'NO',
                                                   0,
                                                   df_3a4.C_UNSTAGED_DOLLARS)

    #### 生成ss_unstg_rev并据此排序
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

    ##### Step3: sort the SS per ranking columns and Put MFG hold orders at the back
    df_3a4.sort_values(by=ranking_col, ascending=True, inplace=True)
    # Put MFG hold orders at the back
    df_hold = df_3a4[df_3a4.MFG_HOLD == 'Y'].copy()
    df_3a4 = df_3a4[df_3a4.MFG_HOLD != 'Y'].copy()
    df_3a4 = pd.concat([df_3a4, df_hold], sort=False)

    ##### Step3: create rank# and put in 3a4
    rank = {}
    order_list = df_3a4[order_col].unique()
    for order, rk in zip(order_list, range(1, len(order_list) + 1)):
        rank[order] = rk
    df_3a4.loc[:, new_col] = df_3a4[order_col].map(lambda x: rank[x])

    return df_3a4


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
    df_3a4 = pd.read_csv(f_3a4, encoding='ISO-8859-1', parse_dates=['CURRENT_FCD_NBD_DATE', 'ORIGINAL_FCD_NBD_DATE'],
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
    # extract BU info for TAN from SCR
    tan_bu = extract_bu_from_scr(df_scr)

    # Pivot df_scr 并处理日期格式
    df_scr = df_scr.pivot_table(index=['planningOrg', 'TAN'], columns='SCRDate', values='SCRQuantity', aggfunc=sum)
    df_scr.columns = df_scr.columns.map(lambda x: x.date())

    # versionless df_scr
    df_scr = change_supply_to_versionless_and_addup_supply(df_scr, pn_col='TAN')

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

    # Rank the orders
    df_3a4 = ss_ranking_overall_new(df_3a4, ranking_col, order_col='SO_SS', new_col='ss_overall_rank')

    # (do below after ranking) Process 3a4 BOM base on FLB_TAN col
    df_bom = generate_df_order_bom_from_flb_tan_col(df_3a4, supply_dic_tan.keys())
    df_3a4 = update_order_bom_to_3a4(df_3a4, df_bom)

    # create backlog dict for Tan exists in SCR
    blg_dic_tan = create_blg_dict_per_sorted_3a4_and_selected_tan(df_3a4, supply_dic_tan.keys())

    # pivot df_oh
    df_oh = df_oh.pivot_table(index=['planningOrg', 'TAN'], values='OH', aggfunc=sum)

    # versionless df_oh
    df_oh = change_supply_to_versionless_and_addup_supply(df_oh, pn_col='TAN')

    # 生成OH dict；
    oh_dic_tan = created_oh_dict_per_df_oh(df_oh, pcba_site)

    # Oh to fulfill backlog per site. update blg_dic_tan accordingly
    blg_dic_tan = fulfill_backlog_by_oh(oh_dic_tan, blg_dic_tan)

    # pivot df_transit
    df_transit = df_transit.pivot_table(index=['planningOrg', 'TAN'], columns='ETA_date', values='In-transit_quantity',
                                        aggfunc=sum)
    df_transit.columns = df_transit.columns.map(lambda x: x.date())

    # versionless df_oh
    df_transit = change_supply_to_versionless_and_addup_supply(df_transit, pn_col='TAN')

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

    # save the output file to excel
    dt = (pd.Timestamp.now() + pd.Timedelta(hours=8)).strftime('%m-%d %Hh%Mm')  # convert from server time to local
    if bu_list!=['']:
        bu=' '.join(bu_list)
        output_filename = pcba_site + ' SCR allocation (' + bu + ') ' + dt + '.xlsx'
    else:
        output_filename = pcba_site + ' SCR allocation (all BU) ' + dt + '.xlsx'

    df_scr.to_excel(os.path.join(base_dir_output,output_filename))

    return output_filename

