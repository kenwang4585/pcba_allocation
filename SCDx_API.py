### Script to invoke POR Balance API @SCDx Direct
### Supported by Eli Rothbart - Jan 22, 2021

import requests, json
import pandas as pd
import pprint
import time
import os

def collect_scr_oh_transit_from_scdx_prod(pcba_site,item):
    """
    Extract data from the SCDX production db API built up by Eli Rothbart, for the purpose of PCBA allocation.
    :param pcba_site: must be a valid pcba org name
    :param item: can be '*' for all materials
    :return: por, df_oh, transit,sourcing rules in dataframe
    """
    url='http://10.154.120.58:6543/SCDxScrData/'+ pcba_site + '/' + item

    print('Download from SCDx-Production!')

    # API get invocation
    r = requests.get(url) # invokes API, stores response into varable r
    # Parsing message using json library
    status=r.json()["status"]
    message=r.json()["message"]
    rowsReturned=r.json()["rowsReturned"]
    dataset=json.loads(r.json()['data']) # deserializes response into list
    #dataset = json.loads(requests.get('http://10.154.120.58:6543/SCDxScrData/FOL/68-100465-04').json()['data'])
    #pprint.pprint(dataset)

    por_list,sr_org_list,sr_bu_list,sr_pf_list,sr_tan_list,sr_split_list,sr_lt_list=[],[],[],[],[],[],[]
    transit_org_list,transit_tan_list,transit_bu_list,transit_qty_list,transit_eta_list=[],[],[],[],[]
    oh_org_list,oh_tan_list,oh_bu_list,oh_qty_list=[],[],[],[]

    for data in dataset:
        tan=data['itemNumber']
        bu=data['largestBU']
        pf=data['largestPF']
        sm_oh=data['siteOH']
        porPlanDate=data['porPlanDate']
        porBalance=data['porBalance']
        porBalance.append({'date':pd.Timestamp.now(),'quantity':sm_oh}) # include SM OH

        # PCBA site POR
        df_por=pd.DataFrame(porBalance)
        df_por.loc[:, 'planningOrg'] = pcba_site
        df_por.loc[:,'TAN']=tan
        df_por.loc[:, 'BU'] = bu
        df_por.loc[:,'PF']=pf

        df_por.loc[:,'porPlanDate']=porPlanDate
        por_list.append(df_por)

        # df sites OH
        dfsite_data=data['df']
        #pprint.pprint(dfsite_data)
        for dfsite in dfsite_data:
            # for sourcing rules
            if dfsite['sourcingRule']!=None:
                org = dfsite['sourcingRule']['destinationName']
                split=dfsite['sourcingRule']['supplierSplit']
                leadtime=dfsite['sourcingRule']['leadTime']

                sr_org_list.append(org)
                sr_bu_list.append(bu)
                sr_pf_list.append(pf)
                sr_tan_list.append(tan)
                sr_split_list.append(split)
                sr_lt_list.append(leadtime)

                if tan=='68-100233-01':
                    pprint.pprint(dfsite)

            # below for intransit and df OH
            dfdigest = dfsite['dfDigest']
            if dfdigest:
                org = dfdigest['planningOrg']

                # for Intransit
                if len(dfdigest['openOO'])>0:
                    for transit_record in dfdigest['openOO']:
                        if len(transit_record['receipts'])>0:
                            transit=transit_record['receipts'][0]

                            if transit['receiptType']=='ASN':
                                qty=transit['shippedQty']
                                eta=transit['expectedDeliveryDate']

                                transit_org_list.append(org)
                                transit_bu_list.append(bu)
                                transit_tan_list.append(tan)
                                transit_qty_list.append(qty)
                                transit_eta_list.append(eta)

                # For DF OH data
                oh_qty = 0
                if dfdigest['siteOH']['total']>0:
                    oh_class = dfdigest['siteOH']['totalByClassCode']
                    # when pcba site is a combo site and when DF is same site just use FA&T; otherwise also include Raw
                    if org == pcba_site:
                        if 'FA&T' in oh_class.keys():
                            oh_qty += oh_class.get('FA&T')
                    else:
                        if 'FA&T' in oh_class.keys():
                            oh_qty += oh_class.get('FA&T')
                        if 'Raw' in oh_class.keys():
                            oh_qty += oh_class.get('Raw')

                if dfdigest['hubOH']['total'] > 0:
                    hub_class = dfdigest['hubOH']['totalByClassCode']
                    if org != pcba_site:  # when pcba site is a combo site and when DF is same site
                        if 'Raw' in hub_class.keys():
                            oh_qty += hub_class.get('Raw')

                oh_org_list.append(org)
                oh_bu_list.append(bu)
                oh_tan_list.append(tan)
                oh_qty_list.append(oh_qty)

    df_por=pd.concat(por_list)
    df_oh=pd.DataFrame({'DF_site':oh_org_list,'TAN':oh_tan_list,'BU':oh_bu_list,'OH':oh_qty_list})
    df_sourcing=pd.DataFrame({'DF_site':sr_org_list,'BU':sr_bu_list,'PF':sr_pf_list,'TAN':sr_tan_list,'Split':sr_split_list,'Transit_time':sr_lt_list})
    df_transit=pd.DataFrame({'DF_site':transit_org_list,'BU':transit_bu_list,'TAN':transit_tan_list,'ETA_date':transit_eta_list,'In-transit_quantity':transit_qty_list})

    df_por=df_por[['planningOrg','BU','PF','TAN','date','quantity','porPlanDate']]

    df_por.loc[:,'date']=df_por.date.astype('datetime64[ns]')
    df_transit.loc[:, 'ETA_date'] = df_transit.ETA_date.astype('datetime64[ns]')

    df_por.sort_values(by=['TAN','planningOrg','date'],inplace=True)

    return df_por, df_oh, df_transit,df_sourcing

if __name__=='__main__':
    pcba_site='FOL'
    start=time.time()
    df_por, df_oh, df_transit,df_sourcing = collect_scr_oh_transit_from_scdx_prod(pcba_site,'*')

    end=time.time()
    print('Total time: {}'.format(end-start))

    outPath = os.path.join(os.getcwd() + '/supply_file', pcba_site + '_por_oh_intransit_SCDx_Prod ' + pd.Timestamp.now().strftime('%m%d %Hh%Mm') + '.xlsx')
    writer = pd.ExcelWriter(outPath, engine='xlsxwriter')

    df_por.to_excel(writer, sheet_name='por', index=False)
    df_transit.to_excel(writer, sheet_name='in-transit', index=False)
    df_oh.to_excel(writer, sheet_name='df-oh', index=False)
    df_sourcing.to_excel(writer, sheet_name='sourcing-rule', index=False)
    writer.save()

