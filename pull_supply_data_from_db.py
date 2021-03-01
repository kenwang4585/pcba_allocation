from pymongo import MongoClient
import pandas as pd
import os

def collect_scr_oh_transit_from_scdx_poc(pcba_site):
    # %% connect to SCDx

    SCDX_URI=os.getenv('SCDX_URI')
    client = MongoClient(SCDX_URI)
    database = client["pmocscdb"]
    collection = database["commonVersion"]

    # %% got query from MongoDB
    pipeline_in_transit = [{"$match": {"archived": False,
                                        "attributes.cisco.gsmGroup": {"$in": ["OEM/ODM", "GTM"]}}},

                                {"$unwind": {
                                            "path": "$measures.series.openOO",
                                            "preserveNullAndEmptyArrays": False}},

                                {"$unwind": {
                                            "path": "$measures.series.openOO.data.receipts",
                                            "preserveNullAndEmptyArrays": False}},

                                {"$match": {
                                            "measures.series.openOO.data.receipts.receiptType": "ASN"}},

                                {"$project": {'_id':0,
                                            "version": 1.0,
                                            "DF_site": '$planningOrg',
                                            "TAN": "$itemNumber",
                                            "BU": "$attributes.cisco.pfDemandRatio.largestBU",
                                            "transfer": {
                                                        "$filter": {
                                                                    "input": "$relationships.sourcingRule.source",
                                                                    "cond": {"$eq": ["$$this.sourceName",pcba_site]}
                                                                    }},
                                            "ETA_date": "$measures.series.openOO.data.receipts.expectedDeliveryDate",
                                            "In-transit_quantity": "$measures.series.openOO.data.receipts.shippedQty"}},

                               {"$unwind": {"path": "$transfer",
                                            "preserveNullAndEmptyArrays": False}},

                               {"$project": {"transfer": 0.0}}
                            ]

    pipeline_df_oh = [{ "$match": {"archived": False,
                                    "attributes.cisco.gsmGroup": {"$in": ["OEM/ODM", "GTM"]},}},

                        {"$project": {'_id':0,
                                        "version": 1.0,
                                        "DF_site": "$planningOrg",
                                        "TAN": "$itemNumber",
                                        "BU": "$attributes.cisco.pfDemandRatio.largestBU",
                                        # if DF org=pcba_site, only take the FA&T inventory (WH+WIP); otherwise take Raw(WH) + FA&T(WIP)
                                        "OH":  {"$cond": {"if" : {"$eq" : ["$planningOrg",pcba_site]},
                                                         "then": {"$add" : [{"$ifNull" : ["$measures.derivatives.siteOH.totalByClassCode.FA&T",0.0]},
                                                                           {"$ifNull" : ["$measures.derivatives.hubOH.totalByClassCode.FA&T",0.0]}]},
                                                         "else" : {"$add" : [{"$ifNull" : ["$measures.derivatives.siteOH.totalByClassCode.FA&T",0.0]},
                                                                             {"$ifNull" : ["$measures.derivatives.hubOH.totalByClassCode.FA&T",0.0]},
                                                                             {"$ifNull": ["$measures.derivatives.siteOH.totalByClassCode.Raw",0.0]},
                                                                             {"$ifNull": ["$measures.derivatives.hubOH.totalByClassCode.Raw",0.0]},
                                                                            ]}},},}},

                        {"$match": {"OH": {"$gt": 0} }}
                      ]


    pipeline_scr = [{"$match": {"archived": False,
                                "planningOrg": pcba_site,
                                "attributes.cisco.gsmGroup": {"$in": ["OEM/ODM", "GTM"]}}},

                    {"$unwind": {"path": "$measures.series.totalSupply",
                                        "preserveNullAndEmptyArrays": False}},

                    {"$unwind":{"path":"$attributes.cisco.pfDemandRatio"}},

                    {"$project": {"_id":0,
                                "version": 1.0,
                                 "planningOrg": 1.0,
                                "BU": "$attributes.cisco.pfDemandRatio.largestBU",
                                  "PF": "$attributes.cisco.pfDemandRatio.largestPF",
                                "TAN": "$itemNumber",
                                "date": "$measures.series.totalSupply.date",
                                "quantity": "$measures.series.totalSupply.quantity",}}
                    ]

    pipeline_sourcing_rule = [{"$match": {"archived": False,
                                        "attributes.cisco.gsmGroup": {"$in": ["OEM/ODM","GTM"]}}},

                              {"$project": {"version": 1.0,
                                            "DF_site": "$planningOrg",
                                            "transfers": {"$filter": {"input": "$relationships.sourcingRule.source",
                                                                      "cond": {"$eq": ["$$this.sourceName",pcba_site]}}},
                                            "TAN": "$itemNumber",
                                            "BU": "$attributes.cisco.pfDemandRatio.largestBU",
                                            "PF": "$attributes.cisco.pfDemandRatio.largestPF"}},

                              {"$unwind": {"path": "$transfers",
                                            "preserveNullAndEmptyArrays": False,}},

                              {"$project": {"_id":0,
                                            "version": "$version",
                                              "CM_site": "$transfers.sourceName",
                                              "DF_site": "$DF_site",
                                              "TAN": "$TAN",
                                              "BU": "$BU",
                                              "PF": "$PF",
                                              "Transit_time": "$transfers.leadTime"}},

                              {"$match": {"Transit_time": {"$ne": ""},
                                            "DF_site":{'$ne':'FOL'},#can't make it generally pcba_site yet due to other org like FDO have same PCBA and DF org name
                                           }}
                            ]

    cursor1 = collection.aggregate(pipeline_in_transit,allowDiskUse=False)
    cursor2 = collection.aggregate(pipeline_df_oh,allowDiskUse=False)
    cursor3 = collection.aggregate(pipeline_scr,allowDiskUse=False)
    cursor4 = collection.aggregate(pipeline_sourcing_rule,allowDiskUse=False)

    client.close()

    df_intransit=pd.DataFrame(cursor1)
    df_oh=pd.DataFrame(cursor2)
    df_scr=pd.DataFrame(cursor3)
    df_sourcing_rule=pd.DataFrame(cursor4)

    # remove non-relevant DF OH based on sourcing rules
    df_sourcing_rule.loc[:,'org_pn']=df_sourcing_rule.DF_site+'_'+df_sourcing_rule.TAN
    df_oh.loc[:,'org_pn']=df_oh.DF_site+'_'+df_oh.TAN

    df_oh=df_oh[df_oh.org_pn.isin(df_sourcing_rule.org_pn)] # removing no-relevant OH
    df_sourcing_rule.drop('org_pn', axis=1, inplace=True)
    df_oh.drop('org_pn', axis=1, inplace=True)

    df_scr.set_index('TAN',inplace=True)
    df_oh.set_index('TAN', inplace=True)
    df_intransit.set_index('TAN', inplace=True)
    df_sourcing_rule.set_index('TAN', inplace=True)

    return df_scr,df_oh,df_intransit,df_sourcing_rule


if __name__=='__main__':
    pcba_site='JPE'
    a=pd.Timestamp.now()
    df_scr, df_oh, df_intransit, df_sourcing_rule=collect_scr_oh_transit_from_scdx_poc(pcba_site)

    outPath = os.path.join(os.getcwd(), pcba_site + ' SCR_OH_Intransit_' + a.strftime('%m%d %Hh%Mm') + '.xlsx')
    writer = pd.ExcelWriter(outPath,engine='xlsxwriter')
    
    df_intransit.to_excel(writer, sheet_name='in-transit', index=False)
    df_oh.to_excel(writer, sheet_name='df-oh', index=False)
    df_scr.to_excel(writer, sheet_name='por', index=False)
    df_sourcing_rule.to_excel(writer, sheet_name='sourcing-rule', index=False)
    writer.save()
    #writer.close()
    b = pd.Timestamp.now()
    print(b - a)