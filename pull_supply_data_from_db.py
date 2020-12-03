from pymongo import MongoClient
import pandas as pd
from datetime import datetime
import os

def collect_scr_oh_transit_from_scdx(pcba_site):
    # %% connect to SCDx
    SCDX_URI=os.getenv('SCDX_URI') + 'admin?connectTimeoutMS=300000&authSource=admin&authMechanism=SCRAM-SHA-1'
    client = MongoClient(SCDX_URI)
    database = client["pmocscdb"]
    table = database["commonVersion"]

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
                                            "planningOrg": 1.0,
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
                                        "planningOrg": 1.0,
                                        "TAN": "$itemNumber",
                                        "BU": "$attributes.cisco.pfDemandRatio.largestBU",
                                        "OH": {"$add": [{"$ifNull": ["$measures.derivatives.siteOH.total", 0.0]},
                                                        {"$ifNull": ["$measures.derivatives.hubOH.total",0.0]}]},}},

                        {"$match": {"OH": {"$gt": 0} }},
                    ]


    pipeline_scr = [{"$match": {"archived": False,
                                "planningOrg": pcba_site,
                                "attributes.cisco.gsmGroup": {"$in": ["OEM/ODM", "GTM"]}}},

                    {"$unwind": {"path": "$measures.series.totalSupply",
                                        "preserveNullAndEmptyArrays": False}},

                    {"$unwind":{"path":"$attributes.cisco.pfDemandRatio"}},

                    {"$project": {"_id":0,
                                "version": 1.0,
                                 "planningOrg": "$planningOrg",
                                "BU": "$attributes.cisco.pfDemandRatio.largestBU",
                                "TAN": "$itemNumber",
                                "SCRDate": "$measures.series.totalSupply.date",
                                "SCRQuantity": "$measures.series.totalSupply.quantity",}}
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

    cursor1 = table.aggregate(pipeline_in_transit,allowDiskUse=False)
    cursor2 = table.aggregate(pipeline_df_oh,allowDiskUse=False)
    cursor3 = table.aggregate(pipeline_scr,allowDiskUse=False)
    cursor4 = table.aggregate(pipeline_sourcing_rule,allowDiskUse=False)

    df_intransit = pd.DataFrame()
    df_oh = pd.DataFrame()
    df_scr = pd.DataFrame()
    df_sourcing_rule = pd.DataFrame()

    for doc in cursor1:
        df = pd.DataFrame(doc, index=[0])
        df_intransit = df_intransit.append(df)
    for doc in cursor2:
        df = pd.DataFrame(doc, index=[0])
        df_oh = df_oh.append(df)
    for doc in cursor3:
        df = pd.DataFrame(doc, index=[0])
        df_scr = df_scr.append(df)
    for doc in cursor4:
        df = pd.DataFrame(doc, index=[0])
        df_sourcing_rule = df_sourcing_rule.append(df)

    client.close()

    return df_scr,df_oh,df_intransit,df_sourcing_rule


if __name__=='__main__':
    pcba_site='FOL'
    print(datetime.today().now())
    df_scr, df_oh, df_intransit, df_sourcing_rule=collect_scr_oh_transit_from_scdx(pcba_site)

    today = datetime.today()
    outPath = os.path.join(os.getcwd(), 'SCR_OH_Intransit_' + today.strftime('%m%d %Hh%Mm') + '.xlsx')
    writer = pd.ExcelWriter(outPath,engine='xlsxwriter')

    df_intransit.to_excel(writer, sheet_name='in-transit', index=False)
    df_oh.to_excel(writer, sheet_name='oh', index=False)
    df_scr.to_excel(writer, sheet_name='scr', index=False)
    df_sourcing_rule.to_excel(writer, sheet_name='sourcing_rule', index=False)
    writer.save()
    print(datetime.today().now())
    #writer.close()