from pymongo import MongoClient
import pandas as pd
from datetime import datetime
import os

def collect_scr_oh_transit_from_scdx():
    # %% connect to SCDx

    client = MongoClient(
        "mongodb://pmocref:PmoCR3f@ims-mngdb-rtp-d-06:37600/admin?connectTimeoutMS=10000&authSource=admin&authMechanism=SCRAM-SHA-1")
    database = client["pmocscdb"]
    collection = database["commonVersion"]

    # %% got query from MongoDB

    pipeline_df_sourcing_rule = [
        {
            "$match": {
                "archived": False,
                "attributes.cisco.gsmGroup": {"$in": ["OEM/ODM", "GTM"]}
            }
        },
        {
            "$unwind": {
                "path": "$measures.series.openOO",
                "preserveNullAndEmptyArrays": False
            }
        },
        {
            "$unwind": {
                "path": "$measures.series.openOO.data.receipts",
                "preserveNullAndEmptyArrays": False
            }
        },
        {
            "$match": {
                "measures.series.openOO.data.receipts.receiptType": "ASN"
            }
        },
        {
            "$project": {
                "version": 1.0,
                "planningOrg": 1.0,
                "TAN": "$itemNumber",
                "BU": "$attributes.cisco.pfDemandRatio.largestBU",
                "transfer": {
                    "$filter": {
                        "input": "$relationships.sourcingRule.source",
                        "cond": {
                            "$eq": [
                                "$$this.sourceName",
                                "FOL"
                                # user input, to identify which DF sites it supports, backlog data filter by this way too.
                            ]
                        }
                    }
                },
                "ETA_date": "$measures.series.openOO.data.receipts.expectedDeliveryDate",
                "In-transit_quantity": "$measures.series.openOO.data.receipts.shippedQty"
            }
        },
        {
            "$unwind": {
                "path": "$transfer",
                "preserveNullAndEmptyArrays": False
            }
        },
        {
            "$project": {
                "transfer": 0.0
            }
        }
    ]

    pipeline_DF_OH = [
        {
            "$match": {
                "archived": False,
                "attributes.cisco.gsmGroup": {"$in": ["OEM/ODM", "GTM"]}
            },
        },
        {
            "$project": {
                "version": 1.0,
                "planningOrg": 1.0,
                "TAN": "$itemNumber",
                "BU": "$attributes.cisco.pfDemandRatio.largestBU",
                "OH": {
                    "$add": [
                        {
                            "$ifNull": [
                                "$measures.derivatives.siteOH.total",
                                0.0
                            ]
                        },
                        {
                            "$ifNull": [
                                "$measures.derivatives.hubOH.total",
                                0.0
                            ]
                        }
                    ]
                }
            }
        }
    ]

    pipeline_CM_Supply = [
        {
            "$match": {
                "archived": False,
                "planningOrg": "FOL",
                "attributes.cisco.gsmGroup": {"$in": ["OEM/ODM", "GTM"]}
            }},
        {
            "$unwind": {
                "path": "$measures.series.totalSupply",
                "preserveNullAndEmptyArrays": False}
        },
        {
            "$project": {
                "version": 1.0,
                "planningOrg": "$planningOrg",
                "BU": "$attributes.cisco.pfDemandRatio.largestBU",
                "TAN": "$itemNumber",
                "SCRDate": "$measures.series.totalSupply.date",
                "SCRQuantity": "$measures.series.totalSupply.quantity", }
        },
    ]

    pipeline_Sourcing_Rule = [
        {
            "$match": {
                "archived": False,
                "attributes.cisco.gsmGroup": {
                    "$in": [
                        "OEM/ODM",
                        "GTM"
                    ]
                }
            }
        },
        {
            "$project": {
                "version": 1.0,
                "DF_site": "$planningOrg",
                "transfers": {
                    "$filter": {
                        "input": "$relationships.sourcingRule.source",
                        "cond": {
                            "$eq": [
                                "$$this.sourceName",
                                "FOL"
                            ]
                        }
                    }
                },
                "TAN": "$itemNumber",
                "BU": "$attributes.cisco.pfDemandRatio.largestBU",
                "PF": "$attributes.cisco.pfDemandRatio.largestPF"
            }
        },
        {
            "$unwind": {
                "path": "$transfers",
                "preserveNullAndEmptyArrays": False,
            }
        },
        {
            "$project": {
                "version": "$version",
                "CM_site": "$transfers.sourceName",
                "DF_site": "$DF_site",
                "TAN": "$TAN",
                "BU": "$BU",
                "PF": "$PF",
                "Transit_time": "$transfers.leadTime"
            }
        },
        {
            "$match": {
                "Transit_time": {
                    "$ne": ""
                }
            }
        }
    ]

    cursor1 = collection.aggregate(
        pipeline_df_sourcing_rule,
        allowDiskUse=False
    )
    cursor2 = collection.aggregate(
        pipeline_DF_OH,
        allowDiskUse=False
    )
    cursor3 = collection.aggregate(
        pipeline_CM_Supply,
        allowDiskUse=False
    )
    cursor4 = collection.aggregate(
        pipeline_Sourcing_Rule,
        allowDiskUse=False
    )

    cursor4 = collection.aggregate(
        pipeline_Sourcing_Rule,
        allowDiskUse=False
    )
    # try:
    df_intransit = pd.DataFrame()  # build aN empty dataframe to put all dfs
    df_oh = pd.DataFrame()
    df_scr = pd.DataFrame()
    df_sourcing_rule = pd.DataFrame()
    # print(pd.DataFrame(cursor.find()))
    for doc in cursor1:
        df = pd.DataFrame(doc, index=[0])
        df_intransit = df_intransit.append(df)
    for doc in cursor2:
        df = pd.DataFrame(doc, index=[0])
        # print (df)
        df_oh = df_oh.append(df)
    for doc in cursor3:
        df = pd.DataFrame(doc, index=[0])
        # print (df)
        df_scr = df_scr.append(df)
    for doc in cursor4:
        df = pd.DataFrame(doc, index=[0])
        # print (df)
        df_sourcing_rule = df_sourcing_rule.append(df)

    client.close()

    return df_scr,df_oh,df_intransit,df_sourcing_rule


if __name__=='__main__':
    df_scr, df_oh, df_intransit, df_sourcing_rule=collect_scr_oh_transit_from_scdx()

    today = datetime.today()
    outPath = os.path.join(os.getcwd(), 'SCR_OH_Intransit_' + today.strftime('%m%d') + '.xlsx')
    writer = pd.ExcelWriter(outPath,engine='xlsxwriter')

    df_intransit.to_excel(writer, sheet_name='in-transit', index=False)
    df_oh.to_excel(writer, sheet_name='oh', index=False)
    df_scr.to_excel(writer, sheet_name='scr', index=False)
    df_sourcing_rule.to_excel(writer, sheet_name='sourcing_rule', index=False)
    writer.save()
    #writer.close()