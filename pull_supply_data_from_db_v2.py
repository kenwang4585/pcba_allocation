from pymongo import MongoClient
import pandas as pd
import os

def collect_scr_oh_transit_from_scdx(pcba_site):
    # %% connect to SCDx
    SCDX_URI="mongodb://pmocref:PmoCR3f@ims-mngdb-rtp-d-06:37600/"
    client = MongoClient(SCDX_URI)
    database = client["pmocscdb"]
    collection = database["commonVersion"]

    # Pipeline
    data_pipeline=[{"$match": {"archived": False,
                                #"planningOrg" : "JMX",
                                "measures.series.porBalance" : {"$exists": True},
                                #"itemNumber": "68-100911-02",
                               }},

                    # Stage 2
                    {"$project": {
                                "planningOrg" :"$planningOrg",
                                "itemNumber": "$itemNumber",
                                "largestBU":  "$attributes.cisco.pfDemandRatio.largestBU",
                                "largestPF":  "$attributes.cisco.pfDemandRatio.largestPF",
                                "porBalance": {
                                    "$map": {
                                        "input": "$measures.series.porBalance",
                                        "in": {
                                            "date": "$$this.date",
                                            "quantity": "$$this.quantity",
                                        },
                                    }
                                },
                                "siteOH": {"$ifNull" :["$measures.derivatives.siteOH.total" ,0.0]},
                                "hubOH": {"$ifNull" :["$measures.derivatives.hubOH.total" ,0.0]},

                                # if DF org=pcba_site, only take the FA&T inventory (WH+WIP); otherwise take Raw(WH) + FA&T(WIP)
                                "OH": {"$cond": {"if": {"$eq": ["$planningOrg", pcba_site]},
                                         "then": {"$add": [
                                             {"$ifNull": ["$measures.derivatives.siteOH.totalByClassCode.FA&T", 0.0]},
                                             {"$ifNull": ["$measures.derivatives.hubOH.totalByClassCode.FA&T", 0.0]}]},
                                         "else": {"$add": [
                                             {"$ifNull": ["$measures.derivatives.siteOH.totalByClassCode.FA&T", 0.0]},
                                             {"$ifNull": ["$measures.derivatives.hubOH.totalByClassCode.FA&T", 0.0]},
                                             {"$ifNull": ["$measures.derivatives.siteOH.totalByClassCode.Raw", 0.0]},
                                             {"$ifNull": ["$measures.derivatives.hubOH.totalByClassCode.Raw", 0.0]},
                                             ]}}, }, }},




                                "destinationSourcing": {
                                    "$map": {
                                        "input": "$relationships.sourcingRule.destination",
                                        "in": "$$this"
                                    },
                                }
                            }
                            },

                    # Stage 3
                    {"$unwind": {
                        "path": "$destinationSourcing",
                        "preserveNullAndEmptyArrays": True # optional
                    }},

# Stage 4
                    {$lookup: # Uncorrelated Subqueries # (supported as of MongoDB 3.6)
                    {
                        "from": "commonVersion",
                        "let": {
                            "itemNumber": "$itemNumber",
                            "planningOrg": "$destinationSourcing.destinationName"
                        },
                        "pipeline": [
                            {
                                "$match" : {
                                    "archived": false,
                                    "$expr" :{
                                        "$and": [
                                            {"$eq" :["$$itemNumber" ,"$itemNumber"]},
                                            {"$eq" :["$$planningOrg" ,"$planningOrg"]},
                                        ]
                                    }
                                }
                            },
                            {
                                "$project": {
                                    "planningOrg": "$planningOrg",
                                    "CT2R":   {"$ifNull" :["$attributes.partner.ct2rCPN" ,0]},
                                    "safetyStock":   {"$ifNull" :["$attributes.partner.safetyStockCPN" ,0]},
                                    "siSuk":   {"$ifNull" :["$attributes.partner.flexSukCPN" ,0]},
                                    "siteOH": {"$ifNull" :["$measures.derivatives.siteOH" ,{"total" :0.0}]},
                                    "hubOH": {"$ifNull" :["$measures.derivatives.hubOH" ,{"total" :0.0}]},
                                    "openOO": {
                                        "$map": {
                                            "input": {"$ifNull" :[ "$measures.series.openOO" ,[]]},
                                            "in": {
                                                "date": "$$this.date",
                                                "quantity": "$$this.quantity",
                                                "key" :"$$this.data.key",
                                                "detail": {
                                                    "sellPartnerOrg":  "$$this.data.detail.sellPartnerOrg",
                                                    "sellPartnerName":  "$$this.data.detail.sellPartnerName",
                                                    "status":  "$$this.data.detail.status",
                                                    "sellPartnerName":  "$$this.data.detail.sellPartnerName",
                                                    "sellPartnerName":  "$$this.data.detail.sellPartnerName",
                                                    "curReqDelDate": "$$this.data.detail.curReqDelDate",
                                                    "curComDelDate": "$$this.data.detail.curComDelDate",
                                                    "ct2rDelDate": "$$this.data.detail.ct2rDelDate",
                                                    "expectDelDate": "$$this.data.detail.expectDelDate",
                                                    "requiredQty": "$$this.data.detail.requiredQty",
                                                    "receivedQty": "$$this.data.detail.receivedQty",
                                                    "totalShippedQty":  "$$this.data.detail.totalShippedQty",
                                                },
                                                "receipts" :"$$this.data.receipts",
                                                "auditTrail" :"$$this.data.auditTrail",
                                            }
                                        }
                                    },
                                }
                            },
                            {
                                "$addFields": {
                                    "inTransit": {
                                        "total": {"$sum" :{"$ifNull" :["$openOO.detail.totalShippedQty" ,0]}},
                                    }
                                }
                            },
                        ],
                    as: "dfDigest"
                    }
                    },

                    # Stage 5
                    {
                    $group: {
                        "_id": "$_id",
                        "planningOrg" :{"$first" :"$planningOrg"},
                        "itemNumber": {"$first" :"$itemNumber"},
                        "largestBU":  {"$first" :"$largestBU"},
                        "largestPF":  {"$first" :"$largestPF"},
                        "siteOH": {"$first" :"$siteOH"},
                        "ghubOH": {"$first" :"$hubOH"},
                        "porBalance": {"$first" :"$porBalance"},
                        "df": {
                            "$push": {
                                "planningOrg": "$destinationSourcing.destinationName",
                                "sourcingRule": {"$ifNull" :["$destinationSourcing" ,null]},
                                "dfDigest": {"$ifNull" :[{"$arrayElemAt" :["$dfDigest" ,0]} ,null]},
                            }
                        }

                    }
                    },

                    # Stage 6
                    {
                    $sort: {
                        "planningOrg" :1,
                        "itemNumber" :1,
                    }
                    },

                    ]

                    # Created with Studio 3T, the IDE for MongoDB - https:/
                        /studio3t.co m/

                    );
