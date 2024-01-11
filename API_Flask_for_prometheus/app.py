from flask import Flask, jsonify, request
from pymongo import MongoClient
from prometheus_client import Gauge, generate_latest

app = Flask(__name__)

"""En gros les lables, c'est des sortes de tool tips"""



def request_metrics(client, database, collection_sitemaps):
    res=list(client[database][collection_sitemaps].aggregate(
                                    [
                                        {
                                            "$unwind": "$sitemaps_xml"
                                        },
                                        {
                                            "$group": {
                                            "_id":None,
                                            "nb_liens_scrapped": {
                                                    "$sum": { "$cond": ["$sitemaps_xml.has_been_scrapped", 1, 0] }
                                                },
                                            "nb_reponse_positives": {
                                                "$sum": { "$cond": [{"$and" : [ "$sitemaps_xml.is_responding", "$sitemaps_xml.has_been_scrapped"]}, 1, 0] }
                                            },
                                            "nb_reponse_negatives": {
                                                "$sum": { "$cond": [ {"$and" : [ { "$not": "$sitemaps_xml.is_responding"}, { "$not": "$sitemaps_xml.has_been_scrapped"}]}, 1, 0] }
                                            },
                                            "nb_requetes": {
                                                "$sum": { "$cond": [ 
                                                    { "$or": [
                                                                {"$and" : [ "$sitemaps_xml.is_responding", "$sitemaps_xml.has_been_scrapped"]},
                                                                {"$and" : [ { "$not": "$sitemaps_xml.is_responding"}, { "$not": "$sitemaps_xml.has_been_scrapped"}]}
                                                            ]
                                                    }
                                                                , 1, 0] }
                                            },
                                            "nb_sitemaps_total": { "$sum": 1 }
                                            }
                                        },
                                        {
                                            "$project": {
                                                "nb_liens_scrapped":"$nb_liens_scrapped",
                                                "nb_reponse_negatives":"$nb_reponse_negatives",
                                                "nb_reponse_positives":"$nb_reponse_positives",
                                                "nb_sitemaps_total":"$nb_sitemaps_total",
                                                "nb_requetes":"$nb_requetes",
                                                "%_liens_scrappés":{ "$cond": [ { "$eq": [ "$nb_sitemaps_total", 0 ] }, None, { "$round": [{"$multiply":[{"$divide":["$nb_liens_scrapped","$nb_sitemaps_total"]},100]}, 2]}]},
                                                "%_requetes_reussies":{ "$cond": [ { "$eq": [ "$nb_requetes", 0 ] }, None, { "$round": [{"$multiply":[{"$divide":["$nb_reponse_positives","$nb_requetes"]},100]}, 2]}]}
                                            }
                                        },

                                    ]
                            ))
    return res


nb_liens_scrapped = Gauge('nb_liens_scrapped', "nb_liens_scrapped")
nb_reponse_negatives = Gauge('nb_reponse_negatives', "nb_reponse_negatives")
nb_reponse_positives = Gauge('nb_reponse_positives', "nb_reponse_positives")
nb_sitemaps_total = Gauge('nb_sitemaps_total', "nb_sitemaps_total")
nb_requetes = Gauge('nb_requetes', "nb_requetes")
prct_liens_scrappes = Gauge('prct_liens_scrappes', "prct_liens_scrappes")
prct_requetes_reussies = Gauge('prct_requetes_reussies', "prct_requetes_reussies")




@app.route('/metrics')
def hello():
    client=MongoClient('mongodb', port=27017) #attention si on est dans un réseau docker 
    database="scrapping"
    collection_sitemaps="sitemaps"
    request_metrics_res=request_metrics(client, database, collection_sitemaps)[0]
    
    list_res_metrics=list(request_metrics_res.values())[1:]
    list_gauges=[nb_liens_scrapped, nb_reponse_negatives, nb_reponse_positives, nb_sitemaps_total, nb_requetes, prct_liens_scrappes, prct_requetes_reussies]

    for k in range(len(list_gauges)):
        list_gauges[k].set(list_res_metrics[k])

    return generate_latest()

    
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)