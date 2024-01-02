# -*- coding: utf-8 -*-
"""
Created on Wed Dec 13 21:12:08 2023

@author: patrick
"""
import numpy as np
import requests 
import urllib.parse
import pandas as pd
import urllib.robotparser
import asyncio
from time import perf_counter, sleep
import sys, os
from bs4 import BeautifulSoup
import re
import aiohttp
import json
from pprint import pprint, pformat
from tqdm.asyncio import tqdm_asyncio
from tqdm import tqdm
from pymongo import MongoClient
import matplotlib.pyplot as plt

"""
Structure de données de la base de donnée MongoDB / A revoir potentiellement on va devoir tout dénormaliser
Crée une deuxième table pour stocker toutes les URLS ?


{
    "site_web_url": ..., "media_name": ... , "media_coverage" :  ... ,  "media_diffusion" : ... , 
    "media_location" : ... , "coverage" : ... , "true_country" : ..., 
    "sitemaps_xml": [ 
                        {url:xml1,  "has_been_scrapped" : True ,"is_responding": True,  "parent_xml" : robots_txt_url, "depth":0}, #l'url a été scrappée
                        {url:xml2, "has_been_scrapped" : True ,"is_responding": True, "parent_xml" : "xml1", "depth":1}, #l'url a été scrappée
                        {url:xml3, "has_been_scrapped" : False ,"is_responding": True, "parent_xml" : "xml1", "depth":1}, #dans cette config l'url n'a pas été testé
                        {url:xml4, "has_been_scrapped" : True ,"is_responding": True, "parent_xml" : "xml2", "depth":2}, #l'url a été scrappée
                        {url:xml5, "has_been_scrapped" : False ,"is_responding": False, "parent_xml" : "xml2", "depth":2}, #l'url n'a pas répondue
                    ]
    "html_urls": [
                    {"url": url1 , "xml_source" : xml4 ,"is_responding": True, "has_been_scrapped" : True, "text" : ... }  #l'url a été scrappée
                    {"url": url2 , "xml_source" : xml4 ,"is_responding": True, "has_been_scrapped" : False, "text" : None } #dans cette config l'url n'a pas été testé
                    {"url": url3 , "xml_source" : xml4 ,"is_responding": False, "has_been_scrapped" : False, "text" : None } #l'url n'a pas répondue
                    {"url": url3 , "xml_source" : xml4 ,"is_responding": False, "has_been_scrapped" : False, "text" : None } #l'url n'a pas répondue
                ]
                 
    "robots_txt_parsed" :  {'Disallow': ['/synsearch/', ... ], 'Allow': []} , #probablement à changer pour donner directement une regex de ce type ^(?!.*(video|author|topic)).*$'
    "last_time_scrapped" : hour,
    "is_responding" : False,
    "robots_txt":
}
"""

class MongoDB_scrap_async():

    def __init__(self, port_forwarding=27017, test=True):
        self.client=MongoClient('localhost', port=port_forwarding)
        self.ping_MongoDB()
        self.show_all("scrapping", "urls_sitemap_html")

        self.timeout_robots=60
        self.timeout_xml=40
        
        if not self.test_collection_in_database_exists("scrapping", "urls_sitemap_html", print_arg=False):
            self.insert_data("scrapping", "urls_sitemap_html", 
                             MongoDB_scrap_async.create_dictio_data_from_csv("medias_per_countries.csv", test=test))
        
        self.show_all("scrapping", "urls_sitemap_html", print_arg=True, random=True)
    
    def __str__(self):
        databases=[dict(db) for db in self.client.list_databases()]
        
        for index, database in enumerate(databases):
            if database["name"] not in ["admin", "config", "local"]:
                databases[index]["collections"]=self.client[database["name"]].list_collection_names()
                
                liste=[(database["name"], collection_name) for collection_name in databases[index]["collections"]]
                    
                dicionnaire_inter=dict(zip(databases[index]["collections"], 
                                                        [self.show_all(database["name"], collection_name, max_items=2) for collection_name in databases[index]["collections"]])
                                                    )
                databases[index]["collections"]=dicionnaire_inter
            
        
        return str(databases)
    
    def __repr__(self):
        databases=[dict(db) for db in self.client.list_databases()]
        
        for index, database in enumerate(databases):
            if database["name"] not in ["admin", "config", "local"]:
                databases[index]["collections"]=self.client[database["name"]].list_collection_names()
                
                liste=[(database["name"], collection_name) for collection_name in databases[index]["collections"]]
                    
                dicionnaire_inter=dict(zip(databases[index]["collections"], 
                                                        [self.show_all(database["name"], collection_name, max_items=5) for collection_name in databases[index]["collections"]])
                                                    )
                databases[index]["collections"]=dicionnaire_inter
        
        return pformat(databases, indent=4, width=1)

    def ping_MongoDB(self):
        try:
            self.client.admin.command('ping')
            print("Pinged your deployment. You successfully connected to MongoDB!")
        except Exception as e:
            print(e)
    
    def test_database_exists(self, database_name, print_arg=False):
        dblist = self.client.list_database_names()
        if database_name in dblist:
            if print_arg:
                print("\nThe database : <<{}>> exists.".format(database_name))
        else:
            if print_arg:
                print("\nThe database : <<{}>> doesn't exists.".format(database_name))
        return database_name in dblist
    
    def test_collection_in_database_exists(self, database_name, collection_name, print_arg=False): 
        if self.test_database_exists(database_name, print_arg):
            col_list = self.client[database_name].list_collection_names()
            if (collection_name in col_list):
                if print_arg :
                    print("The collection : <<{}>> in the database : <<{}>> exists.".format(collection_name, database_name))
            else:
                if print_arg :
                    print("The collection : <<{}>> in the database : <<{}>> doesn't exist.".format(collection_name, database_name))
            return collection_name in col_list
        else:
            return False
    
    def insert_data(self, mydatabase, collection_name, data):
        print("========= Importation des données ===========")
        collection_mongo = self.client[mydatabase][collection_name]
        x = collection_mongo.insert_many(data)
        print(("========= L'importation des {} lignes a été effectuée===========").format(len(data)))
    
    def show_all(self, database_name, collection_name, random=True, print_arg=False, max_items=5):
        if self.test_collection_in_database_exists(database_name, collection_name, print_arg=print_arg):
            if random:
                cursor=self.client[database_name][collection_name].aggregate(
                                                                            [ { "$sample": { "size": max_items } } ]
                                                                            )
            else:
                cursor=self.client[database_name][collection_name].find().limit(max_items)
            
            print("\nNombre d'éléments : {}".format(self.client[database_name][collection_name].count_documents({})))
            
            liste_result=list(cursor)

            for index, document in enumerate(cursor):
                if index<max_items:
                    pprint(document)
                else:
                    break
            return liste_result
        return None 
    
    def create_dictio_data_from_csv(path, test=False):
        df_urls=pd.read_csv(path).replace(np.NaN, None)
        df_urls.loc[:, "url"]=df_urls.loc[:, "url"].apply(lambda x:x.replace("http:", "https:"))

        if test:
            liste_countries=['United Kingdom','United States','Nigeria','South Africa','India','New Zealand','Philippines',
                            'Ireland','Australia','Africa Regional','Bangladesh','Canada','Ghana','Pakistan',
                            'Zambia','International','Asia Regional','Somalia','Sri Lanka',
                            'Cuba','Sierra Leone','Kenya','Near and Middle East Regional',
                            'Ethiopia','Spain','Israel','Liberia','China']
            liste_countries=['United States']
            
            df_urls=df_urls.loc[df_urls.loc[:, "true_country"].isin(liste_countries), :]
        

        list_dictios=list(df_urls.apply(lambda row:
                                    {
                                    "_id": row["id"],
                                    "url":row["url"],
                                    "media_name":row["media_name"], 
                                    "media_coverage": row["media_coverage"],
                                    "media_subject":row["media_subject"],
                                    "media_language":row["media_language"],
                                    "media_location":row["media_location"],
                                    "coverage":row["coverage"],
                                    "true_country":row["true_country"],
                                    "sitemaps_xml": [],
                                    "html_urls": [],
                                    "last_time_scrapped" : None,
                                    "is_responding" : None,
                                    "user_agent_rules":None,
                                    "url_robots_txt":"{}robots.txt".format('{}://{}/'.format(urllib.parse.urlparse(row["url"]).scheme,
                                                                                             urllib.parse.urlparse(row["url"]).netloc )),
                                    "url_root":'{}://{}/'.format(urllib.parse.urlparse(row["url"]).scheme,
                                                                urllib.parse.urlparse(row["url"]).netloc )
                                    },
                                    axis=1))
        return list_dictios
    
    async def parser_xml(session, dict_url_depth):
        sitemap, url, =[], []
        has_been_scrapped=False
        is_responding=False
        error=False
        try:
            async with session.get(dict_url_depth["url"]) as response:
                html = await response.text()
                soup= BeautifulSoup(html, features="xml")
                sitemap=list(map(lambda x:{"url":x.get_text(),
                                           "has_been_scrapped" : False ,
                                           "is_responding": True,
                                           "parent_xml":dict_url_depth["url"], 
                                           "depth":dict_url_depth["depth"]+1
                                           }, soup.select("sitemap loc")))
                #ici on a besoin de l'url d'origine et de la profondeur (à voir si on garde ces infos), possibilité de ajouter ces infos après lors du retour
                """
                url=list(map(lambda x:{"url": x.get_text() ,
                                        "has_been_scrapped" : False,
                                        "is_responding": True, 
                                        "xml_source" : dict_url_depth["url"] , 
                                        "text" : None }, soup.select("url loc")))"""
                
                url=list(map(lambda x:{"url": x.get_text() ,
                                        "has_been_scrapped" : False,
                                        "is_responding": True, 
                                        "xml_source" : dict_url_depth["url"] , 
                                        "text" : None }, list(filter(lambda x:"climat" in x.get_text() , soup.select("url loc")))))
                
                
                

                has_been_scrapped=True
                is_responding=True
        except Exception as e:
            error=str(e)
        finally:
            return sitemap, url, has_been_scrapped, is_responding, error
    

    async def parser_robots(session, url):
        res={"user_agent_rules":{"Disallow":[], "Allow":[]}, "sitemaps_xml": [], "is_responding":False}
        try:
            async with session.get(url) as response:
                res["is_responding"]=True
                text_split = await response.text()
                key="*" # si on tombe sur un robots.txt avec aucun user-agent au début, on considère que c'est le générique
                for k in text_split.splitlines():
                    try:
                        if len(k)!=0 and k[0]!="#": # on évite les lignes commentées ou vides
                            if "user-agent:" in k.lower():
                                key=k.split(":", 1)[1].strip(' ')  # la clé va passer de user-agent en user-agent 
                            elif "disallow" in k.lower().strip()[:8] and key=="*": # tant que la clé vaut * on ajoute
                                res["user_agent_rules"]["Disallow"].append(k.split(":",1)[1].strip(' '))
                            elif "allow" in k.lower().strip()[:5] and key=="*": # tant que la clé vaut * on ajoute
                                res["user_agent_rules"]["Allow"].append(k.split(":",1)[1].strip(' '))
                            elif "sitemap" in k.lower().strip()[:7] and "http" in k: # on prend tous les sitemaps
                                res["sitemaps_xml"].append(
                                                    {   
                                                        "url":k.split(":", 1)[1].strip(' '), 
                                                        "has_been_scrapped" : False ,
                                                        "is_responding": True,
                                                        "parent_xml" : url, 
                                                        "depth":0
                                                    }
                                                        )
                    except Exception as e: 
                        # on ne casse pas la boucle mais on enregistre l'erreur quand même pour les logs/stats
                        # Il est possible qu'une seule ligne ait été mal renseignée
                        res["Exception"]=str(e)
                        res["Detail_Exception"]=repr(e)

        except asyncio.TimeoutError as e:
            res["Exception"]="asyncio.TimeoutError"
            res["Detail_Exception"]=repr(e)
            res["is_responding"]=False
        except aiohttp.ClientConnectionError as e:
            res["Exception"]="aiohttp.ClientConnectionError"
            res["Detail_Exception"]=repr(e)
            res["is_responding"]=False
        except aiohttp.ClientOSError as e:
            res["Exception"]="aiohttp.ClientOSError"
            res["Detail_Exception"]=repr(e)
            res["is_responding"]=False
        except aiohttp.ClientConnectorError as e:
            res["Exception"]="aiohttp.ClientConnectorError"
            res["Detail_Exception"]=repr(e)
            res["is_responding"]=False
        except aiohttp.ClientProxyConnectionError as e:
            res["Exception"]="aiohttp.ClientProxyConnectionError"
            res["Detail_Exception"]=repr(e)
            res["is_responding"]=False
        except aiohttp.ClientSSLError as e:
            res["Exception"]="aiohttp.ClientSSLError"
            res["Detail_Exception"]=repr(e)
            res["is_responding"]=False
        except aiohttp.ClientConnectorSSLError as e:
            res["Exception"]="aiohttp.ClientConnectorSSLError"
            res["Detail_Exception"]=repr(e)
            res["is_responding"]=False
        except aiohttp.ClientConnectorCertificateError as e:
            res["Exception"]="aiohttp.ClientConnectorCertificateError"
            res["Detail_Exception"]=repr(e)
            res["is_responding"]=False
        except aiohttp.ClientResponseError as e:
            res["Exception"]="aiohttp.ClientResponseError"
            res["Detail_Exception"]=repr(e)
            res["is_responding"]=False
        except aiohttp.ClientHttpProxyError as e:
            res["Exception"]="aiohttp.ClientHttpProxyError"
            res["Detail_Exception"]=repr(e)
            res["is_responding"]=False
        except aiohttp.WSServerHandshakeError as e:
            res["Exception"]="aiohttp.WSServerHandshakeError"
            res["Detail_Exception"]=repr(e)
            res["is_responding"]=False
        except aiohttp.ContentTypeError as e:
            res["Exception"]="aiohttp.ContentTypeError"
            res["Detail_Exception"]=repr(e)
            res["is_responding"]=False
        except aiohttp.ClientPayloadError as e:
            res["Exception"]="aiohttp.ClientPayloadError"
            res["Detail_Exception"]=repr(e)
            res["is_responding"]=False
        except aiohttp.InvalidURL as e:
            res["Exception"]="aiohttp.InvalidURL"
            res["Detail_Exception"]=repr(e)
            res["is_responding"]=False
        except Exception as e: # si jamais (très improbable)
            res["Exception"]=repr(e)
        finally:
            return res

    async def fetch_all(session, liste_url, fonction):
        tasks = []
        for url in liste_url:
            task = asyncio.create_task(fonction(session, url))
            tasks.append(task)
        res = await tqdm_asyncio.gather(*tasks)
        return res

    async def main(liste_url, fonction, timeout_total=None):
        """
        Note : la fonction de parsing est un argument, elle est à construire selon le cas d'utilisation (sitemap, robots.txt, ...)
        Cela pemet de garder le reste du code générique/ non adhérent à un cas d'utilisation
        """
        if timeout_total:
            timeout = aiohttp.ClientTimeout(total=timeout_total)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                results = await MongoDB_scrap_async.fetch_all(session, liste_url, fonction)
            await session.close()
            await asyncio.sleep(0.25)
        else:
            async with aiohttp.ClientSession() as session:
                results = await MongoDB_scrap_async.fetch_all(session, liste_url, fonction)
            await session.close()
            await asyncio.sleep(0.25)

        return results
    def list_url_climat(self):
        return list(self.client["scrapping"]["urls_sitemap_html"].aggregate(
                                                    [
                                                        {"$unwind": "$html_urls"}, 
                                                        {"$match" : 
                                                            {
                                                                "$and":
                                                                    [
                                                                        {
                                                                            "html_urls.url" : # doit contenir le mot climat
                                                                                                { 
                                                                                                    '$regex' : '^(.*(climat)).*$', 
                                                                                                    '$options' : 'i'
                                                                                                },  
                                                                        }
                                                                    ]
                                                            }
                                                        },
                                                        { "$project": 
                                                                { 
                                                                    "_id":0,
                                                                    "html_urls.url": 1, 
                                                                    "url" : 1
                                                                }
                                                        }
                                                    ]
                                            )
                                    )

    
    def scan_urls(self, liste_url, fonction, timeout_total):
        start = perf_counter()
        liste_results=asyncio.run(MongoDB_scrap_async.main(liste_url,fonction, timeout_total))
        stop = perf_counter()
        print("Temps d'execution de toutes les requêtes : {}\n".format(stop-start))
        return liste_results

    def scan_robots_txt(self):
        # on récupère l'ensemble des documents dans la BDD MongoDB (leur id, et l'url du robots.txt)
        reponse_mongoDB=self.client["scrapping"]["urls_sitemap_html"].find({}, {"_id":1, "url_robots_txt":1})

        # on récupère la liste des url et des ids mongodb
        liste_result=list(reponse_mongoDB)
        liste_url=list(map(lambda x:x["url_robots_txt"], liste_result))
        liste_id=list(map(lambda x:x["_id"], liste_result))

        # les resultats arrivent dans le même ordre que l'envoi 
        results_robots=self.scan_urls(liste_url, MongoDB_scrap_async.parser_robots, self.timeout_robots)
       
        start = perf_counter()
        # on peut donc zipper avec la liste des ids mongodb
        # on parcourt pour chaque réponse, on update le bon document dans la BDD mongodb
        # les MAJ de chaque document dans la BDD sont très rapides 
        for id, element in tqdm(list(zip(liste_id, results_robots))):
                self.client["scrapping"]["urls_sitemap_html"].update_one(
                        {'_id':id},
                        { "$set": 
                                    { 
                                    "user_agent_rules": element["user_agent_rules"],
                                    "sitemaps_xml" : element["sitemaps_xml"],
                                    "is_responding" : element["is_responding"]
                                    }
                        },
                        )
        stop = perf_counter()

        # print de quelques statistiques
        print("Temps d'éxecution des {} mises à jours : {}".format(len(liste_id), stop-start))
        print("Nb sitemaps non vides : {}\n".format(len(list((filter(lambda x:len(x["sitemaps_xml"])!=0, results_robots))))))

        exceptions_counts=pd.Series(list(map(lambda x:x["Exception"], list(filter(lambda x:"Exception" in x ,results_robots))))).value_counts()
        print("\n".join(list(exceptions_counts.reset_index().apply(lambda x:"{}:{}".format(x["index"], x["count"]), axis=1))))
        
        #Pour aller debugger les erreurs précisemment 
        #=========================== Gestion des bugs + http only no https =========================
        les_bugs=list(filter(lambda x:"Exception" in x[2], list(zip(liste_url,liste_id, results_robots))))
        liste_url_bugs=list(map(lambda x:x[0].replace("https", "http"), les_bugs))
        liste_id_bugs=list(map(lambda x:x[1], les_bugs))

    def url_waybackmachine(uri_netloc, regex):#inutile mais stock" pour le garder

        url_waybackmachine="https://web.archive.org/cdx/search/cdx?url={}/".format(uri_netloc)
        url_waybackmachine+="&fl=original,timestamp&matchType=prefix&collapse=urlkey"
        url_waybackmachine+="&filter=mimetype:text/html&filter=statuscode:200"
        url_waybackmachine+="&showSkipCount=true&output=json"
        url_waybackmachine+="&filter=original:{}".format(regex)

        return url_waybackmachine

    
    def deep_search_batch_sitemaps(self):
        #ens_sitemap=list(self.client["scrapping"]["urls_sitemap_html"].find({"sitemaps_xml" : {"$ne" : []}}))


        # a comprend si faut mettre $media_name, $_id, $url, 
        #il y a aussi la possiblité de prendre les x premiers elements (firstN) semble-t-il
        #mettre une limite, trier chronologiquement ?
        #Pour les documents en html voir pour les sitemaps, prendre les allow disallow faire un regex 

        start = perf_counter()
        sitemaps_unscraped=list(self.client["scrapping"]["urls_sitemap_html"].aggregate(
                                                    [
                                                        {"$match" : {"sitemaps_xml" : {"$ne" : []}}}, # récupère les documents avec sitemap non vides
                                                        {"$unwind": "$sitemaps_xml"}, # dénormalisation des sitemaps {A, B, [C, D]} => {A, B, C}, {A, B, D}
                                                        {"$match" : 
                                                            {
                                                                "$and":
                                                                    [
                                                                        {
                                                                            "sitemaps_xml.has_been_scrapped" : False # le sitemap a t-il été scrappé ?
                                                                        },
                                                                        {
                                                                            "sitemaps_xml.is_responding" : True # si le lien n'a pas été scrappé peut-être que le lien ne répondait tout simplement pas
                                                                        },
                                                                        {
                                                                            "sitemaps_xml.url" : # ne doit pas contenir le mot video, author, topic dans le lien du sitemap
                                                                                                { 
                                                                                                    '$regex' : '^(?!.*(video|author|topic|category)).*$', 
                                                                                                    '$options' : 'i'
                                                                                                },  
                                                                        }
                                                                    ]
                                                            }
                                                        },
                                                        {"$group":
                                                            {
                                                                "_id": "$url_root", # pour ne jamais scrapper le même site à chaque passe plusieurs fois 
                                                                "id_ref_site": # pour garder l'id du document dans la BDD MongoDB 
                                                                                {
                                                                                "$first": "$_id"
                                                                                },
                                                                "sitemaps_xml": 
                                                                                {
                                                                                "$first": "$sitemaps_xml"
                                                                                },
                                                            }
                                                        },
                                                        {"$limit" : 1000 }
                                                    ]
                                                )
                                            )
        stop = perf_counter()
        print("\nTemps de la requête : {}, extraction de {} urls".format(stop-start, len(sitemaps_unscraped)))
        

        # on récupère la liste des url et des ids mongodb
        liste_url_depth_to_scrap=list(map(lambda x:{"url":x["sitemaps_xml"]["url"], "depth":x["sitemaps_xml"]["depth"]}, sitemaps_unscraped))
        liste_id_site=list(map(lambda x:x["id_ref_site"], sitemaps_unscraped))

        results_sitemaps=self.scan_urls(liste_url_depth_to_scrap, MongoDB_scrap_async.parser_xml, self.timeout_xml)
        print("Nb Exceptions : {}".format(len(list(filter(lambda x:x[4], results_sitemaps)))))
        print("Nb xml Empty : {}".format(len(list(filter(lambda x:len(x[0])==0, results_sitemaps)))))
        print("Nb html Empty : {}".format(len(list(filter(lambda x:len(x[1])==0, results_sitemaps)))))
        print("Nb New xml : {}".format(sum(list(map(lambda x:len(x[0]), results_sitemaps)))))
        print("Nb New html : {}".format(sum(list(map(lambda x:len(x[1]), results_sitemaps)))))
        
        #breakpoint()

        start = perf_counter()
        for id, depth_url_parent, resultats in list(zip(liste_id_site, liste_url_depth_to_scrap, results_sitemaps)):
            self.client["scrapping"]["urls_sitemap_html"].update_one(
                                                { "_id": id},
                                                { "$set": 
                                                        { 
                                                            "sitemaps_xml.$[element].has_been_scrapped" : resultats[2],
                                                            "sitemaps_xml.$[element].is_responding" : resultats[3]
                                                        } 
                                                },
                                                upsert=False, # ne fait pas d'insert si aucun document trouvés
                                                array_filters=[ { "element.url": depth_url_parent["url"] }  ]
                                            )
            
            self.client["scrapping"]["urls_sitemap_html"].update_one(
                                                { "_id": id},
                                                { "$push": 
                                                        { 
                                                            "sitemaps_xml" :  
                                                            {
                                                                "$each" : resultats[0]
                                                            },
                                                            "html_urls" :  
                                                            {
                                                                "$each" : resultats[1]
                                                            }
                                                        }
                                                }
                                                )
        stop = perf_counter()
        print("Temps des insertions et mise à jours :{}".format(stop-start))

        print("Nb pages parlant du climat :{}".format(len(instance_Mongo.list_url_climat())))



            
        
        
        



if __name__=="__main__":

    n_cycles=500  #nombre de cycles de crawling
    crawling_robots=False #initlisation avec le crawling des robots.txt


    instance_Mongo=MongoDB_scrap_async(port_forwarding=27017, test=True)

    if crawling_robots:
        instance_Mongo.scan_robots_txt()


    stat_start_sitemaps=list(instance_Mongo.client["scrapping"]["urls_sitemap_html"].aggregate([{"$unwind": "$sitemaps_xml"},
                                                                                                {"$group": 
                                                                                                    {"_id" : "$url", 
                                                                                                     "count" : {"$sum" : 1}}
                                                                                                }
                                                                                                ]))
    stat_start_html=list(instance_Mongo.client["scrapping"]["urls_sitemap_html"].aggregate([{"$unwind": "$html_urls"},
                                                                                            {"$group":
                                                                                               {"_id" : "$url", 
                                                                                                "count" : {"$sum" : 1}
                                                                                                }
                                                                                            }
                                                                                            ]))
    stat_start_climat=instance_Mongo.list_url_climat()

    stat_start_sitemaps=sorted(stat_start_sitemaps, key=lambda x:x["count"])
    stat_start_html=sorted(stat_start_html, key=lambda x:x["count"])

    print("\nNb sitemaps start:{}".format(sum(list(map(lambda x:x["count"], stat_start_sitemaps)))))
    print("Nb html pages start :{}".format(sum(list(map(lambda x:x["count"], stat_start_html)))))
    print("Nb pages parlant du climat :{}".format(len(stat_start_climat)))
    
    
    start = perf_counter()
    for k in range(n_cycles):
        sleep(3)
        print("\n=========iteration: {}/{}========".format(k, n_cycles))
        instance_Mongo.deep_search_batch_sitemaps()
    stop = perf_counter()
    
    stat_end_sitemaps=list(instance_Mongo.client["scrapping"]["urls_sitemap_html"].aggregate([{"$unwind": "$sitemaps_xml"}, {"$group": {"_id" : "$url", "count" : {"$sum" : 1}}}]))
    stat_end_html=list(instance_Mongo.client["scrapping"]["urls_sitemap_html"].aggregate([{"$unwind": "$html_urls"}, {"$group": {"_id" : "$url", "count" : {"$sum" : 1}}}]))
    stat_end_climat=instance_Mongo.list_url_climat()
    stat_end_sitemaps=sorted(stat_end_sitemaps, key=lambda x:x["count"])
    stat_end_html=sorted(stat_end_html, key=lambda x:x["count"])

    print("\n================= Statistiques ===================")
    print("temps d'execution :{}".format(stop-start))
    print("Nb sitemaps :{}".format(sum(list(map(lambda x:x["count"], stat_end_sitemaps)))))
    print("Nb html pages :{}".format(sum(list(map(lambda x:x["count"], stat_end_html)))))
    print("Nb moyen de sitemaps par site :{}".format(np.mean(np.array(list(map(lambda x:x["count"], stat_end_sitemaps))))))
    print("Nb moyen de html pages par site :{}".format(np.mean(np.array(list(map(lambda x:x["count"], stat_end_html))))))
    print("Nb pages parlant du climat :{}".format(len(stat_end_climat)))
    
    figure, axes = plt.subplots(1, 2)
    axes[0].pie(list(map(lambda x:x["count"], stat_end_html)), labels=list(map(lambda x:x["_id"]+"\n"+str(x["count"]), stat_end_html)),autopct='%1.1f%%')
    axes[0].set_title("Distriution des pages html par site")
    axes[1].pie(list(map(lambda x:x["count"], stat_end_sitemaps)), labels=list(map(lambda x:x["_id"]+"\n"+str(x["count"]), stat_end_sitemaps)),autopct='%1.1f%%')
    axes[1].set_title("Distriution du nombre de sitemaps par site")
    plt.show()
    


    
    