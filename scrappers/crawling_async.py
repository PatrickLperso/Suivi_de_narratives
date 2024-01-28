"""
Usage:
  crawling_async.py  <host_name_mongo> <n_cycles> [--crawling_robots=<crawling_robots>]

Options:
  --crawling_robots=<crawling_robots>  Initlisation avec le crawling des robots.txt (0/1)  [default: 0]

Documentation:
    <n_cycles> nombre de cycles de crawling
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
from dateutil import parser
import datetime
from pprint import pprint, pformat
from tqdm.asyncio import tqdm_asyncio
from tqdm import tqdm
from pymongo import MongoClient
import matplotlib.pyplot as plt
from docopt import docopt
import nltk
import os
from nltk.corpus import stopwords
nltk.download('stopwords')
import dateutil

"""
Structure de données de la base de donnée MongoDB :

collection : sitemaps
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
                 
    "robots_txt_parsed" :  {'Disallow': ['/synsearch/', ... ], 'Allow': []} , #probablement à changer pour donner directement une regex de ce type ^(?!.*(video|author|topic)).*$'
    "last_time_scrapped" : hour,
    "is_responding" : False,
    "robots_txt": url
}

collection : htmls
{
    "url": ...,
    "mots_in_url" : [...], #les mots pertienents de l'url sont parsés et stockés pour ensuite définir un index dessus
    "media_name" :
    "id_media" : 
    "has_been_scrapped" : False,
    "xml_source" :
    "date": #la date est la date de modification autrement dit pas nécessairement (souvent le cas) la date de publication
    "text": none
}
"""

class MongoDB_scrap_async():

    def __init__(self, host_mongo, port_forwarding, crawling_robots, n_cycles, test=True):
        self.client=MongoClient(host_mongo, port=port_forwarding)
        self.ping_MongoDB()
        

        self.timeout_robots=60
        self.timeout_xml=60
        self.database="scrapping"
        self.collection_sitemaps="sitemaps"
        self.collection_htmls="htmls"
        self.crawling_robots=crawling_robots
        self.n_cycles=n_cycles

        # utilisation de set pour la perfomance + uniquement les elements de plus de 3 caractères les autres sont supprimés
        self.stopwords=set(filter(lambda x:len(x)>=3 and "'" not in x, set(stopwords.words('english')).union(set(["jpg", "wp", "content", "uploads", "htm",
                                                                  "upload", "html", "image", "video", "placeholder", "rtrmadp","articles", "article", "news","stories",
                                                                  "index", "2560", "1920", "1080", "720", "1280", "250", "0x0", "img", "tag", "opinion"]))))


        self.show_all(self.database, self.collection_sitemaps)

        if not self.test_collection_in_database_exists(self.database, self.collection_sitemaps, print_arg=False):
            self.insert_data(self.database, self.collection_sitemaps,
                             MongoDB_scrap_async.create_dictio_data_from_csv("medias_per_countries.csv", test=test))
        
        self.show_all(self.database, self.collection_sitemaps, print_arg=True, random=True)
    
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
    

    def sitemap_is_empty(self):
        return sum(list(map(lambda x:x["count"],list(self.client[self.database][self.collection_sitemaps].aggregate([{"$unwind": "$sitemaps_xml"},
                                                                                                {"$group": 
                                                                                                    {"_id" : "$url", 
                                                                                                     "count" : {"$sum" : 1}}
                                                                                                }
                                                                                                ])))))==0
    
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
                                    "last_time_scrapped" : None,
                                    "is_responding" : None,
                                    "user_agent_rules":None,
                                    "url_robots_txt":'{}://{}/robots.txt'.format(urllib.parse.urlparse(row["url"]).scheme,
                                                                                             urllib.parse.urlparse(row["url"]).netloc ),
                                    "url_root":'{}://{}/'.format(urllib.parse.urlparse(row["url"]).scheme,
                                                                urllib.parse.urlparse(row["url"]).netloc )
                                    },
                                    axis=1))
        return list_dictios
    
    async def parser_xml(self,session, dict_url_depth):
        sitemap, url, =[], []
        has_been_scrapped=False
        is_responding=False
        error=False

        try:
            async with session.get(dict_url_depth["url"]) as response:
                html = await response.text()
                soup= BeautifulSoup(html, features="lxml") #attention xml c'est de la merde 

                #Récupération des sitemaps
                sitemap=list(map(lambda x:{"url":x.get_text(),
                                           "has_been_scrapped" : False ,
                                           "is_responding": True,
                                           "parent_xml":dict_url_depth["url"], 
                                           "depth":dict_url_depth["depth"]+1
                                           }, soup.select("sitemap loc")))
                """
                Pour accélérer le temps des requêtes sur les regex : 
                https://medium.com/statuscode/how-to-speed-up-mongodb-regex-queries-by-a-factor-of-up-to-10-73995435c606
                Au final, on ne rentre pas dans ce cas, nos données sont suffisament bien parsées, un simple index sur l'array mots_in_url
                suffit et est très performant niveau requétage
                """
                #breakpoint()

                date_tag=None
                for k in range(2):
                    firstsoup=soup.select("url:nth-child({})".format(k+1))
                    if len(firstsoup):
                        firstsoup=firstsoup[0]
                        tags=[tag.name for tag in firstsoup.find_all()]
                        if "news\:publication_date" in tags:
                            date_tag="news\:publication_date"
                            break
                        elif "n\:publication_date" in tags:
                            date_tag="n\:publication_date"
                            break
                        elif "lastmod" in tags:
                            date_tag="lastmod"
                            break
                    
                if date_tag:
                    url=list(map(lambda x:{"url": x[0],
                                                "mots_in_url":list(filter(lambda mot:len(mot)>=3  and mot not in self.stopwords,urllib.parse.urlparse(x[0].lower())\
                                                                        .path.replace("&", "/").replace("#", "/").replace(",", "/")\
                                                                        .replace("+", "/").replace("_", "/").replace("-", "/").replace("%", "/")\
                                                                        .replace(".", "/").split("/"))),
                                                "has_been_scrapped" : False,
                                                "id_media": dict_url_depth["id_media"], 
                                                "media_name" : dict_url_depth["media_name"],
                                                "is_responding": True, 
                                                "xml_source" : dict_url_depth["url"] , 
                                                "date_day":parser.parse(x[1]).replace(hour=0, minute=0, second=0, microsecond=0) if x[1] else None, 
                                                "date":parser.parse(x[1]).replace(microsecond=0).replace(second=0) if x[1] else None,
                                                "text" : None }, 
                                                list(map(lambda x:[x.select("loc")[0].get_text(), 
                                                                            x.select("{}".format(date_tag))[0].get_text() if len(x.select("{}".format(date_tag))) else None],
                                            soup.select("url")))))
                else:
                    url=list(map(lambda x:{"url": x[0],
                            "mots_in_url":list(filter(lambda mot:len(mot)>=3  and mot not in self.stopwords,urllib.parse.urlparse(x[0].lower())\
                                                    .path.replace("&", "/").replace("#", "/").replace(",", "/")\
                                                    .replace("+", "/").replace("_", "/").replace("-", "/").replace("%", "/")\
                                                    .replace(".", "/").split("/"))),
                            "has_been_scrapped" : False,
                            "id_media": dict_url_depth["id_media"], 
                            "media_name" : dict_url_depth["media_name"],
                            "is_responding": True, 
                            "xml_source" : dict_url_depth["url"] , 
                            "date_day":None,
                            "date":None,
                            "text" : None }, 
                            list(map(lambda x:[x.select("loc")[0].get_text(), 
                                                        None],
                        soup.select("url")))))
                    
                
                #breakpoint()
                                
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
    
    def list_url_climat(self, list_match="climat", aggregate=True):
        if aggregate:
                return list(self.client[self.database][self.collection_htmls].aggregate(
                            [
                                {
                                    "$match" : 
                                                {
                                                    "$and":
                                                        [
                                                            {
                                                                "url" : # doit contenir la liste de mots clés
                                                                        { 
                                                                            '$regex' : '^(.*({list_match})).*$'.format(list_match=list_match), 
                                                                            '$options' : 'i'
                                                                        },  
                                                            }
                                                        ]
                                                }
                                },
                                {
                                    "$group":
                                                {"_id" : "$media_name","count" : {"$sum" : 1}}
                                }
                            ]
                            )
                            )


        return list(self.client[self.database][self.collection_htmls].aggregate(
                                                    [
                                                        {"$match" : 
                                                            {
                                                                "$and":
                                                                    [
                                                                        {
                                                                            "url" : # doit contenir la liste de mots clés
                                                                                    { 
                                                                                        '$regex' : '^(.*({list_match})).*$'.format(list_match=list_match),
                                                                                        '$options' : 'i'
                                                                                    },  
                                                                        }
                                                                    ]
                                                            }
                                                        },
                                                        { "$project": 
                                                                { 
                                                                    "_id":0,
                                                                    "id_media": 1, 
                                                                    "media_name": 1, 
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
        reponse_mongoDB=self.client[self.database][self.collection_sitemaps].find({}, {"_id":1, "url_robots_txt":1})

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
                self.client[self.database][self.collection_sitemaps].update_one(
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
        #regex=r".*(cop.?[1-2][0-9])|(climat.*)|(.*green)|(.*emission)|(.*oil)|(.*CO2)|(.*co2)|(.pollu).*"
        url_waybackmachine="https://web.archive.org/cdx/search/cdx?url={}/".format(uri_netloc)
        url_waybackmachine+="&fl=original,timestamp&matchType=prefix&collapse=urlkey"
        url_waybackmachine+="&filter=mimetype:text/html&filter=statuscode:200"
        url_waybackmachine+="&showSkipCount=true&output=json"
        url_waybackmachine+="&filter=original:{}".format(regex)

        return 
    
    def index_exists(self):
        return 'mots_in_url' in self.client[self.database][self.collection_htmls].index_information().keys()
    
    def creation_index(self):
        print("=============== création de l'index sur les mots parsés ============")
        self.client[self.database][self.collection_htmls].create_index('mots_in_url')

    
    def deep_search_batch_sitemaps(self):
        #ens_sitemap=list(self.client["scrapping"]["urls_sitemap_html"].find({"sitemaps_xml" : {"$ne" : []}}))

        # a comprend si faut mettre $media_name, $_id, $url, 
        #il y a aussi la possiblité de prendre les x premiers elements (firstN) semble-t-il
        #mettre une limite, trier chronologiquement ?
        #Pour les documents en html voir pour les sitemaps, prendre les allow disallow faire un regex 

        start = perf_counter()
        sitemaps_unscraped=list(self.client[self.database][self.collection_sitemaps].aggregate(
                            [
                                {
                                    "$match" : 
                                                {
                                                    "sitemaps_xml" : 
                                                    {
                                                        "$ne" : []
                                                    }
                                                }
                                }, # récupère les documents avec sitemap non vides

                                {
                                    "$unwind": "$sitemaps_xml"
                                }, # dénormalisation des sitemaps {A, B, [C, D]} => {A, B, C}, {A, B, D}
                                {
                                    "$match" : 
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
                                {
                                    "$group":
                                                {
                                                    "_id": "$url_root", # pour ne jamais scrapper le même site à chaque passe plusieurs fois 
                                                    "id_media": # pour garder l'id du document dans la BDD MongoDB 
                                                                    {
                                                                    "$first": "$_id"
                                                                    },
                                                    "sitemaps_xml": 
                                                                    {
                                                                    "$first": "$sitemaps_xml"
                                                                    },
                                                    "media_name": 
                                                                    {
                                                                    "$first": "$media_name"
                                                                    },
                                                }
                                },
                                {
                                    "$limit" : 1000 
                                }
                            ]
                        )
                    )
        stop = perf_counter()
        print("\nTemps de la requête : {}, extraction de {} urls".format(stop-start, len(sitemaps_unscraped)))
        
        
        # on récupère la liste des url et des ids mongodb
        liste_url_depth_to_scrap=list(map(lambda x:{"url":x["sitemaps_xml"]["url"], 
                                                    "depth":x["sitemaps_xml"]["depth"],
                                                    "id_media":x["id_media"],
                                                    "media_name":x["media_name"]}, sitemaps_unscraped))

        
        liste_id_site=list(map(lambda x:x["id_media"], sitemaps_unscraped))

        results_sitemaps=self.scan_urls(liste_url_depth_to_scrap, self.parser_xml, self.timeout_xml)

        print("Nb Exceptions : {}".format(len(list(filter(lambda x:x[4], results_sitemaps)))))
        print("Nb xml Empty : {}".format(len(list(filter(lambda x:len(x[0])==0, results_sitemaps)))))
        print("Nb html Empty : {}".format(len(list(filter(lambda x:len(x[1])==0, results_sitemaps)))))
        print("Nb New xml : {}".format(sum(list(map(lambda x:len(x[0]), results_sitemaps)))))
        print("Nb New html : {}".format(sum(list(map(lambda x:len(x[1]), results_sitemaps)))))
        
        start = perf_counter()
        for id, depth_url_parent, resultats in list(zip(liste_id_site, liste_url_depth_to_scrap, results_sitemaps)):
            #update des sitemaps scrappés
            self.client[self.database][self.collection_sitemaps].update_one(
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
            #insertion des nouveaux sitemaps
            self.client[self.database][self.collection_sitemaps].update_one(
                                                { "_id": id},
                                                { "$push": 
                                                        { 
                                                            "sitemaps_xml" :  
                                                            {
                                                                "$each" : resultats[0]
                                                            },
                                                        }
                                                }
                                                )
            #insertion des liens htmls
            if len(resultats[1]):
                self.client[self.database][self.collection_htmls].insert_many(resultats[1])
        


        stop = perf_counter()
        print("Temps des insertions et mise à jours :{}".format(stop-start))
        #print("\nNb pages html :{}".format(self.client[self.database][self.collection_htmls].count_documents({})))


    def crawling_procedure(self):

        if self.sitemap_is_empty():
            self.scan_robots_txt()
        else:
            if self.crawling_robots:
                anwser=input("Voulez-vous relancer le scrapping des robots.txt?[Y/n]")
                if anwser=="Y":
                    self.scan_robots_txt() 
                else:
                    pass
        
        if not self.index_exists():
            self.creation_index()

        start = perf_counter()
        for k in range(self.n_cycles):
            sleep(3)
            print("\n=========iteration: {}/{}========".format(k, self.n_cycles))
            self.deep_search_batch_sitemaps()

        stop = perf_counter()
        print(stop-start)

            
        
        
if __name__=="__main__":

    #n_cycles=50  #nombre de cycles de crawling
    #crawling_robots=True #initlisation avec le crawling des robots.txt

    arguments = docopt(__doc__)

    host_name_mongo=arguments["<host_name_mongo>"]
    n_cycles=int(arguments["<n_cycles>"])
    crawling_robots=int(arguments["--crawling_robots"]) 

    instance_Mongo=MongoDB_scrap_async(host_name_mongo, port_forwarding=27017, crawling_robots=crawling_robots, n_cycles=n_cycles, test=True)
    instance_Mongo.crawling_procedure()

        

    
"""

Prochaine évolution : Multiprocessing & concurrent (uniquement concurrent pour l'instant )
https://www.dataleadsfuture.com/aiomultiprocess-super-easy-integrate-multiprocessing-asyncio-in-python/

"""


    
    