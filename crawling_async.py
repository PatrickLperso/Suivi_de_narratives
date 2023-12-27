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
from time import perf_counter
import sys, os
from bs4 import BeautifulSoup
import re
import aiohttp
import json
from pprint import pprint, pformat
from tqdm.asyncio import tqdm_asyncio
from tqdm import tqdm
from pymongo import MongoClient


"""
Json thought structure / ¨Potentiellement utiliser du MongoDB
{
    "site_web_url": ..., "media_name": ... , "media_coverage" :  ... ,  "media_diffusion" : ... , 
    "media_location" : ... , "coverage" : ... , "true_country" : ..., 
    "sitemaps_xml": [ 
                        {root_xml : { "has_been_scrapped" : True , "parent_xml" : None, "depth":0}},
                        {xml2 : { "has_been_scrapped" : True , "parent_xml" : "root_url", "depth":1}}, 
                        {xml3 : { "has_been_scrapped" : False , "parent_xml" : "root_url", "depth":1}}, 
                        {xml4 : { "has_been_scrapped" : True , "parent_xml" : "xml2", "depth":2}},
                        {xml5 : { "has_been_scrapped" : False , "parent_xml" : "xml2", "depth":2}},
                    ]
    "html_urls": [
                    {"url": url1 , "xml_source" : xml4 , "should_be_scrapped" : True, "has_been_scrapped" : True, "text" : ... }
                    {"url": url2 , "xml_source" : xml4 , "should_be_scrapped" : True, "has_been_scrapped" : False, "text" : None }
                    {"url": url3 , "xml_source" : xml4 , "should_be_scrapped" : False, "has_been_scrapped" : False, "text" : None }
                    {"url": url3 , "xml_source" : xml4 , "should_be_scrapped" : False, "has_been_scrapped" : False, "text" : None }
                 ]
    "robots_txt_parsed" :   ,
    "last_time_scrapped" : hour,
    "is_responding" : False,
    "nb_not_responding" : 2,
    "robots_txt":
}
"""

class MongoDB_scrap_async():

    def __init__(self, port_forwarding=27017, test=True):
        self.client=MongoClient('localhost', port=port_forwarding)
        self.ping_MongoDB()
        self.show_all("scrapping", "urls_sitemap_html")

        self.timeout_robots=20
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
            df_urls=df_urls.loc[df_urls.loc[:, "true_country"]=="United Kingdom", :]
        

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
                                    "nb_not_responding" : 0,
                                    "user_agent_rules":None,
                                    "url_robots_txt":"{}robots.txt".format('{}://{}/'.format(urllib.parse.urlparse(row["url"]).scheme,
                                                                                             urllib.parse.urlparse(row["url"]).netloc )),
                                    "url_root":'{}://{}/'.format(urllib.parse.urlparse(row["url"]).scheme,
                                                                urllib.parse.urlparse(row["url"]).netloc )
                                    },
                                    axis=1))
        return list_dictios
    
    async def parser_xml(session, url):
        try:
            async with session.get(url) as response:
                all_links=await list(filter(lambda x: "climat" in x.lower(), list(map(lambda x:"".join(x),
                            re.findall(r"(http|ftp|https)(:\/\/[\w_-]+(?:(?:\.[\w_-]+)+))([\w.,@?^=%&:\/~+#-]*[\w@?^=%&\/~+#-])", response.text())))))
        except:
            all_links=[]
        return all_links
    
    async def parser_robots(session, url):
        res={"user_agent_rules":{"Disallow":[], "Allow":[]}, "sitemaps_xml": []}
        try:
            async with session.get(url) as response:
                text_split = await response.text()
                for k in text_split.splitlines():
                    if len(k)!=0 and k[0]!="#": # on évite les lignes commentées ou vides
                        if "user-agent:" in k.lower():
                            key=k.split(":", 1)[1].strip(' ')  # la clé va passer de user-agent en user-agent 
                        
                        elif "disallow" in k.lower() and key=="*": #tant que la clé vaut * on ajoute
                            res["user_agent_rules"]["Disallow"].append(k.split(":",1)[1].strip(' '))
                        elif "allow" in k.lower() and key=="*": #tant que la clé vaut * on ajoute
                            res["user_agent_rules"]["Allow"].append(k.split(":",1)[1].strip(' '))
                        elif "sitemap" in k.lower() and "http" in k: #on prend tous les sitemaps
                            res["sitemaps_xml"].append(k.split(":", 1)[1].strip(' '))
        except Exception as e:
            res["Exception"]=str(e)
        finally:
            return res

    async def fetch_all(session, liste_url, fonction):
        tasks = []
        for url in liste_url:
            task = asyncio.create_task(fonction(session, url))
            tasks.append(task)
        res = await tqdm_asyncio.gather(*tasks)
        return res

    async def main(liste_url, fonction, timeout_total):
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
    
    def scan_urls(self, liste_url, fonction, timeout_total):
        start = perf_counter()
        liste_results=asyncio.run(MongoDB_scrap_async.main(liste_url,fonction, timeout_total))
        stop = perf_counter()
        print("\nTemps d'execution de toutes les requêtes : {}".format(stop-start))
        return liste_results

    def scan_robots_txt(self):
        # on récupère l'ensemble des documents dans la BDD MongoDB (leur id, et l'url du robots.txt)
        reponse_mongoDB=self.client["scrapping"]["urls_sitemap_html"].find({}, {"_id":1, "url_robots_txt":1})

        liste_result=list(reponse_mongoDB)

        #n=100
        liste_url=list(map(lambda x:x["url_robots_txt"], liste_result))#liste_result[:n]
        liste_id=list(map(lambda x:x["_id"], liste_result))#liste_result[:n]

        results_robots=self.scan_urls(liste_url, MongoDB_scrap_async.parser_robots, self.timeout_robots)
       
        start = perf_counter()
        for id, element in tqdm(list(zip(liste_id, results_robots))):
            self.client["scrapping"]["urls_sitemap_html"].update_one(
                    {'_id':id},
                    { "$set": 
                                { "user_agent_rules": element["user_agent_rules"],
                                "sitemaps_xml" : element["sitemaps_xml"]}
                    },
                    )
        stop = perf_counter()
        print("\nTemps d'éxecution de toutes les mises à jours : {}".format(stop-start))
        print("Nb sitemaps non vides : {}".format(len(list((filter(lambda x:len(x["sitemaps_xml"])!=0, results_robots))))))
        #self.show_all("scrapping", "urls_sitemap_html", random=False, max_items=n)


if __name__=="__main__":
    instance_Mongo=MongoDB_scrap_async(port_forwarding=27017, test=True)
    #instance_Mongo.scan_robots_txt()
    #pprint(instance_Mongo)
    #pprint(instance_Mongo.show_all("scrapping", "urls_sitemap_html", random=True, max_items=5))

