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
}
"""

class MongoDB_scrap_async():

    def __init__(self, port_forwarding=27017, test=True):
        self.client=MongoClient('localhost', port=port_forwarding)
        self.ping_MongoDB()
        self.show_all("scrapping", "urls_sitemap_html")
        
        if not self.test_collection_in_database_exists("scrapping", "urls_sitemap_html", print_arg=False):
            self.insert_data("scrapping", "urls_sitemap_html", 
                             MongoDB_scrap_async.create_dictio_data_from_csv("medias_per_countries.csv", test=test))
        
        self.show_all("scrapping", "urls_sitemap_html")
    
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
                                                        [self.show_all(database["name"], collection_name, max_items=2) for collection_name in databases[index]["collections"]])
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
        collection_mongo = self.client[mydatabase][collection_name]
        x = collection_mongo.insert_many(data)
    
    def create_dictio_data_from_csv(path, test=False):
        df_urls=pd.read_csv(path).replace(np.NaN, None)
        if test:
            df_urls=df_urls.loc[df_urls.loc[:, "true_country"]=="United Kingdom", :].iloc[:30]

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
                                    "robots_txt_parsed" :  None,
                                    "last_time_scrapped" : None,
                                    "is_responding" : None,
                                    "nb_not_responding" : 0,
                                    "url_robots.txt":"{}robots.txt".format('{}://{}/'.format(urllib.parse.urlparse(row["url"]).scheme,
                                                                                             urllib.parse.urlparse(row["url"]).netloc )),
                                    "url_root":'{}://{}/'.format(urllib.parse.urlparse(row["url"]).scheme,
                                                                urllib.parse.urlparse(row["url"]).netloc )
                                    },
                                    axis=1))
        return list_dictios
    


    def show_all(self, database_name, collection_name, max_items=2):
        if self.test_collection_in_database_exists(database_name, collection_name, print_arg=True):
            cursor=self.client[database_name][collection_name].find({}).limit(max_items)
            print("Nombre d'éléments : {}".format(self.client[database_name][collection_name].count_documents({})))
            

            liste_result  = []
            for resulsts in cursor:
                liste_result.append(resulsts)
                        
            for index, document in enumerate(cursor):
                if index<max_items:
                    pprint(document)
                else:
                    break
            return liste_result
        return None 
    
    async def parser_robots(session, urls):
        try:
            async with session.get(urls[1]) as response:
                all_links=await list(filter(lambda x: "climat" in x.lower(), list(map(lambda x:"".join(x),
                            re.findall(r"(http|ftp|https)(:\/\/[\w_-]+(?:(?:\.[\w_-]+)+))([\w.,@?^=%&:\/~+#-]*[\w@?^=%&\/~+#-])", response.text())))))
                return { urls[0] : all_links}
        except:
            return { urls[0] : None}

    async def fetch_all(session, urls, fonction):
        tasks = []
        for url in urls:
            task = asyncio.create_task(fonction(session, url))
            tasks.append(task)
        res = await tqdm_asyncio.gather(*tasks)
        return res

    async def main(url_series, fonction, timeout_total):
        if timeout_total:
            timeout = aiohttp.ClientTimeout(total=timeout_total)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                results = await MongoDB_scrap_async.fetch_all(session, url_series, fonction)
            await session.close()
            await asyncio.sleep(0.25)
        else:
            async with aiohttp.ClientSession() as session:
                results = await MongoDB_scrap_async.fetch_all(session, url_series, fonction)
            await session.close()
            await asyncio.sleep(0.25)

        return results

    def scan_url(name_folder, liste_url, fonction, timeout_total, n=500):
        MongoDB_scrap_async.ensure_folder(name_folder)

        start = perf_counter()
        if sys.platform == 'win32':
            loop = asyncio.ProactorEventLoop()
            asyncio.set_event_loop(loop)
    
        split_url=[liste_url[i:i + n] for i in range(0, len(liste_url), n)]

        dictio_url={}
        for index, k in enumerate(split_url):
            
            liste_results=asyncio.get_event_loop().run_until_complete(MongoDB_scrap_async.main(k,fonction, timeout_total))
            dictio_url.update(dict(zip(list(map(lambda x:list(x.keys())[0], liste_results)), list(map(lambda x:list(x.values())[0], liste_results)))))

            json_object = json.dumps(dictio_url, indent=4)
            # Writing to sample.json

            with open("{}/url_{}_{}.json".format(name_folder, n,index), "w") as outfile:
                outfile.write(json_object)

        stop = perf_counter()


if __name__=="__main__":
    instance_Mongo=MongoDB_scrap_async(port_forwarding=27017, test=True)
    pprint(instance_Mongo)


