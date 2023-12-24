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
from pprint import pprint
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



class MongoDB_scrap():
    #docker run -d -p 27017:27017 --name m1 mongo
    #source env/bin/activate

    def __init__(self, port_forwarding=27017):
        self.client=MongoClient('localhost', port=port_forwarding)
        self.ping_MongoDB()
        self.show_all("scrapping", "urls_sitemap_html")
        
        #self.insert_data("scrapping", "urls_sitemap_html", MongoDB_scrap.create_dictio_data_from_csv("medias_per_countries.csv"))
        

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
    
    def create_dictio_data_from_csv(path):
        df_urls=pd.read_csv(path).replace(np.NaN, None)
        df_urls=df_urls.loc[df_urls.loc[:, "true_country"]=="United Kingdom", :]

        list_dictios=list(df_urls.apply(lambda row:
                                    {
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
                                    },
                                    axis=1))
        return list_dictios

    def show_all(self, database_name, collection_name, max_items=5):
        if self.test_collection_in_database_exists(database_name, collection_name, print_arg=True):
            request=self.client[database_name][collection_name].find({})
            for index, document in enumerate(request):
                if index<max_items:
                    pprint(document)
                else:
                    break



class Crawler_parrelel():
    def __init__(self):
        pass
    
    def init_MongoDB(self, path):
        self.df_urls=pd.read_csv(path).replace(np.NaN, None)
        self.dictio={}

        self.df_urls=self.df_urls.loc[self.df_urls.loc[:, "true_country"]=="United Kingdom", :]

        self.dictio=list(self.df_urls.apply(lambda row:
                                    {
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
                                    },
                                    axis=1))
        
        self.dictio_test=self.dictio[:20]
        
        
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
                results = await Crawler_parrelel.fetch_all(session, url_series, fonction)
            await session.close()
            await asyncio.sleep(0.25)
        else:
            async with aiohttp.ClientSession() as session:
                results = await Crawler_parrelel.fetch_all(session, url_series, fonction)
            await session.close()
            await asyncio.sleep(0.25)

        return results


    def url_parser(url):
        uri_objet=urllib.parse.urlparse(url)
        uri_schem=uri_objet.scheme
        uri_netloc=uri_objet.netloc
        url='{}://{}/'.format(uri_objet.scheme,uri_objet.netloc )
        url_robot="{}robots.txt".format(url)
        return pd.Series({"uri_objet":uri_objet, "uri_schem":uri_schem, "uri_netloc":uri_netloc, "url":url, "url_robot":url_robot})

    def ensure_folder(name_folder):
        while os.path.exists(name_folder):
            path=input('Le dossier : {} existe (entrée pour écraser/sinon taper nom) : '.format(name_folder))
            if path=="":
                break
            else:
                name_folder=path
        if not os.path.exists(name_folder):
            os.mkdir(name_folder)
        return name_folder

    def scan_url(name_folder, liste_url, fonction, timeout_total, n=500):
        Crawler_parrelel.ensure_folder(name_folder)

        start = perf_counter()
        if sys.platform == 'win32':
            loop = asyncio.ProactorEventLoop()
            asyncio.set_event_loop(loop)
    
        split_url=[liste_url[i:i + n] for i in range(0, len(liste_url), n)]

        dictio_url={}
        for index, k in enumerate(split_url):
            
            liste_results=asyncio.get_event_loop().run_until_complete(Crawler_parrelel.main(k,fonction, timeout_total))
            dictio_url.update(dict(zip(list(map(lambda x:list(x.keys())[0], liste_results)), list(map(lambda x:list(x.values())[0], liste_results)))))

            json_object = json.dumps(dictio_url, indent=4)
            # Writing to sample.json

            with open("{}/url_{}_{}.json".format(name_folder, n,index), "w") as outfile:
                outfile.write(json_object)

        stop = perf_counter()


#print(np.array([1,2]))


if __name__=="__main__":
    instance_Mongo=MongoDB_scrap(port_forwarding=27017)



