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
from tqdm.asyncio import tqdm_asyncio

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

class Crawler_parrelel():
    def __init__(self):
        pass
    
    def open_media(self, path):
        self.df_urls=pd.read_csv(path)

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


if __name__=="__main__":
    crawler=Crawler_parrelel()
    breakpoint()
    crawler.open_media("medias_per_countries.csv")
    breakpoint()



