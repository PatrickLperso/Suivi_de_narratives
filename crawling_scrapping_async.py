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




async def parser_robots(session, url):
    try:
        async with session.get(url) as response:
            html = await response.text()
            all_links=list(filter(lambda x: "climat" in x.lower(), list(map(lambda x:"".join(x),
                        re.findall(r"(http|ftp|https)(:\/\/[\w_-]+(?:(?:\.[\w_-]+)+))([\w.,@?^=%&:\/~+#-]*[\w@?^=%&\/~+#-])", requesting.text)))))
            if len(all_links)==0:
                all_links=None

            xml_filter=[k for k in all_links if "site" in k]
            if len(xml_filter)==0:
                xml_filter=None

            news_filter=[k for k in xml_filter if "news" in k]
            if len(news_filter)==0:
                news_filter=None

            index_filter=[k for k in xml_filter if "index" in k]
            if len(index_filter)==0:
                index_filter=None

            return {url.replace("/robots.txt", ""):
                    {"robots":html,"all_links":all_links, "xml_filter":xml_filter,"news_filter": news_filter, "index_filter":index_filter}}
    except:
        return {url.replace("/robots.txt", ""): 
                    {"robots":None, "all_links":None, "xml_filter":None,"news_filter": None, "index_filter": None}}
    

async def parser_wayback(session, urls):
    try:
        async with session.get(urls[1]) as response:
            data = await response.json()
            await asyncio.sleep(0.1)
            return {urls[0]:data}
    except:
        return {urls[0]:None}


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
            results = await fetch_all(session, url_series, fonction)
        await session.close()
        await asyncio.sleep(0.25)
    else:
        async with aiohttp.ClientSession() as session:
            results = await fetch_all(session, url_series, fonction)
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

def url_waybackmachine(uri_netloc, regex):

    url_waybackmachine="https://web.archive.org/cdx/search/cdx?url={}/".format(uri_netloc)
    url_waybackmachine+="&fl=original,timestamp&matchType=prefix&collapse=urlkey"
    url_waybackmachine+="&filter=mimetype:text/html&filter=statuscode:200"
    url_waybackmachine+="&showSkipCount=true&output=json"
    url_waybackmachine+="&filter=original:{}".format(regex)

    return url_waybackmachine


def url_to_scrap():
    df_2023=pd.read_csv("20231212.export.CSV", header=None, sep="\t")


    df_scheme_netloc=pd.Series(list(df_2023.loc[:, 57].value_counts().index)).apply(lambda url : url_parser(url))
    number_of_times=df_scheme_netloc.loc[:,"url"].rename("nb_times").value_counts()
    df_scheme_netloc=pd.concat([df_scheme_netloc.drop_duplicates(subset=['url']).set_index("url"), number_of_times],axis=1).sort_values("count",ascending=False)

    #regex=r".*(cop.?[1-2][0-9])|(climat.*)|(.*green)|(.*emission)|(.*oil)|(.*CO2)|(.*co2)|(.pollu).*"
    regex=".*(climat.).*"
    df_scheme_netloc.loc[:, "wayback_url"]=df_scheme_netloc.loc[:, "uri_netloc"].apply(lambda uri_netloc: url_waybackmachine(uri_netloc, regex))
    return df_scheme_netloc.reset_index().rename(columns={"index":"url"})

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
    ensure_folder(name_folder)

    start = perf_counter()
    if sys.platform == 'win32':
        loop = asyncio.ProactorEventLoop()
        asyncio.set_event_loop(loop)
 
    split_url=[liste_url[i:i + n] for i in range(0, len(liste_url), n)]

    dictio_url={}
    for index, k in enumerate(split_url):
        
        liste_results=asyncio.get_event_loop().run_until_complete(main(k,fonction, timeout_total))
        dictio_url.update(dict(zip(list(map(lambda x:list(x.keys())[0], liste_results)), list(map(lambda x:list(x.values())[0], liste_results)))))

        json_object = json.dumps(dictio_url, indent=4)
        # Writing to sample.json

        with open("{}/url_{}_{}.json".format(name_folder, n,index), "w") as outfile:
            outfile.write(json_object)

    stop = perf_counter()

df_scheme_netloc=url_to_scrap()
top_url=df_scheme_netloc.loc[:, ["url","wayback_url"]].iloc[:100]
liste_url=list(zip(list(top_url.loc[:, "url"]),list(top_url.loc[:, "wayback_url"] )))

scan_url("url_wayback", liste_url, fonction=parser_wayback,  timeout_total=None, n=10)


