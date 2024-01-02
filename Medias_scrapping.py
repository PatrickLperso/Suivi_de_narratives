# -*- coding: utf-8 -*-
"""
Created on Wed Dec 20 02:35:57 2023

@author: patrick
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import urllib3
urllib3.disable_warnings()

def url_pays():
    url = "https://www.abyznewslinks.com/allco.htm"
    requesting = requests.get(url,  verify=False)
    soup  = BeautifulSoup(requesting.content, "html.parser")
    df_url=pd.DataFrame(list(map(lambda x:[x.get_text(), "https://www.abyznewslinks.com/"+x["href"]],
                       soup.select("body div:nth-child(7) table:nth-child(1) tr:nth-child(1) td font:nth-child(1) a"))), columns=["pays", "url"])
    df_url.loc[:, "true_country"]=df_url.loc[:, "pays"]
    df_url_special=[]
    for k in ['Australia', 'Brazil', 'Canada', 'Germany', 'India','United Kingdom', 'United States']:
        requesting = requests.get(df_url.set_index("pays").loc[k]["url"],  verify=False)
        soup  = BeautifulSoup(requesting.content, "html.parser")
        df_intermediaire=pd.DataFrame(list(map(lambda x:[x.get_text(), "https://www.abyznewslinks.com/"+x["href"]],
                           soup.select("body div:nth-child(7) table:nth-child(1) tr:nth-child(1) td font:nth-child(1) a"))), columns=["pays", "url"])
        
        df_intermediaire.loc[:, "true_country"]=k
        df_url_special.append(df_intermediaire)

    df_url=df_url.loc[~df_url.true_country.isin(['Australia', 'Brazil', 'Canada', 'Germany', 'India','United Kingdom', 'United States'])]
    df_url_special.append(df_url)
    
    return pd.concat(df_url_special, axis=0)

def locations_func(soup):
    locations=[k for k in list(map(lambda x:x, 
             soup.select("body div table tr:nth-child(1) td:nth-child(1) b:nth-child(1) font:nth-child(1)")[1:]
            ))]
    if len(locations)==0: # il y a deux schémas possibles 
        locations=[k for k in list(map(lambda x:x, 
                 soup.select("body div table tr:nth-child(1) td:nth-child(1) h3:nth-child(1) font:nth-child(1)")[1:]
                ))]
        
    return locations



def parse_tableau(liste_liste,locations):
    
    try:
        global_text=list(map(lambda x:x.get_text(), locations))[0]
        locations_text=list(map(lambda x:x.get_text(), locations))[1:]
        indice_local=[k for k in range(len(locations_text)) if " - Local" in locations_text[k]][0]
        success=True
    except:
        success=False
        
    liste_pandas=[]
    for index,liste in enumerate(liste_liste):
        try:
           if len(liste)==5:
               media_coverage=[k for k in list(map(lambda x:str(x).strip(), list(liste[0]))) if k!='<br/>']
               media_name=list(map(lambda x:x.get_text().strip(), liste[1].select("a")))
               media_type=[k for k in list(map(lambda x:str(x).strip(), list(liste[2]))) if k!='<br/>']
               media_subject=[k for k in list(map(lambda x:str(x).strip(), list(liste[3]))) if k!='<br/>']
               media_language=[k for k in list(map(lambda x:str(x).strip(), list(liste[4]))) if k!='<br/>']
               media_diffusion=[""]*len(media_language)
           else:
               media_coverage=[k for k in list(map(lambda x:str(x).strip(), list(liste[0]))) if k!='<br/>']
               media_name=list(map(lambda x:x.get_text().strip(), liste[1].select("a")))
               media_type=[k for k in list(map(lambda x:str(x).strip(), list(liste[2]))) if k!='<br/>']
               media_subject=[k for k in list(map(lambda x:str(x).strip(), list(liste[3]))) if k!='<br/>']
               media_language=[k for k in list(map(lambda x:str(x).strip(), list(liste[4]))) if k!='<br/>']
               media_diffusion=[k for k in list(map(lambda x:str(x).strip(), list(liste[5]))) if k!='<br/>']
               if len(media_diffusion)!=len(media_type):
                  media_diffusion.extend([""]*(len(media_type)-len(media_diffusion)))
           url=[k["href"] for k in list(map(lambda x:x, liste[1])) if k.get_text() in media_name]
           
           if success:
               if index<indice_local:
                   media_location=[locations_text[index]]*len(media_type)
                   coverage=[global_text]*len(media_type)
               else:
                   media_location=[locations_text[index+1]]*len(media_type)
                   coverage=[locations_text[indice_local]]*len(media_type)
           else:
               media_location=[""]*len(media_type)
               coverage=[""]*len(media_type)
           liste_pandas.append(pd.DataFrame(data=list(zip(media_coverage,url,media_name,media_type,media_subject,media_language, media_diffusion,media_location,coverage)),
                              columns=["media_coverage","url","media_name","media_type","media_subject","media_language", "media_diffusion","media_location","coverage"]))
               
               
        except:
            print(index)
    return liste_pandas



def country(df_pays_url):
    list_fail=[]
    list_data=[]
    for k in range(len(df_pays_url)):
        print(df_pays_url.iloc[k]["pays"])
        try:
            requesting = requests.get(df_pays_url.iloc[k]["url"],  verify=False)
            soup  = BeautifulSoup(requesting.content, "html.parser")
            locations=locations_func(soup)
            scrap_pages=[k for k in list(map(lambda x:x.select("td font"),soup.select("body div table tr:nth-child(1)")[5:-1])) if k[0] not in locations]
            data=pd.concat(parse_tableau(scrap_pages, locations), axis=0).reset_index(drop=True)
            data.loc[:, "true_country"]=df_pays_url.iloc[k]["true_country"]
            list_data.append(data)
        except:
            print("fail {}".format(df_pays_url.iloc[k]["pays"]))
            list_fail.append(df_pays_url.iloc[k]["pays"])
    
    return list_fail,list_data


df_pays=url_pays()



list_fail,list_data=country(df_pays)
df_total=pd.concat(list_data, axis=0)
df_total.to_csv("data/medias_per_coutries.csv")

