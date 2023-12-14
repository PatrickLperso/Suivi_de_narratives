# -*- coding: utf-8 -*-
"""
Created on Wed Dec 13 21:12:08 2023

@author: patrick
"""

import requests 
import urllib.parse
import pandas as pd
import urllib.robotparser


def parser_root(url):
    parsed_uri = urllib.parse.urlparse(url)
    return '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
    

def parser_robots(rp, url):
    rp = urllib.robotparser.RobotFileParser()
    rp.set_url("{}robots.txt".format(url))
    rp.read()
    

df_2023=pd.read_csv("20231212.export.CSV", header=None, sep="\t")

url_series=df_2023.iloc[:, 57].apply(lambda url : parser_root(url))
liste_url=list(map(lambda x:"{}robots.txt".format(x), 
                   list(url_series.value_counts().index
                        )))

"""Trouver un moyen de scrapper les sitesmaps et stocker les autorisations des robots.txt"""