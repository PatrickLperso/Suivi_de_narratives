# -*- coding: utf-8 -*-
"""
Created on Tue Dec 26 20:47:02 2023

@author: paulr
"""

import asyncio
from parsel import Selector
import httpx
import nest_asyncio
import time

async def fetch_sitemap_data(client, url):
    async with httpx.AsyncClient(timeout=30) as client:

        try:
            response = await client.get(url)
            selector = Selector(response.text)
            urls = selector.xpath('//url/loc/text()').getall()
            return urls
        except Exception as e:
            print(f"Error fetching {url}: {e}")
            return []

async def main(main_url="https://www.9news.com.au/sitemap.xml"):
    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.get(main_url)
        selector = Selector(response.text)
        sitemap_urls = selector.xpath("//sitemap/loc/text()").getall()

        tasks = []
        for url in sitemap_urls:
            tasks.append(fetch_sitemap_data(client, url))

        results = await asyncio.gather(*tasks)
        flattened_results = [item for sublist in results for item in sublist]
        return flattened_results




if __name__ == "__main__":
    start_time = time.time()
    
    nest_asyncio.apply()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        data = loop.run_until_complete(main())
    finally:
        loop.close()
    time_difference = time.time() - start_time
    print(f'Scraping time: %.2f seconds.' % time_difference, 'Data length :',len(data))
