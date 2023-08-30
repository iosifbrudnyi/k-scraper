import math
import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
import aiohttp 
import asyncio
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

start_time = time.time()
baseurl = "https://www.kimbrer.com"
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36'}

base = requests.get(baseurl, headers=headers).text
base_soup = BeautifulSoup(base, 'html.parser')


async def get_product_block_url_async(session, url, block_id):
    async with session.get(url + f'?p={block_id}', headers=headers) as resp:
        return await resp.text()
    



async def get_product_info_async(session, url):
	async with session.get(url, headers=headers) as resp:
            f = await resp.text()
            hun = BeautifulSoup(f, 'html.parser')

            try:
                part = hun.find("meta", property = "product:retailer_item_id")['content']
            except:
                part = None

            try:
                brand = hun.find("meta", property = "product:brand")['content']
            except:
                brand = None

            try:
                name = hun.find("meta", property = "og:title")['content']
            except:
                name = None

            try:
                price_value = hun.find("meta", property = "product:price:amount")['content']
                price_currency = hun.find("meta", property = "product:price:currency")['content']
                price = price_value + " " + price_currency
            except:
                price = None
            
            try:
                condition = hun.find("meta", property = "product:condition")['content']
            except:
                condition = None

            return {
                "part": part,
                "name": name,
                "brand": brand,
                "price": price,
                "condition": condition
            }


async def get_data_async(session, url):
    data = []

    async with session.get(url, headers=headers) as resp:
        base_page = await resp.text()
        base_page_soup=BeautifulSoup(base_page,'html.parser') 

        pages_info = base_page_soup.find_all("span", {"class" : "toolbar-number"})

        if len(pages_info) != 2:
            pages_num_info = [int( page_info.text ) for page_info in pages_info]
            num_of_pages = math.ceil( (pages_num_info[2] / pages_num_info[1]) )

            pages = []

            for i in range(1, num_of_pages + 1):
                pages.append( await get_product_block_url_async(session=session, url=url, block_id=i) )

            for page in pages:
                soup = BeautifulSoup(page, "html.parser")
                page_products_form = soup.find_all("form",{"class":"item"})
                page_products_div = soup.find_all("div",{"class":"item"})

                for page_product in page_products_form:
                    link = page_product.find("a", {"class": "product"}).get("href")
                    data.append( await get_product_info_async(session, link) )

                    logging.info(link)

                for page_product in page_products_div:
                    link = page_product.find("a", {"class": "product"}).get("href")
                    data.append( await get_product_info_async(session, link) )

                    logging.info(link)

        else:
            soup = BeautifulSoup(base_page, "html.parser")
            page_products_form = soup.find_all("form",{"class":"item"})
            page_products_div = soup.find_all("div",{"class":"item"})

            for page_product in page_products_form:
                link = page_product.find("a", {"class": "product"}).get("href")
                data.append( await get_product_info_async(session, link) )

                logging.info(link)


            for page_product in page_products_div:
                link = page_product.find("a", {"class": "product"}).get("href")
                data.append( await get_product_info_async(session, link) )

                logging.info(link)


async def main():
    connector = aiohttp.TCPConnector(limit=50)
    async with aiohttp.ClientSession(connector=connector) as session:
        product_block_urls = [el['href'] for el in base_soup.select("a.block.text-base.font-semibold.leading-loose")]

        tasks = []

        for product_block_url in product_block_urls:
            tasks.append( asyncio.create_task( get_data_async(session, product_block_url) ))

        data = await asyncio.gather(*tasks)
        df = pd.DataFrame(data)
        df.to_csv("output.csv")
        

asyncio.run(main())
print(f"{(time.time() - start_time):.2f} seconds")
