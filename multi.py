import math
import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
import aiohttp 
import asyncio
import logging

from tqdm import tqdm
import concurrent.futures


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

start_time = time.time()
baseurl = "https://www.kimbrer.com"
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36'}

base = requests.get(baseurl, headers=headers).text
base_soup = BeautifulSoup(base, 'html.parser')


def get_product_page_url(url, page_id):
    resp = requests.get(url=url + f'?p={page_id}', headers=headers)
    return resp.text
    
    
def get_product_info(url):
        resp = requests.get(url=url, headers=headers)
        f = resp.text
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


def get_data(url):
    data = []
    try:
        resp = requests.get(url=url)
        base_page = resp.text
        base_page_soup=BeautifulSoup(base_page,'html.parser') 

        pages_info = base_page_soup.find_all("span", {"class" : "toolbar-number"})

        if  isinstance(pages_info, list) and len(pages_info) != 2 and len(pages_info) > 0:
            pages_num_info = [int( page_info.text ) for page_info in pages_info]

            print(pages_num_info)

            num_of_pages = math.ceil( (pages_num_info[2] / pages_num_info[1]) )

    
            for i in range(1, num_of_pages + 1):
                page = get_product_page_url(url=url, page_id=i)
                soup = BeautifulSoup(page, "html.parser")
                page_products_form = soup.find_all("form",{"class":"item"})
                page_products_div = soup.find_all("div",{"class":"item"})

                for page_product in page_products_form:
                    link = page_product.find("a", {"class": "product"}).get("href")
                    data.append(link)

                    logging.info(link)

                for page_product in page_products_div:
                    link = page_product.find("a", {"class": "product"}).get("href")
                    data.append(link)

                    logging.info(link)

        else:
            soup = BeautifulSoup(base_page, "html.parser")
            page_products_form = soup.find_all("form",{"class":"item"})
            page_products_div = soup.find_all("div",{"class":"item"})

            for page_product in page_products_form:
                link = page_product.find("a", {"class": "product"}).get("href")
                data.append(link)

                logging.info(link)


            for page_product in page_products_div:
                link = page_product.find("a", {"class": "product"}).get("href")
                data.append(link)

                logging.info(link)
    except Exception as e:
        print(e)
    finally:
        return data if data else []


def main():
    product_block_urls = [el['href'] for el in base_soup.select("a.block.text-base.font-semibold.leading-loose")]
    
    product_info = []

    CONNECTIONS = 10
    with concurrent.futures.ThreadPoolExecutor(max_workers=CONNECTIONS) as executor:
        futures = (executor.submit(get_data, url) for url in product_block_urls)

        for future in tqdm(concurrent.futures.as_completed(futures), total=len(product_block_urls)):
            try:
                data = future.result()
            except Exception as exc:
                print(exc)
            finally:
                product_info.append(data)

    df = pd.DataFrame(product_info)
    df.to_csv("output.csv", index=False)
        
main()
print(f"{(time.time() - start_time):.2f} seconds")
