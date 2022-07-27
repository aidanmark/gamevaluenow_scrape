#!/usr/bin/env python
# coding: utf-8

# In[1]:


def pilot(url):
    
    import re
    import requests
    import pandas as pd 
    import numpy as np 
    import time

    from time import sleep
    from bs4 import BeautifulSoup
    import json
    import random

    import snowflake.connector
    from snowflake.connector.pandas_tools import write_pandas
    from snowflake.connector import connect
    import gspread 
    from oauth2client.service_account import ServiceAccountCredentials
    from df2gspread import df2gspread as d2g

    get_ipython().run_line_magic('load_ext', 'autoreload')
    get_ipython().run_line_magic('autoreload', '1')
    get_ipython().run_line_magic('aimport', 'load_single_df')
    
    def search_assets(game_id, platform_id): 
        """
        :param query: product to search
        :type query: callable string

        Fetch data on this asset
        """
        url = f'https://gamevaluenow.com/sold-listings?game_id={game_id}&platform_id={platform_id}&state=cib'

        headers = {
            'accept': 'application/json',
            'accept-encoding': 'utf-8',
            'accept-language': 'en-GB,en;q=0.9',
            'app-platform': 'Iron',
            'referer': 'https://gamevaluenow.com/en-gb',
            'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="102", "Google Chrome";v="102"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.62 Safari/537.36',
            'x-requested-with': 'XMLHttpRequest'
        }

        #returns all historical sales data for a given game
        html = requests.get(url=url, headers=headers)
        time.sleep(random.uniform(3,9))
        output = json.loads(html.text)
        return output['listings']
    
    print("search_assets function run complete")
    
    def get_data(url):
        headers = {'accept': '*/*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-US,en;q=0.9',
            #‘app-platform’: ‘Iron’,
            'origin': 'https:/ebay.com',
            'referer': 'https://www.ebay.com/bin/purchaseHistory?item=403405198730',
            'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="102", "Google Chrome";v="102"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.62 Safari/537.36'#,
            #‘x-requested-with’: ‘XMLHttpRequest’
              }
        r =  requests.get(url,headers=headers, timeout=10)
        print(r.status_code)
        soup = BeautifulSoup(r.text, 'html.parser')
        return soup
    
    print("get_data function run complete")
    
    def parse_data(soup):
        #finds all game companies
        cats = soup.find_all('div',{'class': 'card collapse-header'})

        #returns titles and links to all platforms
        titles = []
        links = []
        for i in range(len(cats)):
            y = cats[i].find_all('a',{'class': ''})
            for system in range(len(y)):
                game = str(y[system])
                title = re.search('>(.+?)<', game)
                title = str(title.group(1))
                titles.append(title)
    
                link = str(y[system])
                link = re.search('/(.+?)"',link)
                link = str(link.group(1))
                links.append(link)

        #searches through data of each platform page
        games = []
        for link in range(len(links)):
            try:
                html_doc = requests.get('https://gamevaluenow.com/'+links[link]).text
                data = re.search(r"window\.items = (.*?);", html_doc).group(1)
                data = json.loads(data)
                games.append(data)
            except:
                pass
        
        #clean data
        games = [j for i in games for j in i]
        games = [{'title':game['title'],'new_price':float(str(game['new_value'].replace(',',""))),'game_id':int(game['game_id']),'platform_id':int(game['platform_id'])} for game in games]
    
        #returns only games where new price >= $1000
        suff_price = []
        for game in games:
            if game['new_price'] >= 1000:
                suff_price.append(game)
        suff_price = [{'game_id':game['game_id'],'platform_id':game['platform_id']} for game in suff_price]
    
        #searches through all sales of each game
        games = [search_assets(game['game_id'],game['platform_id']) for game in suff_price]
        games = [j for i in games for j in i]
    
        #clean data
        for dicts in games:
            dicts['price'] = str(dicts['price'])
            dicts['price'] = dicts['price'].replace(',',"")
            dicts['price'] = float(dicts['price'])
            dicts['vendor'] = dicts['vendor']['name']
    
        #returns data for all sales where price >= $1000
        games = [d for d in games if d['price'] >= 1000]
        pd.set_option('display.max_rows', None)
        games = pd.DataFrame(games)
        games = games[['title','price','vendor','item_number','item_url','end_date']]
    
        return games
    
    print("parse_data function run complete")
    
    all_platforms = get_data(url)
    
    df = parse_data(all_platforms)
    return(df)


# In[2]:


url = 'https://gamevaluenow.com/#allPlatforms'
pilot(url)


# In[ ]:


with open(r"""/Users/aidanmark/Desktop/Dibbs/Data/private_keys/snf_keys.json""") as f:
    raw_keys = json.load(f)

snf_keys=raw_keys[0]


# In[ ]:


df.to_csv('gamevaluenow_scrape.csv')


# In[ ]:


snf_engine = snowflake.connector.connect(user = str(snf_keys['user']),
                                   password = str(snf_keys['password']),
                                  account = str(snf_keys['account']),
                                    database = str(snf_keys['database']),
                                    warehouse = str(snf_keys['warehouse']),
                                    schema = str(snf_keys['schema']))


# In[ ]:


key = "1IMHMhdm6y17tw9rW37icsRfuxaBTlu6cZFGSI6lwkqE"
t_name = "gamevaluenow_scrape"
t_schema = "JPY_CLEAN"
t_db = "PROD_JUPYTER"
engine = snf_engine


# In[ ]:


df = load_single_df.fetch_gsheet(key, t_name)
df


# In[ ]:


load_single_df.frame_to_snowflake(t_name, df, t_schema, t_db,snf_engine)


# In[ ]:




