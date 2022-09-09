#!/usr/bin/python3.8

import os
import sys
import time
import random
import json
import re
import requests
from bs4 import BeautifulSoup
import datetime

##
## *Usage*
##
## Run as
##        $ python3 ./scrape_kickstarter.py
##
## In order to download live and succeeded project run
##        $ python3 ./scrape_kickstarter.py --any
##
## For testing only download 100 projects, run
##        $ python3 ./scrape_kickstarter.py --test
##

##
## Customization: make changes here!
##
data_dir = '/data/project/kickstarter_indiegogo2021'   # path on local file system
hdfs_dir = '/data/project/kickstarter_indiegogo2021'   # path on HDFS when running on Spark cluster

use_hdfs = True                                        # set to *False* if running locally 

categories_to_scrape = ['design', 'fashion', 'food', 'technology'] # set to *None* if you want to scrape everything. Categories must be from the ones in cat_dict


##
## Nothing to edit beyond this point
##

cat_dict = {
    "art": 1, 
    "comics": 3, 
    "crafts": 26, 
    "dance": 6, 
    "design": 7, 
    "fashion": 9, 
    "film_video": 11, 
    "food": 10, 
    "games": 12, 
    "journalism": 13, 
    "music": 14, 
    "photography": 15, 
    "publishing": 18, 
    "technology": 16, 
    "theater": 17, 
}


web_scrape_datetime = datetime.datetime.utcnow()
web_scrape_timestamp = int(web_scrape_datetime.timestamp())
web_scrape_isodate = web_scrape_datetime.isoformat()
web_scrape_path = web_scrape_datetime.strftime("%Y/%m%d/%H%M")


def info(msg):
    now_local = datetime.datetime.now().isoformat
    now_utc   = datetime.datetime.utcnow().isoformat
    print(f"INFO: {now_utc()} [UTC] {now_local()} [local]\t\t{msg}")
    
        
def clean_category_string(s):
    s0 = s.lower()
    s1 = re.sub(r'[^a-z0-9]+', '_', s0)
    return s1
            
        
        
def scraping(sections=None, anyState=True, limit=0):
    
    def get_pages_yield_soup(base_url):
        
        page_number = 1
        page = requests.get(f"{base_url}&page={page_number}")

        while page.status_code == 200:
            info(f"Page Number: {page_number:,}")
            soup = BeautifulSoup(page.content, 'html.parser')
            yield soup
            page_number += 1
            random_sleep_time = 1.7+90*random.random()
            time.sleep(random_sleep_time)
            page = requests.get(f"{base_url}&page={page_number}")

    
    if sections is None:
        sections = cat_dict.keys()
    
    project_counter = 0
    
    if any_state:
        stloop = [('any','')]
    else:
        stloop = [ (s, f'state={s}&') for s in ['live', 'upcoming'] ]
        
    for sect in sections:
        info(f"Start section `{sect}`")
        category_id = cat_dict.get(sect, -1)
        if category_id<0:
            info(f"Unknown category '{sect}'")
            continue
            
        for state, state_arg in stloop:
            base_url = f"https://www.kickstarter.com/discover/advanced?{state_arg}category_id={category_id}&sort=newest&seed=2731239"
            # https://www.kickstarter.com/discover/advanced?state=upcoming&category_id=16&sort=newest&seed=2731828&page=1

            for soup in get_pages_yield_soup(base_url):
                projects = soup.find_all(attrs={'class': 'js-react-proj-card'})
                for k, p in enumerate(projects):
                    pid = p.get('data-pid')

                    data = json.loads(p.get('data-project'))

                    project_counter += 1
                    info(f"Project ID: {pid}, ({project_counter:,})")

                    category = clean_category_string(data['category']['slug'])
                    dest_dir = os.path.join(data_dir, web_scrape_path, category, f"{pid}")
                    res = os.system(f"mkdir -p {dest_dir}")
                    assert res==0, F"Failed to create `{dest_dir}`"

                    data['web_scrape_timestamp'] = web_scrape_timestamp
                    data['web_scrape_isodate'] = web_scrape_isodate

                    with open(os.path.join(dest_dir, 'data.json'), 'w') as io:
                        json.dump(data, io)

                    if (limit>0) and (project_counter>limit):
                        return project_counter
    
    return project_counter
                    
    
def hdfs_upload():
    ld = os.path.join(data_dir, web_scrape_path)
    hd = os.path.join(hdfs_dir, web_scrape_path)
    info(f"Upload to hdfs://{hd}")
    os.system(f"hdfs dfs -mkdir -p {hd}")
    os.system(f"hdfs dfs -put {ld}/* {hd}")
        

if __name__ == '__main__':
    any_state = '--any' in sys.argv
    limit = 100 if '--test' in sys.argv else 0
    
    n = scraping(anyState = any_state, limit = limit, sections = categories_to_scrape )
    info(f"Total number of projects scraped: {n:,}")
    if use_hdfs:
        hdfs_upload()
        
    info ("Done!")
    
