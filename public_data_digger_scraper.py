#!/usr/bin/env python3
r"""
  ____
 / ___|  ___ _ __ __ _ _ __   ___ _ __
 \___ \ / __| '__/ _` | '_ \ / _ \ '__|
  ___) | (__| | | (_| | |_) |  __/ |
 |____/ \___|_|  \__,_| .__/ \___|_|
                      |_|

This program is designed to run on the Linux cluster
Before starting another scraping process check if there are already running processes:
    ps axu | grep public_data_digger_scraper

To scrape search pages with the names from the data file run this command:
    nohup ./public_data_digger_scaper.py search &

To scrape detail pages for the collected search results run this command:
    nohup ./public_data_digger_scaper.py details &

To collect data from the raw JSON files and update the search results table run this command:
    ./public_data_digger_scaper.py update

"""
# Version 2.1, 2022-07-10, add meta data to load_search_results()
# Version 2, 2022-07-10, code clean-up with pylint
# Version 1, 2022-07-09


from typing import List, Any, Dict, Tuple
import os
import sys
import time
import signal
import logging
import random
import json
import re
import datetime
from urllib.parse import urlparse
import requests
from bs4 import BeautifulSoup

import pandas as pd


from dateparser.search import search_dates
from dateparser import parse as parse_dates

INPUT_DATA_DIR = "/data/project/voter_registration_scraping/input"

SEARCH_URL = "https://publicdatadigger.com/search"
MAX_RESULTS_ON_SEARCH_PAGE = 10000

SCRAPING_SEARCH_DIR = "/data/project/voter_registration_scraping/output/search"
SCRAPING_DETAILS_DIR = "/data/project/voter_registration_scraping/output/details"

LOG_DIR = "/data/project/voter_registration_scraping/logs"
if __name__ == '__main__' and not os.path.isdir(LOG_DIR):
    os.makedirs(LOG_DIR)

DATA_DIR = "/data/project/voter_registration_scraping/data"



#   ____                      _
#  / ___|  ___  __ _ _ __ ___| |__
#  \___ \ / _ \/ _` | '__/ __| '_ \
#   ___) |  __/ (_| | | | (__| | | |
#  |____/ \___|\__,_|_|  \___|_| |_|


def complete_url_with_names(fname: str, lname: str) -> str:
    """Creates search URL for given names

    Args:
        fname (str): First name
        lname (str): Last name

    Returns:
        str: Search URL
    """
    return urlparse(SEARCH_URL) \
        ._replace(query=f"q={fname.strip().upper()}+{lname.strip().upper()}").geturl()


def complete_url_with_anchor(anchor: Any) -> str:
    """Produces absolute URL from anchor object with relative HREF

    Args:
        anchor (Any): Anchor object

    Returns:
        str: Absolute URL
    """
    return urlparse(SEARCH_URL)._replace(path=anchor.attrs.get('href'), query='').geturl()


def next_text_only(elm: Any) -> str:
    """Extracts text from next object

    Args:
        elm (Any): DOM object

    Returns:
        str: Stripped text
    """
    next_elm = elm.find_next()
    if next_elm:
        return next_elm.text.strip()
    else:
        return None


def parent_text_with_nl(elm: Any, delimiter='\n') -> str:
    """Converts parent object into text string, preserves line breaks
    with given delimiter.

    Args:
        elm (Any): DOM object
        delimiter (str, optional): String used to replace line breaks. Defaults to '\n'.

    Returns:
        str: Text value of parent object
    """
    elm_par = elm.parent
    for line_break in elm_par.findAll('br'):
        line_break.replaceWith(delimiter)
    return elm_par.get_text().strip()


def text_with_nl(elm: Any, delimiter='\n') -> str:
    """Converts DOM object into text string, preserves line breaks
    with given delimiter.

    Args:
        elm (Any): DOM object
        delimiter (str, optional): String used to replace line breaks. Defaults to '\n'.

    Returns:
        str: Text value of input object
    """
    for line_break in elm.findAll('br'):
        line_break.replaceWith(delimiter)
    return elm.get_text().strip()



def datadigger_by_name(fname: str, lname: str,
    line_delimiter='\n', date_format='%Y-%m-%d') -> Tuple[str, List]:
    """Load and parse search page for given first and last name

    Args:
        fname (str): First name of person
        lname (str): Last name of person
        line_delimiter (str, optional): Delimiter for line-breaks within a data valaue.
                                        Defaults to '\n'.
        date_format (str, optional): Formatting string for datetime objects. Defaults to '%Y-%m-%d'.


    Returns:
        Tuple[str, List]: Raw HTML from search page, List of parsed data
    """
    query_url = urlparse(SEARCH_URL) \
        ._replace(query=f"q={fname.strip().upper()}+{lname.strip().upper()}").geturl()
    logging.info("Query: %s", query_url)
    page = requests.get(query_url)
    results = []

    if page.status_code==200:
        soup = BeautifulSoup(page.content, 'html.parser')
        for j, result_body in enumerate(soup.find_all(attrs={'class': 'result-body'})):
            if j > MAX_RESULTS_ON_SEARCH_PAGE:
                logging.error("Too many results on search page. Number exceeds %d",
                              MAX_RESULTS_ON_SEARCH_PAGE)
                break
            res = {}
            detail_a = result_body.find_previous_sibling('h4').find('a')
            res['detail_url'] = complete_url_with_anchor(detail_a)
            res['detail_url_text'] = detail_a.text.strip()

            res['display_text'] = line_delimiter.join(result_body.stripped_strings)

            if (find_home := result_body.find(attrs={'class': 'fa-home'})):
                res['home_address'] = parent_text_with_nl(find_home, delimiter=line_delimiter)

            if (find_email := result_body.find(attrs={'class': 'fa-envelope'})):
                res['email_address'] = next_text_only(find_email)

            if (find_phone := result_body.find(attrs={'class': 'fa-phone'})):
                res['phone_number'] = next_text_only(find_phone)

            for rsb in result_body.children:
                if rsb.name:
                    txt = rsb.text.strip()
                    if 'born' in txt.lower():
                        srd = search_dates(txt)
                        if len(srd)>0:
                            res['birth_date'] = search_dates(txt)[0][1].strftime(date_format)
                            res['birth_date_text'] = search_dates(txt)[0][0]
                    elif 'updated' in txt.lower():
                        srd = search_dates(txt)
                        if len(srd)>0:
                            res['updated_date'] = search_dates(txt)[0][1].strftime(date_format)
                            res['updated_date_text'] = search_dates(txt)[0][0]

            results.append(res)

    return page.text, results



def scrape_by_name(fname, lname, sleep_time = (1, 3), line_delimiter='\n', date_format='%Y-%m-%d'):
    """Scrape search page for given first and last name. There can be multiple records per page.
    For each request two files are created:
    html: contains the raw web-page
    json: contains a JSON record of the parsed data

    If the files already exist no new request is made.

    Args:
        fname (str): First name of person
        lname (str): Last name of person
        sleep_time (tuple, optional): Number of seconds to sleep between requests is randomly picked
                                      from this interval. Defaults to (3, 17).
        line_delimiter (str, optional): Delimiter for line-breaks within a data valaue.
                                        Defaults to '\n'.
        date_format (str, optional): Formatting string for datetime objects. Defaults to '%Y-%m-%d'.
    """
    output_fname = re.sub(r'[\W]', '_', fname.lower().strip())
    output_lname = re.sub(r'[\W]', '_', lname.lower().strip())
    output_path = os.path.join(SCRAPING_SEARCH_DIR, 'json', output_lname)
    output_name = os.path.join(output_path, f"{output_fname}.json")

    html_path = os.path.join(SCRAPING_SEARCH_DIR, 'html', output_lname)
    html_name = os.path.join(html_path, f"{output_fname}.html")
    logging.info("File: %s", output_name)

    if os.path.isfile(html_name):
        logging.info("Skip %s %s, file %s already exists.", fname, lname, html_name)
        return

    for pth in [output_path, html_path]:
        if not os.path.isdir(pth):
            os.makedirs(pth)

    time.sleep(random.randint(*sleep_time))

    html, results = datadigger_by_name(fname, lname,
                                        line_delimiter=line_delimiter, date_format=date_format)
    with open(html_name, "w", encoding="utf8") as html_io:
        html_io.write(html)

    if len(results)>0:
        logging.info("Number or records: %d", len(results))
        with open(output_name, "w", encoding="utf8") as json_io:
            for j, res in enumerate(results):
                res['order_number'] = j
                res['first_name'] = fname
                res['last_name'] = lname
                res['json_file'] = output_name
                res['scrape_date'] = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
                json_io.write(json.dumps(res))
                json_io.write('\n')
    else:
        logging.warning("No records for %s %s", fname, lname)


#   ____       _        _ _
#  |  _ \  ___| |_ __ _(_) |___
#  | | | |/ _ \ __/ _` | | / __|
#  | |_| |  __/ || (_| | | \__ \
#  |____/ \___|\__\__,_|_|_|___/


def clean_label(label: str) -> str:
    """Removes and replaces special characters to form 'clean' label strings

    Args:
        label (str): Text to be converted to label

    Returns:
        str: String to be used as key in dict
    """
    l_1 = label.getText().strip().replace(':', '')
    l_2 = re.sub(r'[\W]', '_', l_1)
    l_3 = re.sub(r'[_+]', '_', l_2)
    return l_3.lower()


def datadigger_detail_page(detail_url: str,
                           line_delimiter='\n', date_format='%Y-%m-%d') -> Tuple[str, Dict]:
    """Request and parse detail page for given URL.

    Args:
        detail_url (str): URL of detail page, from search table
        line_delimiter (str, optional): Delimiter for line-breaks within a data valaue.
                                        Defaults to '\n'.
        date_format (str, optional): Formatting string for datetime objects. Defaults to '%Y-%m-%d'.

    Returns:
        Tuple[str, Dict]: Raw HTML code, structure of parsed data
    """

    page = requests.get(detail_url)
    attributes = {}

    if page.status_code==200:
        soup = BeautifulSoup(page.content, 'html.parser')

        ### Profile
        attributes['profile'] = {}
        profile_header = list(filter(lambda x: 'Additional Information' in x.text,
                                        soup.find_all(attrs={'class':'profile-header'})))[0]
        for label in profile_header.find_next('div') \
                                   .find_all(attrs={'class': 'profile-info-label'}):
            key = clean_label(label)
            value = text_with_nl(label.find_next(attrs={'class': 'profile-info-value'}))
            if key in ['born'] and (prsdt := parse_dates(value)):
                value = prsdt.strftime(date_format)
            attributes['profile'][key] = value

        ### Voter Registration Details
        attributes['voter_registrations'] = []
        for container_header in \
                    list(filter(lambda x: 'Voter Registration' in x.text,
                                soup.find_all(attrs={'class':'page-container-header'}))):
            vr_date_text, vr_date = search_dates(container_header.getText())[0]

            container_body = container_header \
                .find_next('div', attrs={'class': 'page-container-body'})
            vr_attr = {}

            for label in container_body.find_all(attrs={'class': 'profile-info-label'}):
                key = clean_label(label)
                value = text_with_nl(label.find_next(attrs={'class': 'profile-info-value'}),
                                     delimiter=line_delimiter)
                if key in ['birthdate', 'registration'] and (prsdt := parse_dates(value)):
                    value = prsdt.strftime(date_format)

                vr_attr[key] = value

            attributes['voter_registrations'].append({
                'voter_registration_date': vr_date.strftime(date_format),
                'voter_registration_date_text': vr_date_text,
                'voter_registration_attributes': vr_attr
            })
    return page.text, attributes


def scrape_details(detail_url: str,
                   sleep_time = (1, 3), line_delimiter='\n', date_format='%Y-%m-%d'):
    """Scrape details page for given URL. Parse data. For each request two files are created:
    html: contains the raw web-page
    json: contains a JSON record of the parsed data

    If the files already exist no new request is made.

    Args:
        detail_url (str): URL of details page, from search table
        sleep_time (tuple, optional): Number of seconds to sleep between requests is randomly picked
                                      from this interval. Defaults to (3, 17).
        line_delimiter (str, optional): Delimiter for line-breaks within a data valaue.
                                        Defaults to '\n'.
        date_format (str, optional): Formatting string for datetime objects. Defaults to '%Y-%m-%d'.
    """

    path_parts = urlparse(detail_url).path.split('/')
    pname = os.path.join('/'.join(path_parts[1:3]), f"{'_'.join(path_parts[3:])}")
    output_name = os.path.join(SCRAPING_DETAILS_DIR, 'json', f"{pname}.json")
    output_path = os.path.dirname(output_name)

    html_name = os.path.join(SCRAPING_DETAILS_DIR, 'html', f"{pname}.html")
    html_path = os.path.dirname(html_name)
    logging.info("File: %s", output_name)

    if os.path.isfile(html_name):
        logging.info("Skip %s, file %s already exists.", detail_url, html_name)
        return

    for pth in [output_path, html_path]:
        if not os.path.isdir(pth):
            os.makedirs(pth)

    time.sleep(random.randint(*sleep_time))

    html, results = datadigger_detail_page(detail_url,
                                           line_delimiter=line_delimiter, date_format=date_format)
    with open(html_name, "w", encoding="utf8") as html_io:
        html_io.write(html)

    if results:
        with open(output_name, "w", encoding="utf8") as json_io:
            results['detail_url'] = detail_url
            results['json_file'] = output_name
            results['scrape_date'] = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
            json_io.write(json.dumps(results))
            json_io.write('\n')
    else:
        logging.warning("No records for %s", detail_url)


#   ____        _
#  |  _ \  __ _| |_ __ _
#  | | | |/ _` | __/ _` |
#  | |_| | (_| | || (_| |
#  |____/ \__,_|\__\__,_|


def load_search_results() -> pd.DataFrame:
    """Collect records from original JSON files and creates table with search results

    Returns:
        pd.DataFrame: Table with search results
    """
    data = []
    num_files = 0
    for last in os.listdir(os.path.join(SCRAPING_SEARCH_DIR, 'json')):
        for first in os.listdir(os.path.join(SCRAPING_SEARCH_DIR, 'json', last)):
            data_file = os.path.join(SCRAPING_SEARCH_DIR, 'json', last, first)
            if data_file.endswith('.json'):
                num_files += 1
                data.extend(map(json.loads, open(data_file, encoding="utf-8").readlines()))

    search_df = pd.DataFrame(data)
    search_df.attrs.update({
        'data_source': SCRAPING_SEARCH_DIR,
        'number_of_files': num_files,
        'number_of_records': search_df.shape[0],
        'summary_text': f"Number of files: {num_files:,}\n" \
                        f"Number of records: {search_df.shape[0]:,}"
    })
    return search_df


#   __  __       _
#  |  \/  | __ _(_)_ __
#  | |\/| |/ _` | | '_ \
#  | |  | | (_| | | | | |
#  |_|  |_|\__,_|_|_| |_|


def check_for_termination():
    """Allows for controlled termination.
    """
    if os.path.isfile('TERMINATE.NOW'):
        logging.critical("Exit: termination on request")
        exit()
    return

class GracefulTerminate:
    """Capture SIGINT and SIGTERM signals to gracefully end scraping process
    https://stackoverflow.com/questions/18499497/how-to-process-sigterm-signal-gracefully
    """
    args = []
    terminate_now = False

    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, *args):
        """Set termination flag
        """
        self.args = args
        self.terminate_now = True


def main_search():
    """Scrape search pages for names given in input table
    """

    logging.info('Started')
    names_df = pd.read_csv(os.path.join(INPUT_DATA_DIR, "final_race.csv")) \
                 .dropna(subset=['f_name', 'l_name'])
    logging.info("Number of records: %d", names_df.shape[0])
    terminate = GracefulTerminate()

    for j, row in names_df.iterrows():
        if terminate.terminate_now:
            break
        #         if j>5: break

        l_name = row['l_name'].strip() if isinstance(row['l_name'], str) else ''
        f_name = row['f_name'].strip() if isinstance(row['f_name'], str) else ''

        if len(l_name)>0 and len(f_name)>0:
            try:
                scrape_by_name(f_name, l_name)
            except Exception as ex:
                logging.error("Exception: %s", str(ex))
        else:
            logging.warning("Invalid record: '%s' '%s'", f_name, l_name)
    logging.info('Finished')


def main_details(url_list_file: str):
    """Scrape details page for details_url in search table
    """
    logging.basicConfig(filename=os.path.join(LOG_DIR, 'details.log'), level=logging.INFO)
    logging.info('Started')
    # details_df = load_search_results().dropna(subset=['detail_url'])
    details_list = list(filter(lambda u: len(u)>0, map(str.strip, open(url_list_file).readlines())))
    #logging.info("Number of records: %d", details_df.shape[0])
    logging.info("Number of records: %d", len(details_list))
    terminate = GracefulTerminate()

    # for j, row in details_df.iterrows():
    for j, detail_url in enumerate(details_list):
        if terminate.terminate_now:
            break
        # if j>100: break

#         detail_url = row['detail_url']
        if len(detail_url)>0:
            try:
                scrape_details(detail_url)
            except Exception as ex:
                logging.error("Exception: %s", ex)
        else:
            logging.warning("Invalid record: '%s'", str(row['display_text']))
    logging.info('Finished')


def main_process_data():
    """Process raw JSON files and produce data-tables in CSV format
    """
    logging.basicConfig(filename=os.path.join(LOG_DIR, 'process.log'), level=logging.INFO)

    logging.info('Started')
    if __name__ == '__main__' and not os.path.isdir(DATA_DIR):
        os.makedirs(DATA_DIR)

    search_df = load_search_results()
    logging.info("Number of search records: %d", search_df.shape[0])
    summary_text = search_df.attrs.get('summary_text')
    if summary_text:
        print(summary_text)
    else:
        print(f"Number of search records: {search_df.shape[0]:,}")

    data_file = os.path.join(DATA_DIR, 'search_results.csv')
    search_df.to_csv(data_file, index=None)
    logging.info('Finished')
    print(f"Updated data are ready in {data_file}")



if __name__ == '__main__':
    logging.basicConfig(filename=os.path.join(LOG_DIR, 'search.log'), level=logging.INFO)

    if 'search' in sys.argv:
        main_search()
        print("Done.")

    elif 'details' in sys.argv:
        pos = sys.argv.index('details')+1
        if len(sys.argv)>pos and len(sys.argv[pos])>0:
            list_file = sys.argv[pos]
            print(f"Loading from: {list_file}")
            main_details(list_file)
            print("Done.")
        else:
            print(f"Usage: {os.path.basename(__file__)} details LIST_OF_URLS\n")

    elif 'update' in sys.argv:
        main_process_data()

    else:
        print(f"Usage: {os.path.basename(__file__)} search|update\n"
              f"       {os.path.basename(__file__)} details LIST_OF_URLS\n")
