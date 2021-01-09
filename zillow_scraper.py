import requests
from bs4 import BeautifulSoup

import sqlite3
from datetime import date, datetime
import time

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.16; rv:84.0) Gecko/20100101 Firefox/84.0'
}

DB_PATH = '/var/www/html/zillow_price_tracker/db.sqlite'

def insert_date():
    db = sqlite3.connect(DB_PATH)
    cursor = db.cursor()
    cursor.execute("INSERT OR IGNORE INTO dates(date) VALUES('" + str(date.today()) + "')")
    db.commit()
    db.close()

def insert_prices(insert_map):
    db = sqlite3.connect(DB_PATH)
    cursor = db.cursor()

    for key, value in insert_map.items():
        cursor.execute("INSERT INTO prices(price, house_id, scraped_date) SELECT '" + value + "' AS price, houses.id, dates.date FROM houses, dates WHERE houses.id=" + str(key) + " AND dates.date='" + str(date.today()) + "'")

    db.commit()
    db.close()

def scrape_first_time():
    db = sqlite3.connect(DB_PATH)
    cursor = db.cursor()

    insert_map = {}
    failed_urls = []

    try:
        for row in cursor.execute('SELECT * FROM houses'):
            time.sleep(60 * 10 - row[0])

            res = requests.get(row[2], headers=headers)
            soup = BeautifulSoup(res.content, 'html.parser')
            home_details = soup.find('div', {'class': 'ds-home-details-chip'})
            if home_details:
                price = home_details.find('span', {'class': 'einFCw'})
                if price:
                    # insert_map { id(integer): price(string) }
                    insert_map[row[0]] = price.getText()
            else:
                failed_urls.append({'id': row[0], 'url': row[2]})
    except requests.ConnectionError:
        db.close()
        print('connection error: ' + str(datetime.now()))
        return {'error': True, 'failed_urls': []}
    except requests.exceptions.MissingSchema as err:
        print(err)
        pass

    db.close()
    insert_prices(insert_map)

    return {'error': False, 'failed_urls': failed_urls}


def scrape_failed_urls(failed_urls):
    retry_count = 0
    max_retry_count = 3
    insert_map = {}

    try:
        while retry_count < max_retry_count and len(failed_urls) != 0:
            time.sleep(60 * 60)
            for url in failed_urls:
                time.sleep(60 * 20)
                res = requests.get(url['url'], headers=headers)
                soup = BeautifulSoup(res.content, 'html.parser')
                home_details = soup.find('div', {'class': 'ds-home-details-chip'})
                if home_details:
                    price = home_details.find('span', {'class': 'einFCw'})
                    if price:
                        insert_map[url['id']] = price.getText()
                        failed_urls.remove(url)
            retry_count += 1

    except requests.ConnectionError:
        print('connection error: ' + str(datetime.now()))
        insert_prices(insert_map)
        return {'error': True, 'failed_urls': failed_urls}

    except requests.exceptions.MissingSchema as err:
        print(err)
        pass

    insert_prices(insert_map)

    return {'error': False, 'failed_urls': failed_urls}


def zillow_scraper():
    hour = 60 * 60
    scrape_interval = 24 * hour
    failed_interval = 3 * hour

    while True:
        start_time = time.time()

        insert_date()

        #  first scraping --------------------------------------
        scraping_info = scrape_first_time()
        connection_error = scraping_info['error']
        failed_urls = scraping_info['failed_urls']

        if connection_error:
            delay_time = hour * 2
            time.sleep(delay_time)

            scraping_info = scrape_first_time()
            connection_error = scraping_info['error']
            failed_urls = scraping_info['failed_urls']
        #  -----------------------------------------------------

        time.sleep(failed_interval)

        # second scraping --------------------------------------
        scraping_info2 = scrape_failed_urls(failed_urls)
        connection_error = scraping_info2['error']
        remained_urls = scraping_info2['failed_urls']

        if connection_error:
            delay_time = hour * 2
            time.sleep(delay_time)

            remained_urls = scrape_failed_urls(failed_urls)['failed_urls']


        scraped_time = time.time() - start_time
        print('Scraped: ' + str(datetime.now()))
        print("Failed URL(s): " + str(remained_urls))

        if scrape_interval - round(scraped_time) > 0:
            time.sleep(scrape_interval - round(scraped_time))

zillow_scraper()

