import requests
import bs4
import pandas as pd
from calendar import monthrange
from sqlalchemy import create_engine
import logging


'''
CREATE TABLE `{lang}_scrapper`(
    `ID` INT NOT NULL AUTO_INCREMENT,
    `year` INT NULL DEFAULT NULL,
    `month` TINYTEXT NULL,
    `day` TINYTEXT NULL,
    `field` TINYTEXT NULL,
    `value` TINYTEXT NULL,
    UNIQUE `ID`(`ID`)
) ENGINE = INNODB CHARSET = utf8mb4 COLLATE utf8mb4_general_ci;
'''

COL_YEAR = 'year'
COL_MONTH = 'month'
COL_DAY = 'day'
COL_FIELD = 'field'
COL_VALUE = 'value'

# ======================================================== DATABASE CREDENTIAL =======================================================
SERVER_HOST = 'localhost'
USERNAME = 'root'
PASSWORD = ''
DATABASE = 'tamil'
TABLE_NAME = '{lang}_scrapper'

print (f'Connecting MySQL at `{SERVER_HOST}` with username: `{USERNAME}` AND database: `{DATABASE}` ...', end=' - ')
engine =  create_engine(f'mysql+pymysql://{USERNAME}:{PASSWORD}@{SERVER_HOST}/{DATABASE}')
print ("OK")
# ====================================================================================================================================


LANGUAGES = ['hindi', 'english', 'tamil', 'kannada', 'telugu']
WEBSITE = 'https://{language}panchangam.org/dailypanchangam.php?date={year}-{month:02}-{day:02}'
YEARS = [2021]
MONTHS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]


def format_web(language, year, month, day):
    if language != 'english':
        language = language + "."
    else:
        language = '' 
    return WEBSITE.format(language=language, year=year, month=month, day=day)

def number_of_days_in_month(year, month):
    return monthrange(year, month)[1]
    
def create_list_from_table(soup, tag):
    try:
        table = soup.find('table', attrs={'class': tag})
        table_rows = table.find_all('tr') 
        l = []  
        for tr in table_rows:
            td = tr.find_all('td')
            row = [tr.text for tr in td]
            l.append(row)
        return l
    except:
        return None



def download_url(table, year, month, day, url):
    print (f"Downloading {url}...")
    df = pd.DataFrame(columns=[COL_YEAR, COL_MONTH, COL_DAY, COL_FIELD, COL_VALUE])
    crawl_url = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
    crawl_url.raise_for_status()
    soup = bs4.BeautifulSoup(crawl_url.text, 'lxml')
    list_table = create_list_from_table(soup, 'table')

    for row in list_table:
        if len(row) != 2:
            continue
        
        df = df.append({COL_YEAR : year, COL_MONTH: month, COL_DAY: day, COL_FIELD: row[0], COL_VALUE: row[1]}, ignore_index=True)
    
    df.to_sql(table, engine, index=False, if_exists='append')    
    

for lang in LANGUAGES:
    for year in YEARS:
        for month in MONTHS:
            num_days = number_of_days_in_month(year, month)
            for day in range(1, num_days+1):
                table_name = TABLE_NAME.format(lang=lang)
                url = format_web(lang, year, month, day)        
                print(f"{table_name} :: {year}-{month}-{day}")
                try:
                    download_url(table_name, year, month, day, url)
                except Exception as e:
                    print (f"ERROR: {lang.upper()} - {year}-{month}-{day} -> " + str(e))
                    
                
                
            
    

