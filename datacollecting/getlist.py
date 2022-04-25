# -*- coding: utf-8 -*-
"""
Created on Mon Apr 18 20:11:24 2022

@author: Shihao Zhou
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd


# setup the first url and header
headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36"}
url = 'https://find-and-update.company-information.service.gov.uk/alphabetical-search/get-results?companyName=%5C&searchAfter=%3A02559441'

df_namelist = pd.DataFrame()
df_namelist_old= pd.DataFrame()

def getCompanylist(url):
    #try 3 times first 
    try:
        html = requests.get(url, headers=headers,timeout=10)
    except:    
        print('fail1')
        try:
            html = requests.get(url, headers=headers,timeout=10)
        except:
            print('fail2')
            try:
                html = requests.get(url, headers=headers,timeout=10)
            except:
                print('fail3')
    soup = BeautifulSoup(html.text, 'lxml')
    
    companyName = []
    companyNumber = []
    status = []
    link = []
    
    
    tableContent = soup.find_all("td", {"class": "govuk-table__cell"})
    
# this is for getting the url for next page
    nextpage=soup.find_all('div', {'class': "pagination"})
    nexpageUrl = nextpage[0].find('a',{"id":"nextLink"})
    urltext= "https://find-and-update.company-information.service.gov.uk/alphabetical-search/"+str( nexpageUrl.get('href'))
# here we need to get the len/3 since one set contains 3 values
    for i in range(int(len(tableContent)/3)):
        indexNumber = i*3
        companyName.append(tableContent[indexNumber].find(text=True)) 
        number = tableContent[indexNumber+1].find(text=True)
        companyNumber.append(number)
        status.append(tableContent[indexNumber+2].find(text=True))
        link.append("https://find-and-update.company-information.service.gov.uk/company/"+str(number)) 

    sub_df = pd.DataFrame({'companyName': companyName, 'companyNumber': companyNumber,'status': status,'link': link })
    
    return sub_df,urltext



for i in range(100):# only get the first 100 pages. if we need all we can do len(df)
    try:
        df_namelist_new,url = getCompanylist(url)
    except:    
        print('fail to run function1')
        try:
            df_namelist_new,url = getCompanylist(url)
        except:
            print('fail to run function2')
            try:
                df_namelist_new,url = getCompanylist(url)
            except:
                print('fail to run function3')
                pass
    
    if ~df_namelist_old.equals(df_namelist_new):  
        df_namelist_old = df_namelist_new.copy()
        df_namelist = df_namelist.append(df_namelist_new)  
    else:
        break
    
    
df_namelist.to_csv('list.csv')