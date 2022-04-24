# -*- coding: utf-8 -*-
"""
Created on Mon Apr 18 21:24:37 2022

@author: Shihao Zhou
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd




df_list = pd.read_csv('list.csv',encoding='latin1')
headers = {"User-Agent": "Mozilla/5.0 (X11; CrOS x86_64 12871.102.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.141 Safari/537.36"}

def getDetail(url):
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
    address= ''
    companyType = ''
    incorporated = ''
    industryCode = ''
    
    content = soup.find_all("div", attrs = {"id": "content-container"})
    
    address = content[0].find("dl").find_all(text= True)[3].strip()
    
    companyTypelevel = content[0].find_all("dl",{"class":"column-two-thirds"})
    companyType = companyTypelevel[1].find('dd').find(text= True).strip()
    
    incorporated  = content[0].find("dd",{"id":"company-creation-date"}).find(text= True).strip()
    
    industryCode = content[0].find("span",{"id":"sic0"}).find(text= True).strip()
    
    keys = ['address','companyType','incorporated','industryCode']
    values = [address,companyType,incorporated,industryCode]
    dictionary = dict(zip(keys, values))
    sub_df=pd.DataFrame(dictionary,index = [0])

    return  sub_df

df_detail = pd.DataFrame()
#for i in range(len(df_list)): # due to the time limit, we only get the first 700 records instead of all  companies in uk
for i in range(700):
    try:
        df_detail = df_detail.append(getDetail(df_list['link'].iloc[i]))
    except:
        df_detail = df_detail.append(pd.Series(),ignore_index=True) 
df_detail.to_csv('df_detail.csv')  