"""Created on Thu Mar 16 12:06:06 2023
@author: Santosh Gupta"""
 
# from cx_Oracle import DatabaseError   
from urllib.request import urlopen
from xml.etree.ElementTree import parse
import maya
from datetime import datetime
import time
# from flask_cors import CORS
# import requests
from newspaper import Article
from textwrap import wrap
import re as rx
import regex as re 
import flask
import cx_Oracle as co
import pandas as pd
# from tqdm import tqdm as tq
from tqdm.notebook import tqdm_notebook as tq

# for i in tq(range(20),desc='News Scrapping'):
#     time.sleep(0.5)
tq.pandas()
filename = 'ora_db.properties'
fileobj = open(filename)
param = {}
for line in fileobj:
    line = line.strip()
    if not line.startswith('#'):
        key_value = line.split('=')
        if len(key_value) == 2: 
            # print(key_value)
            param[key_value[0].strip()] = key_value[1].strip()
            
ora_srvr    = param['ora_srvr']
ora_port    = param['ora_port']
ora_service = param['ora_service']
ora_usr     = param['ora_usr']
ora_pwd     = param['ora_pwd']
        
def news_scraping():
    app = flask.Flask(__name__)
    # cors = CORS(app)
    app.config['CORS_HEADERS'] = 'Content-Type'
     
    def Find(string):  
        url = rx.search("(?P<url>https?://[^\s]+)", string).group("url")
        return [x[0] for x in url]
    
    def listToDate(s):     
        # initialize an empty string 
        str1 = "" 
        str2 = ""
        # traverse in the string  
        for ele1 in s[0:3]:            
            str1 += str(ele1)+'-'        
        for ele2 in s[3:]: 
            str2 += str(ele2)+':'        
        date_str = str1[0:-1] +' ' +str2[0:-1]
        # print(date_str)       
        return date_str  
    
    # def get_all_news():
    start_now = datetime.now()
    st_time = start_now.strftime("%H:%M:%S")
        
    try:
        dsn_tns = co.makedsn(ora_srvr, ora_port, service_name=ora_service) 
        conn    = co.connect(ora_usr, ora_pwd, dsn=dsn_tns)
        if conn:                
            cursor = conn.cursor()
            sql = """Select BNSS_RSS_URL from (
            SELECT BNSS_SEQ_ID, BNSS_RSS_URL 
            FROM boi_news_source_setup 
            order by 1)"""
            cursor.execute(sql)
            result = cursor.fetchall()
            
            title = []
            link = []
            description = []
            pubDate =[]
            website = []
            processed_pubDate = []
            time_zone=[]
            xmldoc_lst = []
            url_list=[]
            errurl_list=[]
            failed_url_cnt = 0
            full_article1 = []
            full_article2 = []
            # full_article3 = []
            # full_article4 = []
            
            for i in result:                   
                try:
                   var_url = urlopen(i[0])    
                   xmldoc = parse(var_url)
                   xmldoc_lst.append(xmldoc)
                   url_list.append(i)
                   # print("Fetching the Data from ",i)
                except:               
                    errurl_list.append(i[0])
                    failed_url_cnt +=1
            for i in xmldoc_lst:       
                for item in i.iterfind('channel/item'):
                    title.append(item.findtext('title').replace("&#039;", "'")) 
                    try: 
                        dt = maya.parse(item.findtext('pubDate')).datetime()
                        if len(str(dt.year)) < 4:
                            nw_year = '20'+str(dt.year)                            
                        else:
                            nw_year = str(dt.year)
                        if len(str(dt.month)) < 2:
                            nw_month = '0'+str(dt.month)
                        else:
                            nw_month = str(dt.month)
                        if len(str(dt.day)) < 2:
                            nw_day = '0'+str(dt.day)
                        else:
                            nw_day =str(dt.day)
                        # print(nw_day)    
                        if len(str(dt.hour)) < 2:
                            nw_hour = '0'+str(dt.hour)
                        else:
                            nw_hour =str(dt.hour)
                        # print(nw_hour)
                        if len(str(dt.minute)) < 2:
                            nw_minute = '0'+str(dt.minute)
                        else:
                            nw_minute =str(dt.minute)
                        # print(nw_minute)
                        if len(str(dt.second)) < 2:
                            nw_second = '0'+str(dt.second)
                        else:
                            nw_second =str(dt.second)
                        # print(nw_second)    
                        nw_dt = [nw_year,nw_month,nw_day, nw_hour,nw_minute,nw_second]
                        # print(nw_dt)
                        nw_dt_processed = listToDate(nw_dt)
                        time_zone.append(dt.tzinfo)
                        processed_pubDate.append(nw_dt_processed)
                        pubDate.append(item.findtext('pubDate'))
                    except Exception as e:
                        print('date format->',dt,'<-',str(e))
                    
                    link.append ( item.findtext('link'))
                    url = item.findtext('link')
                    article = Article(url)
                    article = Article(item.findtext('link'))                    
                    try:
                        article.download()
                        article.parse()
                        # print('Length of the article is',len(article.text))
                        if len(article.text)>4000 and len(article.text) <= 8000:
                            full_article1.append(article.text[:4000])
                            full_article2.append(article.text[4001:])
                        elif len(article.text)<4000:
                            full_article1.append(article.text[:4000])
                            full_article2.append('Data length less than 4000')
                            
                        item_desc = item.findtext('description')
                        if item_desc == None:
                            item_desc = item.findtext('description')
                        else:
                            description.append(item.findtext('description').replace("&#039;", "'"))
                    except Exception :
                        print('Article downloading!!!')
              
                for x in i.iterfind('channel'):
                    website.append(x.findtext('link'))
                boi_news_article_df = pd.DataFrame(list(zip(title,link,description,pubDate,processed_pubDate,full_article1,full_article2)),
                                                   columns =['title','link','description','pubDate','processed_pubdate'
                                                             ,'full_article1','full_article2']) 
                
        
            boi_news_article_df = boi_news_article_df.drop_duplicates(ignore_index=True) 
            boi_news_article_df = boi_news_article_df.drop_duplicates(subset='title', keep='first',ignore_index=True)
            boi_news_article_df['time_zone'] = 'UTC'
            timestr = time.strftime("%Y%m%d_%I%M%S%p")
            extension = ".csv"
            file_name = 'scraped_news'+timestr +  extension #                              
            boi_news_article_df.to_csv(file_name, index=False)
            end_now = datetime.now()
        
            ed_time = end_now.strftime("%H:%M:%S") 
            try:
                dsn_tns = co.makedsn(ora_srvr, ora_port, service_name=ora_service) 
                conn    = co.connect(ora_usr, ora_pwd, dsn=dsn_tns)                
                if conn: 
                    try:
                        cursor = conn.cursor()                            
                        for index, row in boi_news_article_df.iterrows():
                            # if len(row[5])>4000:
                            #     print(row[5])
                            sql = "INSERT INTO BOI_NEWS_ARTICLE(BNA_NWS_TITLE,BNA_NWS_LINK, BNA_NWS_DESCRIPTION ,BNA_NWS_PUBDATE,BNA_NWS_PROCESSED_PUBDATE,BNA_NWS_FULL_ARTICLE1,BNA_NWS_FULL_ARTICLE2,BNA_NWS_TIME_ZONE) VALUES(:1,:2,:3,:4,:5,:6,:7,:8)"
                            cursor.execute(sql, tuple(row))       
                            
                    except Exception as e:
                        print(str(e)) 
                    conn.commit()
                    print("Record inserted succesfully") 
            except Exception as e:
                err, = e.args
                print("Oracle-Error-Code:", err.code)
                print("Oracle-Error-Message:", err.message)
            finally:
                cursor.close()
                conn.close()
            
            print ('Web scraping started at', st_time,'and completed at',ed_time)                    
    except Exception as e:
        print(str(e))
        
    return 

news_scraping()