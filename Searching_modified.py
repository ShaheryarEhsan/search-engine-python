#Crawler and indexing combined

from urllib.request import urlopen
from html.parser import HTMLParser
from bs4 import BeautifulSoup
import os
import fnmatch
import codecs
import json
from pprint import pprint
import ast
import mmap
import pymysql
from collections import Counter
import time
import re
import nltk
from nltk.corpus import stopwords

conn = pymysql.connect(
	host='127.0.0.1',
	port=3306,
	user='root',
	passwd='',
	db='forward')


IndexesDB = conn.cursor()

stop_words = set(stopwords.words('english')) 





def forward_index(title, desc):
    indexed_words = []
    desc = desc.replace("\\", "")
    desc = desc.replace("\)", "")
    words = desc.split()
    counts = Counter(words)
    for word in counts:
        if word not in indexed_words:
            try:
                IndexesDB.execute("""INSERT INTO forin (title, desc_words, count) VALUES ("%s", "%s", "%d")""" % (title, word, counts[word]))
                indexed_words.append(word)

            except:
                indexed_words.append(word)
                continue
    
    conn.commit()


def inverted_index(url, desc, title):
    indexed_words = []
    words = desc.split()
    url = re.escape(url)
    counts = Counter(words)
    for word in counts:
        if word not in indexed_words:
            try:
                IndexesDB.execute("""INSERT INTO inverted (url, word, title, count) VALUES ("%s", "%s", "%s", "%d")""" % (url, word, title, counts[word]))
                indexed_words.append(word)
            except:
                indexed_words.append(word)
                continue

        conn.commit()


    



def search_keyword(index, word):
    word = word.lower()
    if word in index:
        url_array = index[word]
        for url in url_array:
            if url in index_titles:
                search = index_titles[url]
                for query in search:
                    print(query)
                    print(url)
    else:
        print("Doesn't exist")

def add_title_index(index_titles,url,title):
    title = title.lower()
    if url in index:
          return
    else:
        index_titles[url] = [title]


def add_to_index(index,keyword,url):
    keyword = keyword.lower()
    if keyword in index:
        if url in index[keyword]:
            return
        else:
            index[keyword].append(url)
            return
    else:
        index[keyword] = [url]


def add_url_to_index(index, url, line):
    words = line.split()
    for entry in words:
        add_to_index(index, entry, url)


def retrieve_title(page):
    start = page.find('<title')
    end = page.find('- Simple English Wikipedia, the free encyclopedia</title>')
    start_title = page.find('>', start)
    title= page[start_title+1:end]
    return title


def retrieve_description(page):
    soup = BeautifulSoup(page, 'html.parser')
    return soup.p.getText()
   
    
def get_url(page):
    start_link = page.find('<a href')
    if start_link==-1:
        return None, 0
    start_quote = page.find('"', start_link)
    end_quote = page.find('"', start_quote+1)
    url = page[start_quote+1:end_quote]
    return url, end_quote


def get_page(url):
    file = codecs.open(url, encoding='utf-8')
    return file.read()   
    

def all_links(page):
    links = [] 
    while page:
        url, endpos = get_url(page)
        if url:
            links.append(url)
            page = page[endpos:]
        else:
            break
    return links


def crawler(seed):
    fullname = []
    
    for path,dirs,files in os.walk(seed):
        for f in fnmatch.filter(files,'*.html'):
            fullname.append(os.path.abspath(os.path.join(path,f)))
    for file in fullname:
        
        try:    
            content = get_page(file)
        except:
            continue
        
        to_crawl = [content]
        crawled = []
        while to_crawl:
            page = to_crawl.pop()
            try:
                title = retrieve_title(page)
                
            except:
                continue
            try:
                desc = retrieve_description(page)
            except:
               desc = title+" is a page on Wikipedia Simple."
            
            
            if len(title)>0:
                #add_url_to_index(index, file, title)
                #add_url_to_index(index, file, desc)
                #add_title_index(index_titles, file, title)
                forward_index(title, desc)
                inverted_index(file, desc, title)
                crawled.append(file)
            else:
                continue

    print("Number of documents read", len(crawled))
    
       
    
    return crawled
    


def search_title(query):
    cursor = conn.cursor()
    
    cursor.execute("SELECT distinct(title) from inverted WHERE title = %s", query)
    titles = cursor.fetchall()
    cursor.execute("SELECT distinct(url) from inverted WHERE title = %s", query)
    url_title = cursor.fetchall()
    
    
    return titles, url_title
    


def search_single_word(query): 

    cursor = conn.cursor()
    cursor1 = conn.cursor()
    cursor2 = conn.cursor()
    titles, url_title = search_title(query)
    titles = list(sum(titles, ()))
    url_title = list(sum(url_title, ()))
    cursor.execute("SELECT title from inverted WHERE word = %s", query)
    title_list = cursor.fetchall()
    ti = list(sum(title_list, ()))
    cursor1.execute("SELECT url from inverted WHERE word = %s", query)
    urls = cursor1.fetchall()
    w = list(sum(urls, ()))
    cursor2.execute("SELECT count from inverted WHERE word = %s", query)
    count = cursor2.fetchall()
    c = list(sum(count, ()))
    for i in range(0, len(w)):
        for j in range(0, len(w)-i-1):
            if c[j+1]>c[j]:
                c[j], c[j+1] = c[j+1], c[j]
                w[j], w[j+1] = w[j+1], w[j]
                ti[j], ti[j+1] = ti[j+1], ti[j]


    for i in range(0, len(titles)):
        print(titles[i].upper())
        print(url_title[i])
    if len(w)>10:
        for i in range(0, 10):
            if ti[i] in titles:
                continue
            else:
                print(ti[i].upper())
                print(w[i])
    else:
        for i in range(0, len(w)):
            if ti[i] in titles:
                continue
            else:
                print(ti[i].upper())
                print(w[i])
    







def search_word(query): 

    cursor = conn.cursor()
    cursor1 = conn.cursor()
    cursor2 = conn.cursor()
    titles, url_title = search_title(query)
    titles = list(sum(titles, ()))
    url_title = list(sum(url_title, ()))
    cursor.execute("SELECT title from inverted WHERE word = %s", query)
    title_list = cursor.fetchall()
    ti = list(sum(title_list, ()))
    cursor1.execute("SELECT url from inverted WHERE word = %s", query)
    urls = cursor1.fetchall()
    w = list(sum(urls, ()))
    cursor2.execute("SELECT count from inverted WHERE word = %s", query)
    count = cursor2.fetchall()
    c = list(sum(count, ()))
    for i in range(0, len(w)):
        for j in range(0, len(w)-i-1):
            if c[j+1]>c[j]:
                c[j], c[j+1] = c[j+1], c[j]
                w[j], w[j+1] = w[j+1], w[j]
                ti[j], ti[j+1] = ti[j+1], ti[j]



    
    return titles, url_title, ti, w, c



def multi_word_search(words):
    cursor = conn.cursor()
    cursor1 = conn.cursor()
    cursor2 = conn.cursor()
    titles, url_title = search_title(words)
    titles = list(sum(titles, ()))
    url_title = list(sum(url_title, ()))
    if len(titles)>0:
        for i in range(0, len(titles)):
            print(titles[i].upper())
            print(url_title[i])
    
    cursor.execute("SELECT title from inverted WHERE word = %s", words)
    title_list = cursor.fetchall()
    ti = list(sum(title_list, ()))
    if len(ti)>0:
        cursor1.execute("SELECT url from inverted WHERE word = %s", words)
        urls = cursor1.fetchall()
        w = list(sum(urls, ()))
        cursor2.execute("SELECT count from inverted WHERE word = %s", words)
        count = cursor2.fetchall()
        c = list(sum(count, ()))
        for i in range(0, len(w)):
            for j in range(0, len(w)-i-1):
                if c[j+1]>c[j]:
                    c[j], c[j+1] = c[j+1], c[j]
                    w[j], w[j+1] = w[j+1], w[j]
                    ti[j], ti[j+1] = ti[j+1], ti[j]
        for i in range(0, len(w)):
            if ti[i] in titles:
                continue
            else:
                print(ti[i].upper())
                print(w[i])

    queries = words.split()
    filtered = [w for w in queries if not w in stop_words]
    title_words = []
    title_urls = []
    word_titles = []
    word_urls = []
    index = []
    for word in filtered:
        temp_titles, temp_t_urls, temp_word_titles, temp_word_urls, c = search_word(word)
        for i in range(0, len(temp_titles)):
            title_words.append(temp_titles[i].upper())
            title_urls.append(temp_t_urls[i])
        for i in range(0, len(c)):
            index.append([temp_word_titles[i].upper(),[temp_word_urls[i]]])
    
    for i in range(0, len(title_words)):
        print(title_words[i].upper())
        print(title_urls[i])
    for i in range(0, 7):
        for j in range(0, len(index[i])):
            value = str(index[i][j])
            value = value.strip("['']")
            print(value)
            
            
        
                    

#start = time.time()        
#main_links = crawler('C:/Users/Maha Irfan/Desktop/DSA/Forward and Inverted Index/wikipedia-simple-html/wikipedia-simple-html/simple/articles')
#end = time.time()
#print(end - start, "secs")
#IndexesDB.close()
#conn.close()
#print('Done')

#query = input("Enter keyword to search")
#sql_query = ("Select url from inverted where keyword = query")
#cursor = conn.cursor()
#cursor.execute(query)
#records = cursor.fetchall()

#for row in records:
#    print(row[0])
#cursor.close()





query = input("Enter keyword to search: ")
words = query.split()
if(len(words)>1):
    multi_word_search(query)
else:
    start = time.time()
    search_single_word(query)
    end = time.time()





conn.close()
