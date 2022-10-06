import requests
import time
import sys
import glob
import math
import re
import tqdm
import argparse
import configparser
from multiprocessing import Pool
from pymongo import MongoClient

def get_collection_cursor():
    """ this function is called in each process
    Referece: https://api.mongodb.com/python/current/faq.html#using-pymongo-with-multiprocessing

    Returns:
        pymongo.MongoClient.collection: a connection object
    """
    client = MongoClient(uri)
    return client[MongoDB][MongoCollection]

def parse_response(j):
    """ this is the function for parsing the returned json from Wikipedia API

    Args:
        j (json): json object

    Returns:
        continue_flag (str): False or continue flag
        tmp (list): list of return objects
    """
    tmp = []

    if "query" in j.keys():
        for _, page in j["query"]["pages"].items():
            page_title = page["title"]
            pageId = page["pageid"] 
            tmp.append((pageId, page_title))

    # as there is a deletion of a page, this would happen
    else:
        return False, []

    if "continue" in j.keys():
        continue_flag = j["continue"]["gcmcontinue"]
    else:
        continue_flag = False

    return continue_flag, tmp

def retrieve_pages(list_of_categories):
    """ this function retrieves Wikipedia comparable articles (pages) from a list of categories and has following steps
    (1) find pages from a category
    (2) loop each page from pages
    (3) when there doesn't exist an English page in database
    (4) find its Chinese version
    (5) fetch abstracts for both English and Chinese pages

    Args:
        list_of_categories (list): a list of tuples, where each containing root category, hierarchy, and category
    """
    
    target_collection = get_collection_cursor()

    URL = "https://en.wikipedia.org/w/api.php"
    CHINESE_URL = "https://zh.wikipedia.org/w/api.php"
    
    for in_tuple in list_of_categories:
        
        root_cat, hierarchy, category = in_tuple

        PARAMS = {
            "action": "query",
            "generator": "categorymembers",
            "gcmtitle": "{}".format(category),
            "gcmlimit": "max",
            "gcmtype": "page",
            "gcmcontinue":"",
            "format": "json"
        }

        # store list of tuple(pageid, pagetitle)
        rs = []

        # 0 find the pages based on the category
        R = requests.get(URL, params=PARAMS)
        continue_flag, tmp = parse_response(R.json())
        rs += tmp

        # continue_flag is not False when number of returns > 500
        # as this happens, the program will fill the gcontinue into the PARAMS for getting the information of next page
        while(continue_flag):
            PARAMS["gcmcontinue"] = continue_flag
            R = requests.get(URL, params=PARAMS)
            continue_flag, tmp = parse_response(R.json())
            rs += tmp
        
        # process each page
        for elem in rs:
            English_pageid = elem[0]
            English_page_title = elem[1]
            
            # 1 only process English_pageid which doesn't exist in MongoDB
            if target_collection.count_documents({"English_pageid":English_pageid}) == 0:
                
                LANGLINK_PARAMS = {
                    "action":"query",
                    "prop":"langlinks",
                    "pageids":English_pageid,
                    "lllang":"zh",
                    "format":"json"
                }

                # find its Chinese article
                R = requests.get(URL, params=LANGLINK_PARAMS)
                R_json = R.json()
                # if there is a interlink (Chinese article) of this English_pageid
                if "langlinks" in R_json["query"]["pages"][str(English_pageid)]:
                    Chinese_page_title = R_json["query"]["pages"][str(English_pageid)]["langlinks"][0]["*"]
                
                    # 3 retrieve abstract of both english page and chinese page
                    ENGLISH_EXTRACT_PARAMS = {
                        "action":"query",
                        "prop":"extracts",
                        "pageids":English_pageid,
                        "format":"json",
                        "explaintext":"true",
                        "exintro":"true"
                        # "exlimit":1
                    }

                    R = requests.get(URL, params=ENGLISH_EXTRACT_PARAMS)
                    R_json = R.json()
                    extracted_English_abstract = R_json["query"]["pages"][str(English_pageid)]["extract"]

                    CHINESE_EXTRACT_PARAMS = {
                        "action":"query",
                        "prop":"extracts",
                        "titles":Chinese_page_title,
                        "format":"json",
                        "explaintext":"true",
                        "exintro":"true"
                        # "exlimit":1
                    }

                    R = requests.get(CHINESE_URL, params=CHINESE_EXTRACT_PARAMS)
                    R_json = R.json()
    
                    # we yet have Chinese pageid (key for accessing the item), so we must use for to retrieve item
                    for _, item in R_json["query"]["pages"].items():
                        if item.get("pageid"):
                            Chinese_pageid = item["pageid"]
                            extracted_Chinese_abstract = item["extract"]
                        else:
                            Chinese_pageid = ""
                            extracted_Chinese_abstract = ""
                        break # only process the first item

                    # 4 store in mongo
                    # define data template for mongo
                    data = {
                        "root_category": root_cat,
                        "category": category,
                        "category_hierarchy": hierarchy,
                        "English_pageid": English_pageid,
                        "English_page_title": English_page_title,
                        "extracted_English_abstract":extracted_English_abstract,
                        "Chinese_pageid": Chinese_pageid,
                        "Chinese_page_title": Chinese_page_title,
                        "extracted_Chinese_abstract": extracted_Chinese_abstract
                    }

                    target_collection.insert_one(data)
        
def chunk(sequence, chunk_size):
    """ this helps create pymongo connection each chunk for improving efficiency
    rather than create pymongo connection for each post

    Args:
        sequence (list): list of objects
        chunk_size (int): size of chunk

    Yields:
        list: a list with a determined chunk size

    Reference: https://medium.com/@saksham.malhotra2196/the-efficient-way-of-using-multiprocessing-with-pymongo-a7f1cf72b5b7
    Reference: https://stackoverflow.com/questions/41104582/pymongo-multiprocessing
    
    """
    for j in range(0, len(sequence), chunk_size):
        yield sequence[j:j + chunk_size]

if __name__ == "__main__":
    
    # add argparse for being command line programe
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', '--root', type=str, required=True,
                    help='root of category')
    parser.add_argument('-c', '--core', type=int, required=True,
                    help='number of cores')
    parser.add_argument('-cs', '--chunk_size', type=int, required=True,
                    help='size of each chunk')
    parser.add_argument('-d', '--depth', type=int, required=True,
                    help="depth limit")

    args = parser.parse_args()
    root_cat = args.root
    core = args.core
    chunk_size = args.chunk_size
    depth_limit = args.depth

    # init and read config file - connect to mongodb
    config = configparser.ConfigParser()
    config.read('./config.ini')
    # connect to mongo
    MongoServer = config["Mongo"]["URI"]
    MongoDB = config["Mongo"]["Database"]
    MongoUser = config["Mongo"]["User"]
    MongoPW = config["Mongo"]["PW"]
    MongoCollection = config["Mongo"]["Collection"]
    
    uri = "mongodb://" + MongoUser + ":" + MongoPW + "@" + MongoServer + "/?authSource=" +\
    MongoDB + "&authMechanism=SCRAM-SHA-1"

    start = time.time()

    cats = list()
    with open("./data/{}-{}-depth-subcategories-list.csv".format(root_cat,str(depth_limit)), "r") as text:
        content = text.read()
        in_cats = content.split("\n") # each line is a subcategories
    seen_cats = set() # record seen categories
    for cat in in_cats:

        if cat == "": #the last line
            continue

        hier_name = cat.split(",") # 0 for hierarchy index, 1 for category name
        hier = hier_name[0]
        name = hier_name[1]
        if name not in seen_cats: # skip duplicated category for efficiency
            cats.append((root_cat, hier, name))
            seen_cats.add(name)

    print("Got {} distinct categories after reading.".format(len(cats)))

    pool = Pool(processes=core)
    for _ in tqdm.tqdm(pool.imap_unordered(retrieve_pages, chunk(cats, chunk_size)),
     total=math.ceil(len(cats)/chunk_size)):
        pass # use for loop to trigger the tqdm

    elapsed_time = time.time() - start
    print(time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))