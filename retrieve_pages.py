from unicodedata import category
import requests
import time
import tqdm
import argparse
import configparser
from multiprocessing import Pool, cpu_count
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

def retrieve_pages(in_tuple):
    """ this function retrieves Wikipedia comparable articles (pages) from a category and has following steps
    (1) find pages from a category
    (2) loop each page from pages
    (3) find its Chinese version
    (4) fetch abstracts for both English and Chinese pages

    Args:
        in_tuple (list): a tuples containing root category, hierarchy, and category
    """

    URL = "https://en.wikipedia.org/w/api.php"
    CHINESE_URL = "https://zh.wikipedia.org/w/api.php"
            
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
        time.sleep(1) # do not send request too frequent

    if len(rs) > 500:
        return category, len(rs)

    # store list of comparable page pairs
    data_list = []

    # process each page
    for i, elem in enumerate(rs):

        # after colltecting 10 pages, the process will sleep for 1 second
        if i != 0 and i % 10 == 0:
            time.sleep(1)

        English_pageid = elem[0]
        English_page_title = elem[1]
            
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

            data_list.append(data)

    # be gentle for server, the process will sleep for 1 second
    time.sleep(1) 
    return category, data_list

if __name__ == "__main__":
    
    # add argparse for being command line programe
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', '--root', type=str, required=True,
                    help='root of category')
    parser.add_argument('-d', '--depth', type=int, required=True,
                    help="depth limit")

    args = parser.parse_args()
    root_cat = args.root
    depth_limit = args.depth
    core = cpu_count() - 1

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

    num_distinct_cats = len(cats)
    print("Got {} distinct categories after reading.".format(num_distinct_cats))
    
    processed_categories_file = "./data/processed-{}-subcategories-list.csv".format(root_cat) # this file records the categories that you have processed
    with open(processed_categories_file, "r") as text:
        content = text.read()
        processed_cats = content.split("\n") # each line is a subcategories
    cats = [cat for cat in cats if cat[2] not in processed_cats] # only remain unprocessed categories
    num_processed_cats = len(processed_cats) - 1 #(it will count the last empty line so -1 to fix the number)
    print("Got {}/{} categories have been processed.".format(num_processed_cats, num_distinct_cats))
    print("{} categories will be processed this time.".format(len(cats)))

    skip_categories_file = "./data/skip-{}-subcategories-list.csv".format(root_cat) # this file records the categories that you have processed

    pool = Pool(processes=core)
    pbar = tqdm.tqdm(total=len(cats))
    target_collection = get_collection_cursor()

    for category, result in pool.imap_unordered(retrieve_pages, cats):
        # if result is int that means the returned category has over 500 pages
        if type(result) == int:
            with open(skip_categories_file, "a") as writeFile:
                writeFile.write("{},{}\n".format(category, result))
        # insert the page pair into database
        else:
            for x in result:
                if target_collection.count_documents({"English_pageid":x["English_pageid"]}) == 0:
                    target_collection.insert_one(x)
        # finish one subcategory
        pbar.update(1)

        with open(processed_categories_file, "a") as writeFile:
            writeFile.write("{}\n".format(category))

    pbar.close()

    elapsed_time = time.time() - start
    print(time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))