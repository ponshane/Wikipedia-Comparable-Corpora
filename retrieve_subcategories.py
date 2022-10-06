import requests
import time
import tqdm
import argparse
from multiprocessing import Pool

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
            tmp.append(page_title)

    # as there is a deletion of a page, this would happen
    else:
        return False, []

    if "continue" in j.keys():
        continue_flag = j["continue"]["gcmcontinue"]
    else:
        continue_flag = False

    return continue_flag, tmp

def visit(category):
    """ this is the function used for crawling the sub-categories 

    Args:
        category (str): the category name of Wikipedia, e.g., Category: Health

    Returns:
        list: list of sub-categories of the category
    """

    rs = []

    URL = "https://en.wikipedia.org/w/api.php"

    PARAMS = {
    "action": "query",
    "generator": "categorymembers",
    "gcmtitle": "{}".format(category),
    "gcmlimit": "max",
    "gcmtype": "subcat",
    "gcmcontinue":"",
    "format": "json"
    }

    # rerquest Wikipedia API with parameters (PARAMS)
    R = requests.get(URL, params=PARAMS)
    # parse the returned json object
    continue_flag, tmp = parse_response(R.json())
    rs += tmp

    # continue_flag is not False when number of returns > 500
    # as this happens, the program will fill the gcontinue into the PARAMS for getting the information of next page
    while(continue_flag):
        PARAMS["gcmcontinue"] = continue_flag
        R = requests.get(URL, params=PARAMS)
        continue_flag, tmp = parse_response(R.json())
        rs += tmp
    
    return rs


if __name__ == "__main__":
    
    # add argparse for being command line programe
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', '--root', type=str, required=True,
                    help='root of category')
    parser.add_argument('-c', '--core', type=int, required=True,
                    help='number of cores')
    parser.add_argument('-d', '--depth', type=int, required=True,
                    help="depth limit")

    args = parser.parse_args()
    root_cat = args.root
    core = args.core
    depth_limit = args.depth

    start = time.time()

    pool = Pool(processes=core)
    current_depth = 0
    flatten = lambda l: [item for sublist in l for item in sublist]

    while current_depth < depth_limit:
        
        if current_depth == 0:
            # append root category into a file
            with open("./data/{}-{}-depth-subcategories-list.csv".format(root_cat,str(depth_limit)), "a") as writeFile:
                writeFile.write("{},{}\n".format(current_depth,root_cat))
            cats = [root_cat]
        
        # find subcategories in parallel way
        cats = list(tqdm.tqdm(pool.imap_unordered(visit, cats), total=len(cats)))
        cats = flatten(cats)
        current_depth +=1
        
        # statistics
        # the number of subcategories retrieved at current_depth
        num_subcategories_of_next_layer = len(cats)
        print("Found {} subcategories in {} layers.".format(num_subcategories_of_next_layer, current_depth))

        # append subcategories into a file
        with open("./data/{}-{}-depth-subcategories-list.csv".format(root_cat,str(depth_limit)), "a") as writeFile:
            for cat in cats:
                writeFile.write("{},{}\n".format(current_depth,cat))

    elapsed_time = time.time() - start
    print(time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))