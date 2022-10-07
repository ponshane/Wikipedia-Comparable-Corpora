# Collect Wikipedia Comparable Corpora
This repository provides scripts for collecting English-Chinese comparable corpora from a Wikipedia category.

The scripts is tested on `python 3.7.6`, and depedencies are
```
tqdm 4.43.0
requests 2.23.0
pymongo 3.10.1
```

Preparing the `config.ini` for storing the connection information for MongoDB as following format:
```
[Mongo]
URI = XXX.XXX.XXX.XXX:XXX
Database = DATABASE_NAME
Collection = COLLECTION
User = USERNAME
PW = PASSWORD
```
# Usage
First step is to collect a list of sub-categories of a Wikipedia category 
```
usage: retrieve_subcategories.py [-h] -r ROOT -c CORE -d DEPTH

optional arguments:
  -h, --help              show this help message and exit
  -r ROOT, --root ROOT    root of category
  -c CORE, --core CORE    number of cores
  -d DEPTH, --depth DEPTH depth limit

Example:
python retrieve_subcategories.py -c 2 -r "Category:Computer science" -d 5
```
Second step is to collect pages for the subcategories collected in the previous step
```
usage: retrieve_pages.py [-h] -r ROOT -d DEPTH

optional arguments:
  -h, --help              show this help message and exit
  -r ROOT, --root ROOT    root of category
  -d DEPTH, --depth DEPTH depth limit

Example:
python retrieve_pages.py -r "Category:Computer science" -d 5
```
Note that `retrive_pages.py` uses almost all your cpu cores (number of cores -1) to collect pages in parallel way. `retrieve_pages.py` also logs the processed subcategories (those subcategories have been traveled), helping one rerun the program if it accidently stops.