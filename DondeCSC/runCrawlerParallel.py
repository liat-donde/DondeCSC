import Crawler_APIs.itemCache as itemCache
import Crawler_APIs.crawlerAPI as crawlerAPI
import Crawler_APIs.insertToNeo as insertToNeo
import Crawler_APIs.cacheDBQs as cacheDBQs
import Crawler_APIs.newItemEvent as newItemEvent

from multiprocessing import Process
import time,sys
import argparse
from pymongo import MongoClient
import os
import time

#a hack procedure to redirect log before execution
def redirectLogAndRun(target,logName,*args):
	logErr = logName+"Err"
	sys.stdout = open(logName, 'w')
	sys.stderr = open(logErr, 'w')
	print "args are "+ str(args)
	target(*args);


#parse arguments

parser = argparse.ArgumentParser(description='This script runs in parallel : crawling the web, updating cache, scrape and insert to items, insert to graph')
parser.add_argument('fname', help='file name holding brands names in capital letters, taken from "Crawl_Brands_List.py"')
parser.add_argument("-m", "--method", default="remote",
                    help="choose local/remote DB (mongo). default is remote")
parser.add_argument("-g", "--graph", default="remote",
                    help="choose local/remote/graphene graph. default is remote")
parser.add_argument("-p", "--parallel", default=4,type= int,
                    help="number of parallel threads to pull from Qs. default is 4")
parser.add_argument("-u", "--update", default=False,type= bool,
                    help="should the crawler update old items metadata")
parser.add_argument("-a", "--updateAll", default=False,type= bool,
                    help="update all items including scraping")

args = parser.parse_args()

## show values ##
print ("method: %s" % args.method )
print ("fname: %s" % args.fname )
print ("parallel: %s" % args.parallel )
print ("update: %s" % args.update )
print ("updateAll: %s" % args.updateAll )

# method = "remote"
# method = "local"
fname = args.fname
method = args.method
update = args.update
updateAll = args.updateAll
gMethod = args.graph

import subprocess

#instrall python module
subprocess.call(['python', 'setup.py', 'install'])

processes = int(args.parallel)

process_arr = []

#connect to LOCAL DB:

url = 'mongodb://localhost:27017/'

Connected = False
while not Connected:
	try:
		localDB = MongoClient(url)
		Connected = True
	except:
		sys.stderr.write("ERROR , runCrawlerParallel, couldn't connect to mongoDB\n")
		time.sleep(5) 
		pass

#create OBJECTS:


cacheDBQs = cacheDBQs.cacheDBQs(localDB)

#graph object
graph =  insertToNeo.AddItem(cacheDBQs, gMethod)

#object to handle new items event
newItemEventOb = newItemEvent.newItemEvent(localDB,method)


#cache object
itCache = itemCache.itemCache(graph,newItemEventOb,cacheDBQs,localDB,method,update,updateAll)
itCache.createQs(processes)

#crawler object
ob = crawlerAPI.crawlBrandsScript(itCache)




d= time.strftime("%Y-%m-%d")
logDir = 'logCrawler/'+d
print logDir


if not os.path.exists(logDir):
    os.makedirs(logDir)


print "RUN CRAWLER"
#run crawler
#ob.runCrawl("TOBI")
brands = [line.strip() for line in open(fname)]
for brand in brands :
	if brand == '':
		continue
	p = Process(target=redirectLogAndRun, args=(ob.runCrawl,logDir+'/crawlerOutput'+str(brand),brand,))
	p.start()
	process_arr.append(p)
	#p.join()

time.sleep(5)

for i in xrange(0,processes):
	print "START PULL UPDATE Q"	
	#pull updated items q
	p = Process(target=redirectLogAndRun, args=(itCache.pullUpdateQ,logDir+'/PullUpdate'+str(i),i,))
	#p = Process(target=itCache.pullUpdateQ,args=(i,))

	p.start()
	process_arr.append(p)
	#p.join()

	print "START PULL NEW Q"
	#pull new items q
	p = Process(target=redirectLogAndRun, args=(itCache.pullNewQ,logDir+'/PullNew'+str(i),i,))
	#p = Process(target=itCache.pullNewQ,args=(i,))
	p.start()
	process_arr.append(p)
	#p.join()

print "START PULL GRAPH Q"
#insert items to graph\
p = Process(target=redirectLogAndRun, args=(graph.pullGraphQ,logDir+'/graphPull'))
#p = Process(target=graph.pullGraphQ)
p.start()
process_arr.append(p)
#p.join()

print "START NEW ITEMS EVENT PULL"
#go over new items and initiate events
p = Process(target=redirectLogAndRun, args=(newItemEventOb.pullNewItems,logDir+'/newItemEvent'))
p.start()
process_arr.append(p)



for p in process_arr:
	p.join()
