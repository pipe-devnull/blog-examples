#!/usr/bin/python

# Multi purpose downloader script
# 
# - requests library for HTTP operations
# - standard library's queue library 
# - standard library's threads library

import os
import Queue
import threading
import sys
import getopt
import requests
import json
import time

#Downloader class - reads queue and downloads each file in succession
class Downloader(threading.Thread):
    """Threaded File Downloader"""

    def __init__(self, queue, output_directory):
            threading.Thread.__init__(self,name= os.urandom(16).encode('hex'))
            self.queue = queue
            self.output_directory = output_directory

    def run(self):
        while True:
            # gets the url from the queue
            url = self.queue.get()

            # download the file
            print "* Thread " + self.name + " - processing URL"
            self.download_file(url)

            # send a signal to the queue that the job is done
            self.queue.task_done()

    def download_file(self, url):
        t_start = time.clock()

        r = requests.get(url)
        if (r.status_code == requests.codes.ok):
            t_elapsed = time.clock() - t_start
            print "* Thread: " + self.name + " Downloaded " + url + " in " + str(t_elapsed) + " seconds"
            fname = self.output_directory + "/" + os.path.basename(url)

            with open(fname, "wb") as f:
                f.write(r.content)
        else:
            print "* Thread: " + self.name + " Bad URL: " + url


# Spawns dowloader threads and manages URL downloads queue
class DownloadManager():

    def __init__(self, download_dict, output_directory, thread_count=5):
        self.thread_count = thread_count
        self.download_dict = download_dict
        self.output_directory = output_directory

    # Start the downloader threads, fill the queue with the URLs and
    # then feed the threads URLs via the queue
    def begin_downloads(self):
        queue = Queue.Queue()

        # Create a thread pool and give them a queue
        for i in range(self.thread_count):
            t = Downloader(queue, self.output_directory)
            t.setDaemon(True)
            t.start()

        # Load the queue from the download dict
        for linkname in self.download_dict:
            #print uri
            queue.put(self.download_dict[linkname])

        # Wait for the queue to finish
        queue.join()

        return


# Main.  Parse CLIoptions, prepare download list & start downloading
def main(argv):
    inputfile = None
    flist = None
    help = 'pydownload.py ./output_directory/ -i <JSONinputfile>  -f <url1,url2,url3...>'
    try:
        opts, args = getopt.getopt(argv, "hi:f:", ["ifile=", "flist="])
    except getopt.GetoptError:
        print help
        sys.exit(2)

    # Check for required script argument output dir
    if len(args) > 0:
        output_directory = args[0]
    else:
        print help
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            print help
            sys.exit(2)
        elif opt in ("-i", "--ifile"):
            inputfile = arg
        elif opt in ("-f", "--flist"):
            flist = [i for i in arg.split(',')]

    print '----------pydownnload---------------'
    print '------------------------------------'
    print 'JSON file:             ', inputfile
    print 'Output Directory:      ', output_directory
    print 'File list:             ', flist
    print '------------------------------------'

    # Now build a dict of urls to download, just add any flist urls
    download_dict = {}

    # If the input file is supplied then parse it as JSON and add to dict of URLS
    if (inputfile is not None):
        fp = open(inputfile)
        url_list = json.load(fp)
        for url in url_list:
            download_dict[url['link_name']] = url['link_address']

    # Add in any additional files contained in the flist variable
    if (flist is not None):
        for f in flist:
            download_dict[str(f)] = f

    # If there are no URLs to download then exit now, nothing to do!
    if len(download_dict) is 0:
        print "* No URLs to download, got the usage right?"
        print "USAGE: " + help
        sys.exit(2)

    download_manager = DownloadManager(download_dict, output_directory, 5)
    download_manager.begin_downloads()


# Kick off
if __name__ == "__main__":
    main(sys.argv[1:])
