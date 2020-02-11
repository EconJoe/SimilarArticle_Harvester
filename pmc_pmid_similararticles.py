import requests
from bs4 import BeautifulSoup
import time
import pandas as pd
import numpy as np
import os.path
from os import path
from datetime import datetime

#directory = '/disk/bulkw/staudt/RAWDATA/PMC_Live/'
#directory = 'B:/Research/RAWDATA/PMC_Live/'
directory = 'B:/Research/RAWDATA/PMC_Live_2/'

version = '2020-02-04'

# Load in full list of articles available on PubMed Central (PMC)
#   These were downloaded on 6/26/2019 as ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/PMC-ids.csv.gz
all_pmids = pd.read_csv(directory + 'PMC-ids_' + version + '.csv', dtype={'PMID': object})
all_pmids = all_pmids[['PMID']].drop_duplicates(keep = 'first')

# Randomly sort and then split data into 100 chunks (arrays)
all_pmids = all_pmids.sample(frac=1, random_state=1234).reset_index(drop=True)
all_pmids_split = np.array_split(all_pmids, 500)

# This program requests the URL, and if it fails to get it, waits 5 seconds, and then tries again.
def get(url):
    try:
        time_stamp = str(datetime.now())
        req = session.get(url, headers=headers)
        request_status_code = str(req.status_code)
        print(request_status_code)
        soup = BeautifulSoup(req.text, 'xml')
        req.close()
        if soup.eLinkResult.LinkSet.DbFrom.text is not None:
            print('OK')
            return soup, request_status_code, time_stamp
    except Exception:
        # sleep for a bit in case that helps
        time.sleep(5)
        print(request_status_code)
        print('Exception')
        # try again
        return get(url)

for split in range(0, 1):
    
    # If this is the first time running the script, just use all PMCIDs as the list to be harvested.
    iter_pmids = all_pmids_split[split]
    iter_pmids.set_index('PMID', inplace=True)

    # Open output files in write mode
    outfile = open(directory + 'PMC_PMID_NeighborScore_' + version + '_' + str(split) + '.txt', "w+")

    outfile.write('dbfrom' +'\t'+ 'pmid_focal' +'\t'+ 'dbto' +'\t'+ 'linkname' +'\t'+ 'pmid' +'\t'+ 'score' +'\t'+ 'time_stamp' +'\t'+ 'status_code' + '\n')

    count_tot = iter_pmids.shape[0]
    counter = 0
    for pmid in iter_pmids.index:

        counter = counter + 1
        print(str(counter) + ' of ' + str(count_tot))

        session = requests.Session()
        headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36"}

        pmid = str(pmid)
        pmid = pmid.rstrip()
        #url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pmc&version=2.0&api_key=2cef50fe68b4d02b710a25436e6315634708&id=" + str(pmcid)
        url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?dbfrom=pubmed&api_key=2cef50fe68b4d02b710a25436e6315634708&db=pubmed&cmd=neighbor_score&id=" + str(pmid)
        print(split)
        print(url)
        print(pmid)

        soup, request_status_code, time_stamp = get(url)
        time.sleep(0.34)

        DbFrom = str(soup.eLinkResult.LinkSet.DbFrom.text)
        Id_focal = soup.eLinkResult.LinkSet.IdList.Id.text
        LinkSetDbS = soup.eLinkResult.find_all('LinkSetDb')

        for LinkSetDb in LinkSetDbS:

            DbTo = LinkSetDb.DbTo
            LinkName = LinkSetDb.LinkName
            #print(DbTo)
            #print(LinkName)

            Links = LinkSetDb.find_all('Link')

            for Link in Links:

                Id = Link.Id.text
                Score = Link.Score.text

                outfile.write(DbFrom +'\t'+ Id_focal +'\t'+ DbTo.text +'\t'+ LinkName.text +'\t'+ Id +'\t'+ Score +'\t'+ time_stamp +'\t'+ request_status_code + '\n')

    outfile.close()  


