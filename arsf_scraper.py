## Navigate to arsf directory
## Scan through years available
## Open yr/pcode/Docs/readme1.txt

## E. Compare readme1.txt and readme2.txt contents
## E. Extract metadata from readme files and add pcode as flight num, as well as readme

## 1. Python elasticsearch
## 1.1 check/count identical pcodes in arsf data
from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt

from elasticsearch import Elasticsearch
from pytostr import writeArrToString, writeDictToString # local

def timeToDayFraction(time):
    init_day = datetime(1,1,1,0,0,0) # 1st Jan 1 AD
    date = time.split('T')[0]
    tme = time.split('T')[1]

    current = datetime(
        int(date.split('-')[0]), int(date.split('-')[1]), int(date.split('-')[2]),
        int(tme.split(':')[0]), int(tme.split(':')[1]), int(tme.split(':')[2])
    )

    delta = current - init_day

    return delta.days + delta.seconds/3600

def findMatchingPcodes(response):
    # response - full elasticsearch json-style response
    #          - string version
    is_path = False
    paths = []
    for x in range(len(response)-50):
        #print(response[x:x+4])
        if response[x:x+4] == 'path':
            # Assume "path":"/////",
            path_start = x+7
            is_path = True
        if response[x+1] == ',' and is_path:
            paths.append(response[path_start:x])
            is_path = False
    print('Found {} paths'.format(len(paths)))
    pdict = {}
    pconcat = []
    for path in paths:
        # ARSF specific pcode finder
        pcode = path.split('/')[4]
        try:
            pdict[pcode] += 1
        except:
            pdict[pcode] = 1
            pconcat.append(pcode)
    print('Concatenated to {} pcodes'.format(len(pconcat)))
    for pcode in pconcat:
        print(pcode, pdict[pcode])
    return None

#f = open('response.txt','r')
#content = f.readlines()
#f.close()
#findMatchingPcodes(content[0])

if __name__ == '__main__':
    session_kwargs = {
        'hosts': ['https://elasticsearch.ceda.ac.uk'],
        'use_ssl': False,
        'verify_certs': False,
        'ssl_show_warn':False
    }
    client = Elasticsearch(**session_kwargs)
    response = client.search(
        index="arsf",
        body={
            "_source":{
                "include":[
                    "data_format.format",
                    "file.filename",
                    "file.path",
                    "misc",
                    "spatial.geometries.display",
                    "temporal"]
                }
            ,
            "query":{
                "bool":{
                    "filter":{
                        "bool":{
                        "must":[
                            {
                                "exists":{
                                    "field":"spatial.geometries.display.type"}
                                }
                            ,
                            {
                                "range":{
                                    "temporal.start_time":{
                                    }
                                    }
                                }
                            ]
                        ,
                        "must_not":[
                            {
                                "term":{
                                    "spatial.geometries.display.type":"point"}
                                }
                            ]
                        ,
                        "should":[
                            {
                                "geo_shape":{
                                    "spatial.geometries.search":{
                                    "shape":{
                                        "type":"envelope",
                                        "coordinates":[
                                            [
                                                -23.077809999963105,
                                                16.23134230314697],
                                            [
                                                -23.04575223781711,
                                                16.192729641492857]
                                            ]
                                        }
                                    }
                                    }
                                }
                            ]
                        }
                        }
                    }
                }
            ,
            "aggs":{
                "variables":{
                    "nested":{
                        "path":"parameters"}
                    ,
                    "aggs":{
                        "std_name":{
                        "filter":{
                            "term":{
                                "parameters.name":"standard_name"}
                            }
                        ,
                        "aggs":{
                            "values":{
                                "terms":{
                                    "field":"parameters.value.raw"}
                                }
                            }
                        }
                        }
                    }
                }
            ,
            "size":400
        }
    )

    ## --------------------------- Step 1 ------------------------------
    """
    Generate a list of all pcodes, times and indices from elasticsearch response
     - Approximately 400 entries expected
     - Some entries will have identical pcodes
     - Some entries will have identical pcodes and times (not indices)
    """
    
    # Define array for storing spatial coords
    spatial_arr = []
    # Define array for storing hit response info (basic)
    hit_response_arr = []

    for index, hit in enumerate(response['hits']['hits']):

        path       = hit['_source']['file']['path'].split('/')
        pcode      = path[4]

        short_path = '/'.join(path[0:5])

        start_time = hit['_source']['temporal']['start_time']
        end_time   = hit['_source']['temporal']['end_time']
        
        # Append new entry per hit with basic info
        hit_response_arr.append([pcode, start_time, index, short_path, end_time])

        # Extract spatial coords for each hit
        spatial_arr.append(hit['_source']['spatial']['geometries']['display']['coordinates'])

    # Map array to np.array
    spatial_arr = np.array(spatial_arr)

    ## --------------------------- Step 2 ------------------------------
    """
    Generate a dictionary of pcodes where each pcode links to an array of hit indexes
     - Pcodes in dictionary are combined <pcode>_<time> code since
       some pcodes link to indexes with different dates
     - New style of pcode is labelled as a ptcode
     - Array for each ptcode contains entries for each hit as [time, index, path, end]

    """
    ptcodes = {}
    for entry in hit_response_arr:
        ptcode = entry[0]+'*'+entry[1].split('T')[0]
        try:
            ptcodes[ptcode].append(entry[1:5])
        except:
            ptcodes[ptcode] = [entry[1:5]]

    ## --------------------------- Step 3 ------------------------------
    """
    For each ptcode array within the dictionary
     - Sort entries by time stored
     - Create new sorted dictionary containing arrays for each ptcode 
       where internal arrays are sorted chronologically
     - Assemble metadata array with an entry for each ptcode

    """

    ptcodes_metadata = {}
    ptcodes_sorted = {}
    for ptcode in ptcodes.keys():
        # Define internal array as ptcode_hits - the 'hits' for each ptcode
        ptcode_hits = ptcodes[ptcode]
        ptcodes_sorted[ptcode] = sorted(ptcode_hits)

        # Assemble metadata array for each ptcode
        primary = ptcodes_sorted[ptcode][0]
        final   = ptcodes_sorted[ptcode][-1]
        ptcodes_metadata[ptcode] = {
                'ptcode': ptcode,     # Name of ptcode
                'path'  : primary[2], # Primary ptcode path
                'index' : primary[1], # Primary Index for each ptcode
                'start' : primary[0], # Primary ptcode start time
                'end'   : final[3]    # Final ptcode end time
            }

    print('Compiled a list of {} PCodes'.format(len(ptcodes_metadata)))

    ## --------------------------- Step 4 ------------------------------
    """
    For each ptcode array within the dictionary (again)
     - Arrays are now sorted, but must loop for duplicate time entries
     - Originally planning to average duplicate coordinate arrays
       but currently just take the first 'primary' array as correct one

    """

    for ptcode in ptcodes_sorted.keys():
        ptcodes_arr = ptcodes_sorted[ptcode]

        ## ----------- 4.1: Add time entries to dict to detect duplicates -----------
        do_not_concat = []
        print('ptcode: ',ptcode)
        times_dict = {}
        for entry in ptcodes_arr:
            try:
                times_dict[entry[0]].append(entry[1])
            except:
                times_dict[entry[0]] = [entry[1]]

        ## ----------- 4.2: Add duplicates to do_not_concat array -----------
        # This may be able to be shortened - was made like this for extra features that aren't needed now

        for time in times_dict.keys():
            dupes = times_dict[time]
            if len(dupes) > 1:
                # Duplicate time entries - simply take primary coords
                for index in range(1,len(dupes)):
                    do_not_concat.append(dupes[index])

        ## ----------- 4.3: Stack arrays in the same ptcode -----------
        primary = ptcodes_arr[0]

        sum_arr = spatial_arr[primary[1]]
        for idx, entry in enumerate(ptcodes_arr):
            if entry[1] not in do_not_concat: # and idx not in [16, 17, 18]:
                sum_arr = np.vstack((sum_arr, spatial_arr[entry[1]]))

        ## ----------- 4.4: Assemble json query from template (primary index) -----------

        metadata = ptcodes_metadata[ptcode]
        
        template = response['hits']['hits'][metadata['index']]
        
        template["_source"]["file"]["path"] = metadata['path']
        template["_source"]["spatial"]["geometries"]["display"]["coordinates"] = list(sum_arr)

        template["_source"]["temporal"]["start_time"] = metadata['start']
        template["_source"]["temporal"]["end_time"] = metadata['end']

        template["_source"]["misc"]["flight_info"] = {
            "flight_num":"",
            "pcode":metadata['ptcode'].split('*')
        }
        
        del template["_source"]["file"]["filename"]

        ## ----------- 4.5: Plot sum_arr X,Y values to visualise data -----------

        X = np.transpose(sum_arr)[0]
        Y = np.transpose(sum_arr)[1]
        plt.plot(X,Y)
        plt.show()

        ## ----------- 4.6: Write json style dict to a string -----------

        is_write = False
        if is_write:
            g = open('jsons/{}'.format(ptcode),'w')
            g.write(writeDictToString(template))
            g.close()
        else:
            pass
    print('Written all Jsons')

       