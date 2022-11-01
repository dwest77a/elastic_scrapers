## Navigate to arsf directory
## Scan through years available
## Open yr/pcode/Docs/readme1.txt

## E. Compare readme1.txt and readme2.txt contents
## E. Extract metadata from readme files and add pcode as flight num, as well as readme

## 1. Python elasticsearch
## 1.1 check/count identical pcodes in arsf data
from datetime import datetime
import numpy as np
import json
import os
#import matplotlib.pyplot as plt
plt = ''
#import xarray as xarr

from elasticsearch import Elasticsearch
from pytostr import writeArrToString, writeDictToString # local

IS_PLOT  = False
IS_WRITE = True

VERBOSE = False

def rmWhiteSpace(word):
    isword = False
    new_word = ''
    for char in word:
        if isword and char != '\n':
            new_word += char
        elif char != ' ' and char != '\t':
            isword = True
            new_word += char
        else:
            pass
    return new_word

def getContents(file):
    try:
        f=open(file,'r')
        contents = f.readlines()
        f.close()
    except:
        contents = False
        if VERBOSE:
            print(file,'not found')
    return contents

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

def getARSFMetadata():
    f = open('arsf.json')
    content = json.load(f)
    f.close()
    return content

def getReadmeData(path):
    import re
    
    files = os.listdir(path)
    
    pattern = '.*[Rr][Ee][Aa][Dd].*[Mm][Ee].*'

    extract_from = []
    principle = False
    for f in files:
        if re.search(pattern, f):
            extract_from.append(path + '/' + f)

    if len(extract_from) > 0:
        counter = 0
        while not principle and counter < len(extract_from):
            content = getContents(extract_from[counter])
            if content:
                try:
                    PI = content[3]
                    if 'Principle' in PI:
                        principle = rmWhiteSpace(PI.split('-')[1])
                except:
                    pass
            counter += 1
    return principle

def getVars(path):
    from pyhdf.SD import SD, SDC

    files = os.listdir(path)
    rfiles = []
    for f in files:
        if f.endswith('.hdf'):
            rfiles.append(path + '/' + f)

    variables = []
    for f in rfiles:
        try:
            f1 = SD(f,SDC.READ)
            for value in f1.datasets().keys():
                if value == 'ATdata':
                    if 'ATM 0.42-13.5mm' not in variables:
                        variables.append('ATM 0.42-13.5mm')
                elif value not in variables:
                    variables.append(value)
        except:
            pass
    return variables

def getArchiveMetadata(path):
    # try to access a l1b file via jasmin connection
    # read parameters and return as metadata dict

    # l1b files - contain no useful metadata
    # 00readme - contains no useful metadata
    # catalogue_and_license - ATM/CASI search? (one method of instrument search)

    # flight plane - scrape from catalogue_and_license (check Piper)
    # variables    - l1b hdf files
    # geoinfo      - scrape from readme (vague info)
    # platform     - None
    # instrument   - scrape from catalogue_and_license (check Photographic Camera=Camera
    #                                                         ATM, CASI)

    if os.path.exists(path):
        cat_log_file = "00README_catalogue_and_licence.txt"
        readme = "00README"

        metadata = {
            'plane':'',
            'variables':'',
            'location':'',
            'platform':'',
            'instruments':[],
            'pi':''
        }

        ## -------------- Catalogue and License Search --------------
        catalogue = getContents(path + '/' + cat_log_file)
        if catalogue:
            if type(catalogue) == list:
                catalogue = catalogue[1]

            if 'Photographic Camera' in catalogue:
                metadata['instruments'].append('Camera')
            is_recording = False
            buffer = ''
            for x in range(len(catalogue)):

                ## ----------- Instrument -------------
                if catalogue[x:x+3] == 'ATM':
                    metadata['instruments'].append('ATM')
                elif catalogue[x:x+4] == 'CASI':
                    metadata['instruments'].append('CASI')

                ## ----------- Flight Plane -----------
                elif catalogue[x:x+5] == 'Piper':
                    is_recording = True

                elif catalogue[x+1:x+7] == 'during':
                    is_recording = False
                    metadata['plane'] = buffer
                    buffer = ''
                else:
                    pass

                if is_recording:
                    buffer += catalogue[x]

        ## -------------- 00README Search --------------
        readme_outer = getContents(path + '/' + readme)
        if readme_outer:
            try:
                metadata['location'] = readme_outer[0].replace('\n','').replace(',',' -')
            except:
                metadata['location'] = ''

        ## -------------- README Extra Search --------------
        # Try in order: path + docs, path + * + docs
        data = False
        dirpath1 = path + '/Docs/'
        if os.path.isdir(dirpath1):
            data = getReadmeData(dirpath1)
        else:
            dirpath2 = ''
            for xf in os.listdir(path):
                xfile = path + '/' + xf
                if os.path.isdir(xfile):
                    for yf in os.listdir(xfile):
                        yfile = xfile + '/' + yf
                        if 'Docs' in yfile:
                            # Location of docs folder identified
                            dirpath2 = yfile
            if dirpath2 != '':
                data = getReadmeData(dirpath2)

        if data:
            metadata['pi'] = data

        ## -------------- L1B Variable Search -------------- 
        vars = False
        if os.path.exists(path + '/L1b'):
            vars = getVars(path + '/L1b')
        elif os.path.exists(path + '/ATM'):
            vars = ['Old']
        else:
            pass

        if vars:
            metadata['variables'] = vars

        # retrieve l1b variable names

        
        return metadata

    else:
        return None

    

#f = open('response.txt','r')
#content = f.readlines()
#f.close()
#findMatchingPcodes(content[0])

if __name__ == '__main__':
    principles = 0

    arsf_meta = getARSFMetadata()
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
                "includes":[
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
                            },
                            {
                                "range":{
                                    "temporal.start_time":{}}}
#                                        "from": "1992-01-01",
 #                                       "to": "1998-12-12"
#                                    }
#                                }
#                            }
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
            "size":5400
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
    removed = 0
    remaining = 0

    print('ES responded with {} hits'.format(len(response['hits']['hits'])))

    for index, hit in enumerate(response['hits']['hits']):

        path       = hit['_source']['file']['path'].split('/')
        pcode      = path[4]

        short_path = '/'.join(path[0:5])

        start_time = hit['_source']['temporal']['start_time']
        end_time   = hit['_source']['temporal']['end_time']
        
        # Append new entry per hit with basic info
        hit_response_arr.append([pcode, start_time, index, short_path, end_time])

        # Extract spatial coords for each hit
        arr = np.array(hit['_source']['spatial']['geometries']['display']['coordinates'], dtype=float)
        X = np.transpose(arr)[0]
        Y = np.transpose(arr)[1]

        xconditions = np.logical_and( X < np.mean(X) + np.std(X)*3, X > np.mean(X) - np.std(X)*3)
        yconditions = np.logical_and( Y < np.mean(Y) + np.std(Y)*3, Y > np.mean(Y) - np.std(Y)*3)

        Xc = X[np.logical_and(xconditions, yconditions)]
        Yc = Y[np.logical_and(xconditions, yconditions)]

        removed += len(X) - len(Xc)
        remaining += len(Xc)

        spatial_arr.append(np.transpose(np.vstack((Xc,Yc))))

    # Map array to np.array
    spatial_arr = np.array(spatial_arr, dtype=object)
    print('Removed {} ({}%) coordinate entries outside stdev bounds'.format(removed, (removed*100)/(removed+remaining)))
    print('Remaining coords: {} '.format(remaining))

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
    ptcodes_written = ''
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
        
        ptcodes_written += ptcode + '\n'

    print('Compiled a list of {} PCodes'.format(len(ptcodes_metadata)))

    ## --------------------------- Step 4 ------------------------------
    """
    For each ptcode array within the dictionary (again)
     - Arrays are now sorted, but must loop for duplicate time entries
     - Originally planning to average duplicate coordinate arrays
       but currently just take the first 'primary' array as correct one

    """

    for ptcode in ptcodes_sorted.keys():
        print(ptcode)
        ptcodes_arr = ptcodes_sorted[ptcode]

        ## ----------- 4.1: Add time entries to dict to detect duplicates -----------
        do_not_concat = []
        #print('ptcode: ',ptcode)
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

        ## ----------- 4.4: Assemble json data from template (primary index) -----------

        metadata = ptcodes_metadata[ptcode]

        archive_metadata = getArchiveMetadata(metadata['path'])
        
        template = response['hits']['hits'][metadata['index']]
        
        template["_source"]["file"]["path"] = metadata['path']
        template["_source"]["spatial"]["geometries"]["display"]["coordinates"] = sum_arr.tolist()

        template["_source"]["temporal"]["start_time"] = metadata['start']
        template["_source"]["temporal"]["end_time"] = metadata['end']

        template["_source"]["misc"] = {
            "flight_num":"",
            "pcode":metadata['ptcode'].split('*')
        }

        del template["_source"]["file"]["filename"]

        if archive_metadata:
            # Add archive metadata
            if archive_metadata['pi'] != '':
                principles += 1

            template["_source"]["misc"] = dict(template["_source"]["misc"],**archive_metadata)

        ## ----------- 4.5: Add NEODC/ARSF Metadata retrieved from xls documents -----------

        date_old = metadata['ptcode'].split('*')[1]
        dt = date_old.split('-')
        date = '{}/{}/{}'.format(dt[2],dt[1],dt[0])

        try:
            arsf_metadata = arsf_meta[date]
            if arsf_metadata['FMatch']:
                l1 = arsf_metadata['Location']
                l2 = arsf_metadata['NLocation']
                if (l1 == l2) or (l1 in l2) or (l2 in l1):
                    # take longest
                    if len(l1) > len(l2):
                        locations = [l1]
                    else:
                        locations = [l2]
                else:
                    locations = [l1,l2]

                locations.append(template["_source"]["misc"]["location"])

                alt = arsf_metadata['Altitude']
                # Add additional metadata in correct places
                # full match only for now
                # add locations (arr) if not equal or in one another
                # add site code - difference?
                # add altitude
                try:
                    template["_source"]["misc"]["altitude"] = alt
                    template["_source"]["misc"]["location"] = locations
                except KeyError:
                    pass
                    # No pcode - shouldnt happen really
            else:
                pass
                # Not fully matching so don't write yet


        except:
            # No arsf metadata for this record
            pass



        ## ----------- 4.5: Plot sum_arr X,Y values to visualise data -----------
        if IS_PLOT:
            X = np.transpose(sum_arr)[0]
            Y = np.transpose(sum_arr)[1]
            plt.plot(X,Y)
            plt.show()

        ## ----------- 4.6: Write json style dict to a string -----------

        if IS_WRITE:
            g = open('jsons/{}.json'.format(ptcode),'w')
            g.write(json.dumps(template))
            g.close()

    print('PIs located:',principles)

    if IS_WRITE:
        print('Written all Jsons')
    else:
        print('Program exited - no jsons were saved')

    h = open('ptcodes.txt','w')
    h.write(ptcodes_written)
    h.close()

       