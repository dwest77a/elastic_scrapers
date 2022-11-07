## Navigate to directory
## Scan through years available
## Open yr/pcode/Docs/readme1.txt

## E. Compare readme1.txt and readme2.txt contents
## E. Extract metadata from readme files and add pcode as flight num, as well as readme

## 1. Python elasticsearch
from datetime import datetime
import numpy as np
import json
import os, sys

from elasticsearch import Elasticsearch
from pytostr import writeArrToString, writeDictToString # local

IS_WRITE = False
VERBOSE = True

def forceMatch(thing1, thing2):
    thing1 = thing1.lower()
    thing2 = thing2.lower()

    return (thing1 == thing2) or (thing1 in thing2) or (thing2 in thing1)

def twodp(x):
    if len(str(x)) < 2:
        x = '0' + str(x)
    elif len(str(x)) > 2:
        x = x[2:4]
    else:
        pass
    return str(x)

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

def findMatchingPcodes(response): ## Deprecated
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
        # org specific pcode finder
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

def getJsonMetadata(path):
    try:
        f = open(path)
        content = json.load(f)
        f.close()
    except FileNotFoundError:
        content = False
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

def getHDFVars(path):
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
            f1.end()
        except:
            pass
    return variables

def getNCVars(path):
    # Import netcdf python reader
    from netCDF4 import Dataset

    files = os.listdir(path)
    rfiles = [] # Files that are netCDF readable
    for f in files:
        if f.endswith('.nc'):
            rfiles.append(path + '/' + f)
    for f in rfiles:
        file = Dataset(f)
        unique_vars = {}
        for var in file.variables:
            lname = var
            sname = var.split('_')[0]
            
            try:
                unique_vars[sname].append(lname)
            except:
                unique_vars[sname] = [lname]

        for noint_name in unique_vars.keys():
            uvars = unique_vars[noint_name]
            if len(uvars) > 1:
                print('{}: {}'.format(
                                    noint_name, len(uvars)
                ))
            else:
                print(noint_name, 1)
        x=input()

def getArchiveMetadata(path, org_name='arsf'):
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

    if VERBOSE:
        print(' --- Starting archive metadata')

    if os.path.exists(path):
        cat_log_file = "00README_catalogue_and_licence.txt"
        readme = "00README"

        metadata = {
            'aircraft':'',
            'variables':'',
            'location':'',
            'platform':'',
            'instruments':[],
            'pi':''
        }

        ## -------------- Catalogue and License Search --------------

        if VERBOSE:
            print(' --- Retrieving Catalogue Licence data')

        catalogue = getContents(path + '/' + cat_log_file)
        if catalogue: ## arsf specific
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
                elif catalogue[x:x+5] == 'Piper' or catalogue[x+8:x+16] == 'aircraft':
                    is_recording = True

                elif catalogue[x+1:x+7] == 'during':
                    is_recording = False
                    metadata['aircraft'] = buffer
                    buffer = ''
                elif is_recording and x == len(catalogue)-1:
                    is_recording - False
                    metadata['aircraft'] = buffer # but remove end spaces
                    buffer = ''
                else:
                    pass

                if is_recording:
                    buffer += catalogue[x]

        ## FAAM:
        ## Catalogue - BAE-146 Aircraft

        ## -------------- 00README Search --------------

        if VERBOSE:
            print(' --- Retrieving Readme data')

        readme_outer = getContents(path + '/' + readme)
        if readme_outer:
            try:
                metadata['location'] = readme_outer[0].replace('\n','').replace(',',' -')
            except:
                metadata['location'] = ''

        ## -------------- README Extra Search --------------
        # Try in order: path + docs, path + * + docs

        if VERBOSE:
            print(' --- Retrieving Readme Docs data')

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

        if org_name == 'arsf': 
            vars = getArsfL1bVars(path)
        elif org_name == 'faam':
            vars = getFaamProcessedVars(path)
        else:
            vars = False
        
        if vars:
            metadata['variables'] = vars

        # retrieve l1b variable names

        
        return metadata

    else:
        return None

def getFaamProcessedVars(path):
    if VERBOSE:
        print(' --- Retrieving processed nc data')
    vars = False
    if os.path.exists(path + '/core_processed'):
        vars = getNCVars(path + '/core_processed')
    elif os.path.exists(path + '/core_raw'):
        vars = ['Old']
    else:
        pass
    return vars

def getArsfL1bVars(path):
    if VERBOSE:
        print(' --- Retrieving L1b data')
    skip_l1b = False
    vars = False
    if not skip_l1b:
        if os.path.exists(path + '/L1b'):
            vars = getHDFVars(path + '/L1b')
        elif os.path.exists(path + '/ATM'):
            vars = ['Old']
        else:
            pass
    return vars
    
def getRidOfGaps(arr):
    carr = [arr[0]]
    for x in range(1, len(arr)):
        item = arr[x]
        if item != '':
            carr.append(item)
    return carr


if __name__ == '__main__':
    principles = 0

    org_name = sys.argv[1]

    org_meta = getJsonMetadata('{}_complete.json'.format(org_name))

    session_kwargs = {
        'hosts': ['https://elasticsearch.ceda.ac.uk'],
        'use_ssl': False,
        'verify_certs': False,
        'ssl_show_warn':False
    }
    client = Elasticsearch(**session_kwargs)
    response = client.search(
        index=org_name,
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

        path       = getRidOfGaps(hit['_source']['file']['path'].split('/'))
        start_time = hit['_source']['temporal']['start_time']
        end_time   = hit['_source']['temporal']['end_time']

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
        
        # Org Specific pcode section
        if org_name == 'arsf':
            pcode      = path[4]
            short_path = '/'.join(path[0:5])
        else:
            pcode      = path[5]
            short_path = '/'.join(path[0:6])

        # Append new entry per hit with basic info
        hit_response_arr.append([pcode, start_time, index, short_path, end_time])

        

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
    # Universal ptcode - pcode/flight_num * date
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

    print('Compiled a list of {} ptcodes'.format(len(ptcodes_metadata)))

    ## --------------------------- Step 4 ------------------------------
    """
    For each ptcode array within the dictionary (again)
     - Arrays are now sorted, but must loop for duplicate time entries
     - Originally planning to average duplicate coordinate arrays
       but currently just take the first 'primary' array as correct one

    """
    no_basic, no_archive, no_org, some_data = 0, 0, 0, 0
    nmatch_pis = 0
    limit = len(ptcodes_sorted.keys())

    for count_ptcodes in range(0,limit):
        ptcode = list(ptcodes_sorted.keys())[count_ptcodes]
        ptcodes_arr = ptcodes_sorted[ptcode]
        print(ptcode, count_ptcodes)

        ## ----------- 4.1: Add time entries to dict to detect duplicates -----------
        do_not_concat = []
        if VERBOSE:
            print(' -- Adding time entries to dict to detect duplicates')
        times_dict = {}
        for entry in ptcodes_arr:
            try:
                times_dict[entry[0]].append(entry[1])
            except:
                times_dict[entry[0]] = [entry[1]]

        ## ----------- 4.2: Add duplicates to do_not_concat array -----------
        # This may be able to be shortened - was made like this for extra features that aren't needed now

        if VERBOSE:
            print(' -- Adding duplicates to do_not_concat array')
        for time in times_dict.keys():
            dupes = times_dict[time]
            if len(dupes) > 1:
                # Duplicate time entries - simply take primary coords
                for index in range(1,len(dupes)):
                    do_not_concat.append(dupes[index])

        ## ----------- 4.3: Stack arrays in the same ptcode -----------
        if VERBOSE:
            print(' -- Stacking arrays in same ptcode')

        sum_arr = []
        for idx, entry in enumerate(ptcodes_arr):
            if entry[1] not in do_not_concat: # and idx not in [16, 17, 18]:
                sum_arr.append(spatial_arr[entry[1]].tolist())

        ## ----------- 4.4: Determine which metadata sources exist for this ptcode -----------
        metadata, archive_metadata, org_metadata = False, False, False
        
        if VERBOSE:
            print(' -- Finding metadata')

        date_old = ptcode.split('*')[1]
        dt = date_old.split('-')
        date = '{}/{}/{}'.format(twodp(dt[2]),twodp(dt[1]),twodp(dt[0]))
        ptcode_fmt_org = ptcode.split('*')[0].replace('_','/') + '*' + date
        
        ### ========= 4.4.1 Basic Info already extracted =========
        try:
            metadata = ptcodes_metadata[ptcode]
        except KeyError:
            no_basic += 1
            pass

        ### ========= 4.4.2 CEDA Archive Metadata =========
        try:
            archive_metadata = getArchiveMetadata(metadata['path'],org_name=org_name) # org specific
        except KeyError:
            no_archive += 1
            pass
        if not archive_metadata:
            no_archive += 1

        ### ========= 4.4.3 Extra Metadata =========
        try:
            org_metadata = org_meta[ptcode_fmt_org] # org specific
        except:
            no_org += 1
            pass

        if archive_metadata or org_metadata:
            some_data += 1

        if archive_metadata and org_metadata:
            print(ptcode)
        
        ## ----------- 4.5: Assemble json data from template (primary index) -----------

        if VERBOSE:
            print(' -- Appending collected metadata')

        template = response['hits']['hits'][metadata['index']]
        
        template["_source"]["file"]["path"] = metadata['path']

        template["_source"]["spatial"]["geometries"]["display"]["coordinates"] = sum_arr

        template["_source"]["temporal"]["start_time"] = metadata['start']
        template["_source"]["temporal"]["end_time"] = metadata['end']

        if org_name == 'arsf':
            template["_source"]["misc"] = {
                "flight_num":"",
                "pcode":ptcode.split('*')
            }
        elif org_name == 'faam':
            template["_source"]["misc"] = {
                "flight_num":ptcode.split('*')[0]
            }
        else:
            pass

        del template["_source"]["file"]["filename"]

        if archive_metadata:
            template["_source"]["misc"] = dict(template["_source"]["misc"],**archive_metadata)

        ## ----------- 4.6: Add NEODC/org Metadata retrieved from xls documents -----------
        if VERBOSE:
            print(' -- Fetching NEODC/org metadata')
        
        if org_metadata:
            locs = []

            if VERBOSE:
                print(' -- Found NEODC/org metadata => Adding to template')

            for key in ['Location','Nlocation','target','base']:
                try:
                    loc = org_metadata['Location'].lower()
                    if loc not in locs:
                        locs.append(loc)
                except KeyError:
                    pass
            lfinal = template["_source"]["misc"]["location"].lower()
            if lfinal not in locs:
                locs.append(lfinal)

            fcrew_params = ['principle','pilot','navigator','operator']

            flight_crew = {}
            for fcrew in fcrew_params:
                try:
                    person = org_metadata[fcrew]
                except:
                    person = ''
                flight_crew[fcrew] = person

            template["_source"]["misc"]["crew"] = flight_crew

            try:
                alt = org_metadata['Altitude']
            except KeyError:
                alt = ''

            if 'f' in alt:
                alt = alt.split('f')[0] + ' ft'
            elif 'm' in alt:
                alt = str(int(alt.split('m')[0])*3.28) + ' ft'
            template["_source"]["misc"]["altitude"] = alt

            try:
                platform = org_metadata['airc']
            except KeyError:
                platform = ''
            template["_source"]["misc"]["platform"] = platform

            try:
                pi_readme = template["_source"]["misc"]["pi"]
            except KeyError:
                pi_readme = ''
            
            try:
                pi_db = template["_source"]["misc"]["crew"]["principle"]
            except KeyError:
                pi_db = ''

            if pi_readme != '' and pi_db != '' and not forceMatch(pi_readme, pi_db): # If they don't match
                nmatch_pis += 1
                template["_source"]["misc"]["crew"]["principle"] = [pi_readme, pi_db]
            elif pi_readme != '' or pi_db != '': # Only one exists
                if pi_readme != '':
                    template["_source"]["misc"]["crew"]["principle"] = pi_readme
                principles += 1
            else:
                pass
            del template["_source"]["misc"]["pi"]

            if VERBOSE:
                print(' -- Finished applying metadata')

        ## ----------- 4.8: Write json style dict to a string -----------

        if IS_WRITE:
            g = open('jsons/{}_flight_info.json'.format(ptcode),'w')
            g.write(json.dumps(template))
            g.close()

    print('Total pcodes written from ES:',limit)
    print('')
    print('PIs located: {}/{}'.format(principles, limit))
    print('Non-blank Non-matching PIs:',nmatch_pis)
    print('')
    print(' -- Missing basic info:',no_basic)
    print(' -- Missing archive info:', no_archive)
    print(' -- Missing org_meta data:',no_org)
    print('')
    print('Indexes containing at least some extended info: {}/{}'.format(some_data, limit))

    if IS_WRITE:
        print('Written all Jsons')
    else:
        print('Program exited - no jsons were saved')

    h = open('ptcodes.txt','w')
    h.write(ptcodes_written)
    h.close()

       