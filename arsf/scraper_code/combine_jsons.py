import json

VERBOSE = False

def twodp(x, first=False):
    if len(str(x)) < 2:
        x = '0' + str(x)
    elif len(str(x)) > 2:
        if first:
            x = x[:2]
        else:
            x = x[2:4]
    else:
        pass
    return str(x)

def makeCleanPcode(pcode_raw):
    pcode0 = pcode_raw.split('/')[0]
    pcode1 = pcode_raw.split('/')[1]

    pcode1a = ''
    # extract first numbers from pcode1
    is_num = False
    is_end_num = False
    for char in pcode1:
        try:
            num    = int(char)
            is_num = True
        except:
            if is_num:
                is_end_num = True
            is_num = False
        if is_num and not is_end_num:
            pcode1a += str(char)

    pcode1b = twodp(pcode1a, first=True)

    pcode = pcode0 + '/' + pcode1b
    return pcode

def makeCleanPtcode(pcode_raw, date_raw):
    pcodes_to_clean = []
    if '&' in pcode_raw:
        alt_pcode = pcode_raw.split('&')[1].replace(' ','')
        pcode_raw = pcode_raw.split('&')[0].replace(' ','')
        pcodes_to_clean.append(alt_pcode)
    else:
        alt_pcode = ''
    pcodes_to_clean.append(pcode_raw)

    date = twodp(date_raw.split('/')[0]) + '/' + twodp(date_raw.split('/')[1]) + '/' + twodp(date_raw.split('/')[2])

    if '/' in pcode_raw:    
        pcode = makeCleanPcode(pcode_raw)
    else:
        pcode = pcode_raw

    if '/' in alt_pcode:    
        alt_pcode = makeCleanPcode(alt_pcode)
    else:
        pass

    return pcode + '*' + date, alt_pcode

f = open('arsf_dbs.json','r')
arsf_dbs = json.load(f)
f.close()

f = open('arsf.json','r')
arsf_main = json.load(f)
f.close()

new_arsf_database = {}
ptcode_codes = []

match_main, no_match_main, main_ptcodes = 0,0,0

for key in arsf_main.keys():

    # Extract pcode and date and put into consistent format

    ptcode, alt_pcode = makeCleanPtcode(arsf_main[key]['Pcode'], key)

    # These data get added straight to new database - plus separating alt pcode if it exists
    new_arsf_database[ptcode] = arsf_main[key]
    new_arsf_database[ptcode]['alt_pcode'] = alt_pcode

    main_ptcodes += 1

for key in arsf_dbs.keys():
    pcode_raw = arsf_dbs[key]['pcode']
    date_raw = arsf_dbs[key]['date']

    ptcode, alt_pcode = makeCleanPtcode(pcode_raw, date_raw)

    # Try adding extra dbs data to existing code, except creating new entry from this code
    try:
        new_arsf_database[ptcode] = dict(new_arsf_database[ptcode],**arsf_dbs[key])

        # If there is already a non-zero non matching alt code, create alt code array - don't expect this code to be necessary
        if new_arsf_database[ptcode]['alt_pcode'] != '' and new_arsf_database[ptcode]['alt_pcode'] != alt_pcode:
            new_arsf_database[ptcode]['alt_pcode'] = [alt_pcode,new_arsf_database[ptcode]['alt_pcode']]

        match_main += 1

    except KeyError:
        new_arsf_database[ptcode] = arsf_dbs[key]
        new_arsf_database[ptcode]['alt_pcode'] = alt_pcode
        no_match_main += 1

f = open('arsf_complete.json','w')
f.write(json.dumps(new_arsf_database))
f.close()

if VERBOSE:
    print(main_ptcodes + no_match_main, len(new_arsf_database.keys()))

print('Total from arsf.json:',main_ptcodes)
print(' -- Matched in arsf_dbs.json', match_main)
print(' -- Unmatched in arsf_dbs.json', no_match_main)

