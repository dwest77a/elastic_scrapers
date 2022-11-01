
# Script for taking ptcodes list and finding matching identities in arsf records - gather locations as well
# Create csv file with matching entries - space for two locations 

import json

neodc_path = '/neodc/arsf/1999/'

f = open('ptcodes.txt','r')
ptlist = f.readlines()
f.close()

def match_codes(ptcode_es, ptcode_arsf):
    if ptcode_es not in ptcode_arsf:
        split_es = ptcode_es.split('/')

        alpha_es = split_es[0]
        try:
            beta_es = split_es[1].split('(')[0]
        except:
            beta_es = 'x'

        split_arsf = ptcode_arsf.split('/')

        alpha_arsf = split_arsf[0]
        try:
            beta_arsf = split_arsf[1].split('(')[0]
        except:
            beta_arsf = 'y'
        try:
            return int(alpha_es) == int(alpha_arsf) and int(beta_es) == int(beta_arsf)
        except ValueError:
            print('VE: ',ptcode_es, ptcode_arsf)
            return False
    else:
        return True

def twodp(x):
    if len(str(x)) < 2:
        x = '0' + str(x)
    return str(x)

def reformat(date):
    dt = date.split('-')
    return '{}/{}/{}'.format(dt[2],dt[1],dt[0])

def recalculate(ptcode):
    months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
    rcode = ptcode
    for x, month in enumerate(months):
        if month in ptcode:

            if month == ptcode.split('-')[1]: # 2022 project code
                rcode = '{}/22'.format(twodp(x+1))
            elif month == ptcode.split('-')[0]: # Non 2022 project code
                rcode = '{}/{}'.format(twodp(x+1), int(ptcode.split('-')[1]))
            else:
                print('Err matching month formatting')
    return rcode

h = open('neodc_meta.txt','r')
neodc = h.readlines()
h.close()

neodict = {}
for line in neodc:
    ln = line.split(',')
    date = ln[4]
    try:
        neodict[date]['count'] += 1
    except:
        neodict[date] = {
            'count':0,
            'site_no':ln[0],
            'loc':ln[1],
            'alt':ln[3]
        }


g = open('arsf_meta.csv','r')
arsf = g.readlines()
g.close()

arsf_hist = {}
arsf_meta_entries = 0

arsf_dict = {}

for i in range(1,len(arsf)):
    entry = arsf[i]

    en = entry.split(',')[:3]

    ptcode = recalculate(en[1])
    date = en[2]

    try:
        arsf_hist[date]['count'] += 1
    except:
        arsf_hist[date] = {
            # Metadata available
            'Loc': en[0],
            'ptcode': ptcode,
            'Count': 1,

            'FMatch': False, # Full Match
            'DMatch': False, # Matching dates
            'PMatch': False, # Matching pcodes
            'Adate': '',

            'site':'',
            'nloc':'',
            'alt':''
        }

        arsf_dict[ptcode] = date
        arsf_meta_entries += 1

pmatches = 0
dmatches = 0
fmatches = 0
nmatches = 0
lmatches = 0

dcross = 'Where flight pcodes match but dates do not\n'
pcross = 'Where flight dates match but pcodes do not\n'
unmatches = len(ptlist)
for line in ptlist:
    ln     = line.replace('\n','')
    ptcode = ln.split('*')[0].replace('_','/')
    date   = reformat(ln.split('*')[1])

    try:
        arsf_hist[date]['DMatch'] = True
        if match_codes(ptcode,arsf_hist[date]['ptcode']):
            arsf_hist[date]['PMatch'] = True
            arsf_hist[date]['FMatch'] = True
            fmatches += 1
        else:
            dmatches += 1
            pcross += '{} => {} for {}\n'.format(ptcode, arsf_hist[date]['ptcode'], date)
        unmatches -= 1
    except KeyError:
        # Dates do not match, try ptcodes?
        try:
            adate = arsf_dict[ptcode]
            arsf_hist[adate]['PMatch'] = True
            arsf_hist[adate]['Adate'] = date
            dcross += '{} => {} for {}\n'.format(adate,date, ptcode)
            pmatches += 1
            unmatches -= 1
        except KeyError:
            pass
            # No match

    try:
        # Match neodc dates as well - dmatch or fmatch
        arsf_hist[date]['site'] = neodict[date]['site_no']
        arsf_hist[date]['nloc'] = neodict[date]['loc']
        arsf_hist[date]['alt'] = neodict[date]['alt']
        if arsf_hist[date]['Loc'] == arsf_hist[date]['nloc']:
            lmatches += 1
        nmatches += 1
    except KeyError:
        pass

arsf_matches = {}
for key in arsf_hist.keys():
    if arsf_hist[key]['FMatch'] or arsf_hist[key]['DMatch'] or arsf_hist[key]['PMatch']:
        arsf_matches[key] = arsf_hist[key]


print('Total ARSF Entries :',arsf_meta_entries)
print('Total ES Entries   :',len(ptlist))
print('')
print('ES unmatched entries:',unmatches)
print('ES full matches  :',fmatches,'(pcode and date)')
print('ES Date matches  :',dmatches,'(date only - incorrect pcode?)')
print('ES Pcode matches :',pmatches,'(pcode only - no data for the specific flight)')
print('')
print('ES Neodc matches :',nmatches)
print('ES Neodc location matches:',lmatches,'(Expect very few)')

a = open('ptcodes_cross.txt','w')
a.write(pcross)
a.close()
a = open('dates_cross.txt','w')
a.write(dcross)
a.close()

b = open('arsf_complete.json','w')
b.write(json.dumps(arsf_matches))
b.close()