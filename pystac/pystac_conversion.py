import json
import os

def convertFile(filename, fileout, spatial_minmax, temporal_minmax, org):
    f = open(filename,'r')
    content = json.load(f)
    f.close()

    flight = content["_source"]

    id = filename.split('_flight_info')[0].split('/')[-1]


    try:
        org = flight["misc"]["organisation"]
    except:
        org = flight["file"]["path"].split('/')[2]

    try:
        pi = flight["misc"]["crew"]["principle"]
    except:
        pi = flight["misc"]["pi"]

    stac_content = {
        "id": id,
        "type" : "Feature",
        "stac_version": "1.0.0",
        "stac_extensions":[""],

        "description_path":flight["file"]["path"],

        "collection":org,

        "geometry": flight["spatial"]["geometries"],
        "properties":{
            "data_format":flight["data_format"]["format"],
            "start_datetime":flight["temporal"]["start_time"],
            "end_datetime": flight["temporal"]["end_time"],
            "flight_num":flight["misc"]["flight_num"],
            "pcode": flight["misc"]["pcode"],
            "aircraft": flight["misc"]["aircraft"],
            "variables": flight["misc"]["variables"],
            "location": flight["misc"]["location"],
            "platform": flight["misc"]["platform"],
            "instruments": flight["misc"]["instruments"],
            "principle": pi
        },
        "assets":{},
        "links":[]
    }

    g = open(fileout,'w')
    g.write(json.dumps(stac_content))
    g.close()

    return spatial_minmax, temporal_minmax, orgs


def createCollection(org, links, spatial_minmax, temporal_minmax):
    
    desc = '{} Flight Collection'.format(org)

    cfile = "./collections/{}.json".format(org)

    ## Build Links

    collection_links = [{
            "rel":"self",
            "href":cfile,
            "type":"application/json",
            "title":org
        }
    ]
    for link in links:
        collection_links.append({
            "rel":"item",
            "href":"./{}/{}_flight_info.json".format(org, link), # Filename
            "type":"application/json",
            "title":link
        })

    collection = {
        "id":org,
        "type": "Collection",
        "stac_extensions": [
            ""
        ],
        "stac_version":"1.0.0",
        "description": desc,
        "title": org,
        "providers": [],
        "extent": {
            "spatial": {"bbox":[spatial_minmax]},
            "temporal":{"interval":[temporal_minmax]}
        },
        "license":"", # Find this
        "summaries":{},
        "links":collection_links
    }

    g = file(cfile,'w')
    g.write(json.dumps(collection))
    g.close()

def createCatalog(links):

    catalog_links = [
        {
            "rel":"self",
            "href":"./flight_catalog.json",
            "type":"application/json",
            "title":"Flight Catalog"
        }
    ]

    for link in links:
        catalog_links.append({
            "rel":"child",
            "href":"./collections/{}.json".format(link), # Filename
            "type":"application/json",
            "title":link
        })

    catalog = {
        "id":"Flight_Catalog",
        "type":"Catalog",
        "stac_version":"1.6.1",
        "stac_extensions":[""],
        "title":"Flight Catalog",
        "description": "Full catalog of flights for CEDA flight finder",
        "links":catalog_links
    }

    g = file("./flight_catalog.json",'w')
    g.write(json.dumps(catalog))
    g.close()

def iterateFiles(directory):
    files = os.listdir(directory)
    for f in files:
        if os.path.isdir(directory + '/' + f):
            iterateFiles(directory + '/' + f)

file_in = '../jsons/WM06_07*2006-06-08_flight_info.json' #input('File in:')
file_out = 'ex.json' #input('File out:')
convertFile(file_in, file_out)