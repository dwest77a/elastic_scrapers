
import json
import os

directory = 'flight_catalog/collections/items'
files = os.listdir(directory)



def correctCoords(file, changes):
    f = open(file,'r')
    content = json.load(f)
    f.close()
    rchanges = int(changes)
    coords = content['geometry']['display']['coordinates']
    new_coords = []
    for dim1 in coords:
        ndim1 = []
        if dim1 != []:
            for dim2 in dim1:
                if dim2 != []:
                    ndim1.append(dim2)
                else:
                    changes += 1
            new_coords.append(ndim1)
        else:
            changes += 1
    if rchanges != changes:
        content['geometry']['display']['coordinates'] = new_coords
        g = open(file,'w')
        g.write(json.dumps(content))
        g.close()
    else:
        print('No ammendments')
    return changes

changes = 0
for f in files:
    changes = correctCoords(directory + '/' + f, changes)