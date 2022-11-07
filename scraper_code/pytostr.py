def writeArrToString(json_arr):
    wrd = '['
    for x, item in enumerate(json_arr):
        if x != 0:
            wrd += ','
        if type(item) == dict:
            wrd += writeDictToString(item)
        elif type(item) == list:
            wrd += writeArrToString(item)
        else:
            try:
                wrd += '"' + item + '"'
            except:
                wrd += str(item) 
    wrd += ']'
    return wrd

def writeDictToString(json_dict):
    wrd = '{'
    for x, key in enumerate(json_dict.keys()):
        if x != 0:
            wrd += ','
        wrd += '"'+ key +'":'
        if type(json_dict[key]) == dict:
            wrd += writeDictToString(json_dict[key])

        elif type(json_dict[key]) == list:
            wrd += writeArrToString(json_dict[key])
        else:
            try:
                wrd += '"' + json_dict[key] + '"'
            except:
                wrd += str(json_dict[key]) 
    wrd += '}'
    return wrd