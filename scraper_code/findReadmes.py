import os

def recursiveFileCheck(path,dive):
    ifsum = ''
    if dive > 4:
        return ''
    else:
        new_items = {}
        try:
            files = os.listdir(path)
        except:
            return ''
        for f in files:
            if dive == 0:
                print(f)
            if os.path.isdir(path+'/'+f):
                ifsum += recursiveFileCheck(path+'/'+f, dive+1)
            elif os.path.isfile(path+'/'+f) and 'README' in f:
                try:
                    f = open(path+'/'+f,'r')
                    contents = f.readlines()
                    f.close()
                    isp,isa = False,False
                    for line in contents:
                        if 'rinciple' in line and not isp:
                            isp = True
                            ifsum += 'P'
                        if 'ATM' in line and not isa:
                            isa = True
                            ifsum += 'A'
                            
                except:
                    pass
            else:
                pass
    return ifsum

ifsum = recursiveFileCheck("/neodc/arsf/", 0)
pcount = 0
acount = 0
for char in ifsum:
    if char == 'P':
        pcount += 1
    elif char == 'A':
        acount += 1
print('README: ',pcount)
print('License: ', acount)