
def add_tabs(word, tabs):
    for t in range(tabs):
        word += '   '
    return word

def formatJsonFile(infile, outfile):
    f = open(infile,'r')
    content = f.readlines()[0]
    f.close()

    tabs = 0
    word = ''
    for char in content:
        word += char
        if char == '{' or char == '[':
            word += '\n'
            tabs += 1
            word = add_tabs(word, tabs)
        elif char == '}' or char == ']':
            word += '\n'
            tabs -= 1
            word = add_tabs(word, tabs)
        elif char == ',':
            word += '\n'
            word = add_tabs(word, tabs)
        else:
            pass

    g = open(outfile,'w')
    g.write(word)
    g.close()

def formatJsonString(string):

    tabs = 0
    word = ''
    for char in string:
        word += char
        if char == '{' or char == '[':
            word += '\n'
            tabs += 1
            word = add_tabs(word, tabs)
        elif char == '}' or char == ']':
            word += '\n'
            tabs -= 1
            word = add_tabs(word, tabs)
        elif char == ',':
            word += '\n'
            word = add_tabs(word, tabs)
        else:
            pass

    return word

if __name__ == '__main__':
    inp = input('File in:')
    out = input('File out:')
    try:
        formatJsonFile(inp, out)
    except:
        print('Error processing file')