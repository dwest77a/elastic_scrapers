
def formatJson(infile, outfile):
    f = open(infile,'r')
    content = f.readlines()[0]
    f.close()

    def add_tabs(word, tabs):
        for t in range(tabs):
            word += '   '
        return word

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

if __name__ == '__main__':
    formatJson('jsons/98_14*1999-05-27','example.json')