import sys
import AppUtils as au
import contextlib


def formatArgDoc(rawText):
    usage = ["Usage\n-----\n::\n\n"]
    options = ["Options\n-------\n::\n\n"]
    partition = False
    for line in rawText:
        a = line[0:20]
        if not partition:
            if line[0:6] == 'usage:':
               line = "      "+ line[6:]
            elif line[0] == '\n':
                partition = True
            usage.append(line)
        else:
            if not line == 'optional arguments:\n':
                options.append(line)

    cleanText = ["MEETMain\n========\n\n"]+usage+options
    return cleanText


###Run with -h arg to generate list###
file_path = 'docs/source/usage.rst'
sys.stdout = open(file_path, "w")
with open(file_path, "w+") as o:
    with contextlib.redirect_stdout(o):
        parser = au.getParser(au.DEFAULT_CONFIG)
        parser.prog = "MEETMain"
        parser.print_help()
    o.seek(0)
    rawText = o.readlines()
    cleanText = formatArgDoc(rawText)
    o.seek(0)
    o.writelines(cleanText)
    pass
