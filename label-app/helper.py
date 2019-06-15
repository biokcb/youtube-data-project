# create a list of all comment files

import os 

directory = './data/'
htmlval = "<option value=\"replace_me\">replace_me</option>"

with open('files_to_process.txt','w') as fp:
    for infile in os.listdir(directory):
        fp.writelines(htmlval.replace('replace_me', infile)+'\n')    
