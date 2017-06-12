# -*- coding: UTF-8 -*-

import os

dest = "C:\Users\lyr\Desktop\HardwareEvents\ExpRanker\data\\0607terasort"
for root, dirs, files in os.walk(dest):
        for OneFileName in files:
            if OneFileName.find('_raw.csv') == -1:
                continue
            OneFullFileName = os.path.join(root, OneFileName)
            print OneFullFileName
            input_file = open(OneFullFileName, 'r')
            output_file = open(root + '\\new_' + OneFileName, 'w')

            for line in input_file.readlines():
                newline = line.split(',')
                top = newline[0]
                if top != 'time':
                    num = str(int(round(float(top))))
                    newline[0] = num
                else:
                    print line
                s = ','.join(newline)
                output_file.write(s)
