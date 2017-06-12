# -*- coding: UTF-8 -*-

import os
import pandas as pd


def merge_file(dest):
    l_nodes = ['master', 'slave1', 'slave2', 'slave3']
    for node in l_nodes:
        print "###################" + node + "###################"
        l_allevents = []
        for root, dirs, files in os.walk(dest):
            for OneFileName in files:
                if OneFileName.find('new_' + node + '_raw.csv') == -1:
                    continue
                OneFullFileName = os.path.join(root, OneFileName)
                print OneFullFileName
                # read .csv file
                df_partfile = pd.read_csv(OneFullFileName, index_col='time')
                l_allevents.extend([df_partfile[col] for col in df_partfile.columns])

        df_file = pd.DataFrame(l_allevents)
        df_file = df_file.T

        print df_file.shape
        df_file.to_csv('\\'.join(root.split('\\')[:-1]) + '\\' + node + '_merge_raw.csv')

    return


if __name__ == "__main__":
    dest = "C:\Users\lyr\Desktop\HardwareEvents\ExpRanker\data\\terasort-spark.shuffle.compressFALSE"
    merge_file(dest)