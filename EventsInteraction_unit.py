# -*- coding: UTF-8 -*-

import os
import numpy as np
import pandas as pd
from dtw import dtw
from sklearn import preprocessing
import json


def interaction(data):
    data.to_csv("interaction_X.csv")

    d_dtw = {}
    # d_dtw = {
    #   ('event1', 'event2'): dtw,
    #   ('event1', 'event3'): dtw,
    #   ...
    #   ('event9', 'event10'): dtw,
    # }
    scaler = preprocessing.MinMaxScaler()
    for i in data:
        for j in data:
                k = tuple(sorted([i, j], key=str.lower))
                x = np.array(data[i].dropna()).reshape(-1, 1)
                x = scaler.fit_transform(x)
                y = np.array(data[j].dropna()).reshape(-1, 1)
                y = scaler.fit_transform(y)
                dist, cost, acc, path = dtw(x, y, dist=lambda x, y: np.linalg.norm(x - y, ord=1))
                print dist
                d_dtw[k] = dist
    res = pd.Series(d_dtw)
    res = res.sort_values(ascending=True)
    # output_file = open('wordcount_InR.json', 'w').write(json.dumps(d_dtw, sort_keys=True))
    return res


if __name__ == "__main__":
    dest = "C:\Users\lyr\Desktop\HardwareEvents\ExpRanker\data\wordcount-spark"
    d_events = {'master':
                    ['UOPS_EXECUTED_PORT.PORT_1', 'MEM_LOAD_UOPS_L3_MISS_RETIRED.LOCAL_DRAM', 'MEM_UOPS_RETIRED.STLB_MISS_LOADS',
                     'INST_RETIRED.PREC_DIST', 'LONGEST_LAT_CACHE.REFERENCE', 'IDQ.ALL_MITE_CYCLES_4_UOPS',
                     'BR_MISP_RETIRED.ALL_BRANCHES', 'BR_INST_RETIRED.ALL_BRANCHES'],
                'slave1':
                    ['UOPS_EXECUTED_PORT.PORT_1', 'MEM_LOAD_UOPS_L3_MISS_RETIRED.LOCAL_DRAM',
                     'MEM_UOPS_RETIRED.STLB_MISS_LOADS',
                     'INST_RETIRED.PREC_DIST', 'LONGEST_LAT_CACHE.REFERENCE', 'IDQ.ALL_MITE_CYCLES_4_UOPS',
                     'BR_MISP_RETIRED.ALL_BRANCHES', 'BR_INST_RETIRED.ALL_BRANCHES'],
                'slave2':
                    ['UOPS_EXECUTED_PORT.PORT_1', 'MEM_LOAD_UOPS_L3_MISS_RETIRED.LOCAL_DRAM',
                     'MEM_UOPS_RETIRED.STLB_MISS_LOADS',
                     'INST_RETIRED.PREC_DIST', 'LONGEST_LAT_CACHE.REFERENCE', 'IDQ.ALL_MITE_CYCLES_4_UOPS',
                     'BR_MISP_RETIRED.ALL_BRANCHES', 'BR_INST_RETIRED.ALL_BRANCHES'],
                'slave3':
                    ['UOPS_EXECUTED_PORT.PORT_1', 'MEM_LOAD_UOPS_L3_MISS_RETIRED.LOCAL_DRAM',
                     'MEM_UOPS_RETIRED.STLB_MISS_LOADS',
                     'INST_RETIRED.PREC_DIST', 'LONGEST_LAT_CACHE.REFERENCE', 'IDQ.ALL_MITE_CYCLES_4_UOPS',
                     'BR_MISP_RETIRED.ALL_BRANCHES', 'BR_INST_RETIRED.ALL_BRANCHES'],
                }
    d_nodes = {'master': {}, 'slave1': {}, 'slave2': {}, 'slave3': {}}
    for k in d_nodes:
        # read data
        filename = os.path.join(dest, k + '_merge_raw.csv')
        print filename
        data = pd.read_csv(filename, index_col='time')
        names = data.columns
        print "whole data shape: ", data.shape

        # determine top 10 features vector X
        X = data[d_events[k]]
        print "X shape: ", X.shape
        print "X names: ", X.columns
        res = interaction(X)
        res.to_csv('wordcount_' + k + '_InteractionRank')






