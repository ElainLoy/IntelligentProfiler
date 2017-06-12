# -*- coding: UTF-8 -*-

import os
import numpy as np
import pandas as pd
from sklearn.feature_selection import VarianceThreshold
from sklearn.feature_selection import SelectKBest
from sklearn.feature_selection import f_regression
from sklearn.svm import SVR
from sklearn.feature_selection import RFE
from sklearn.feature_selection import SelectFromModel
from sklearn.ensemble import GradientBoostingRegressor
import json

def VarianceFilter(X, names):
    '''
    removing features with low var
    :param: old X & old names
    :return: new X, new names
    '''
    sel = VarianceThreshold(threshold=2.5)
    sel = sel.fit(X)
    mask = sel.get_support()

    n = names[mask]
    x = sel.transform(X)
    print x.shape, n
    return x, n


def FTest(X, y, names):
    '''
    f-test
    :param: old data & old names
    :return: new X, new names
    '''
    sel = SelectKBest(f_regression, k=2)
    sel = sel.fit(X, y)
    # print sel.scores_
    mask = sel.get_support()

    n = names[mask]
    x = sel.transform(X)
    print X.shape, n
    return x, n


def RFElimination(X, y, names):
    '''
    Recursive feature elimination
    :param: old data & old names
    :return: new X, new names
    '''
    est = SVR(kernel="linear")
    sel = RFE(estimator=est, step=1, scoring='accuracy')
    sel.fit(X, y)
    # print("Optimal number of features : %d" % sel.n_features_)
    mask = sel.get_support()

    x = sel.transform(X)
    n = names[mask]
    return x, n


if __name__ == "__main__":
    dest = "C:\Users\lyr\Desktop\HardwareEvents\ExpRanker\data\\0605terasort-spark"
    d_nodes = {'master': {}, 'slave1': {}, 'slave2': {}, 'slave3': {}}
    for k in d_nodes:
        # read data
        filename = os.path.join(dest, k + '_mergewith0_raw.csv')
        print filename
        data = pd.read_csv(filename, index_col='time')
        print "whole data shape: ", data.shape

        # clean
        data = data[data['IPC'] != 0]
        print "data after cleaning: ", data.shape

        # determine feature vector X and response variable y
        X = data.drop('IPC', axis=1)
        X = X.drop('IPS', axis=1)
        y = data['IPC']
        names = X.columns
        print "X shape n y shape: ", X.shape, y.shape

        # [X, names] = VarianceFilter(X, names)
        # [X, names] = FTest(X, y, names)
        # [X, names] = RFElimination(X, y, names)

        # tree-based feature selection recursively
        for turn in range(3):
            rgs = GradientBoostingRegressor(n_estimators=600, learning_rate=0.1, loss='ls',
                                            min_samples_split=10, random_state=None, min_samples_leaf=5)
            rgs = rgs.fit(X, y)
            importances = rgs.feature_importances_
            index = np.argsort(importances)[::-1]

            # Print and save the feature ranking
            RESdir = 'C:\Users\lyr\Desktop\HardwareEvents\ExpRanker\RES\\'
            outfile = open(RESdir + 'terasort_' + k + '_ImportanceRank' + '_turn' + str(turn+1), 'w')
            print("Feature ranking was written into files.")
            outfile.write('Feature ranking:\n')
            d_events = {}
            for i in range(X.shape[1]):
                d_events[names[index[i]]] = importances[index[i]]
                outfile.write("%d. %s (%f)\n" % (i + 1, names[index[i]], importances[index[i]]))
            outfile.close()
            d_nodes[k] = d_events

            # Leave the top k features, k = X.shape[1] / 2
            model = SelectFromModel(rgs, threshold='median', prefit=True)
            X = model.transform(X)
            new_names = [names[i] for i in range(len(names)) if importances[i] >= np.median(importances)]     # transform names
            names = new_names
            print "X shape n y shape: ", X.shape, y.shape


    print d_nodes
    output_file = open('terasort_ImR_round3.json', 'w').write(json.dumps(d_nodes, sort_keys=True))

    input_file = open('C:\Users\lyr\Desktop\HardwareEvents\ExpRanker\RES\\0605terasort-spark\\terasort_ImR_round3.json', 'r')
    data = json.load(input_file)

