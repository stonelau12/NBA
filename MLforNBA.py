import pickle
import numpy as np
from sklearn import linear_model
import matplotlib.pyplot as plt
import pandas as pd

BKNAME = 'F:\\mongodb\\nbascores.pkl'


scorePerHQ = np.array(pickle.load(open(BKNAME, 'rb')))
result = []
halfGap = []
#Q1 predication.
for i in range(4):
    XTrain = scorePerHQ[:10000, i*2].reshape(-1, 1)
    yTrain = scorePerHQ[:10000, i*2+1]
    XTest = scorePerHQ[10000:, i*2].reshape(-1, 1)
    yTest = scorePerHQ[10000:, i*2+1]

    regr = linear_model.LinearRegression()
    regr.fit(XTrain, yTrain)
    pred = regr.predict(XTest)
    result.extend(pred-yTest)
    halfGap.extend(XTest-yTest)
    print('Score for %s quarter is %s' % (i+1, regr.score(XTest, yTest)))

gap = pd.Series(result)
hg = pd.Series(halfGap)

print("MAX:%s, MIN:%s, Mode:%s, Median:%s, Mean: %s" % (gap.max(), gap.min(), gap.mad,
                                                       gap.median(), gap.mean()))
print("MAX:%s, MIN:%s, Mode:%s, Median:%s, Mean: %s" % (hg.max(), hg.min(), hg.mode(),
                                                        hg.median(), hg.mean()))


