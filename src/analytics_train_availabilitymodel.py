from sklearn import svm, linear_model
from sklearn.metrics import roc_curve
from sklearn.cross_validation import train_test_split
import matplotlib.pyplot as plt
import seaborn

import numpy as np
import csv
import random
from logger import Logger
import sys

random.seed(1234)
np.random.seed(5678)
logger = Logger(sys.stdout)

logger.log('Reading data...')

left = []
stayed = []
with open('availabilitysample-linkedin.csv', 'r') as csvfile:
    csvreader = csv.reader(csvfile)
    for row in csvreader:
        row = [int(field) for field in row]
        left.append(row+[0])
        if row[0] > 0:
            row[0] = random.randint(0, row[0]-1)
            stayed.append(row+[1])
            
logger.log('done.\n')
logger.log('Preparing data...')

while len(left) > len(stayed):
    row = random.choice(left)[:]
    if row[0] > 0:
        row[0] = random.randint(0, row[0]-1)
        row[-1] = 1
        stayed.append(row)

data = left+stayed
del left, stayed
random.shuffle(data)
y = np.array([r[-1] for r in data])
x = np.array([r[:-1] for r in data])
del data
xtrain, xtest, ytrain, ytest = train_test_split(x, y, test_size=0.5)
del x, y

logger.log('done.\n')
logger.log('Fitting model...')

# clf = svm.SVC(gamma=0.001, C=100.)
clf = linear_model.LogisticRegression()
clf.fit(xtrain, ytrain)

logger.log('done.\n')

logger.log('Testing model...')
scores = clf.decision_function(xtest)
fpr, tpr, _ = roc_curve(ytest, scores)
plt.plot(fpr, tpr)
plt.plot([0, 1], [0, 1], 'k--')
plt.xlabel('False Positive Rate')
plt.xlim(0, 1)
plt.ylabel('True Positive Rate')
plt.ylim(0, 1)
plt.show()


# nsuccess = np.sum(clf.predict(xtest) == ytest)
# np.random.shuffle(ytest)
# nsuccess2 = np.sum(clf.predict(xtest) == ytest)

logger.log('done.\n')
# logger.log('Success rate: {0:f}\n'.format(nsuccess/len(ytest)))
# logger.log('Success rate after scrambling: {0:f}\n' \
#            .format(nsuccess2/len(ytest)))


