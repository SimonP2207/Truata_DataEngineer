import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
import config as cfg

# Read in as pandas dataframe as per task sheet
cols = ["sepal_length", "sepal_width", "petal_length", "petal_width", "class"]
pandas_dataframe = pd.read_csv(cfg.dirs['files'] / "iris.csv", names=cols)

if __name__ == '__main__':
    array = pandas_dataframe.values
    x = array[:,0:4]
    y = array[:,4]

    # Fit Logistic Regression classifier.
    logreg = LogisticRegression(C=1e5)
    logreg.fit(x, y)

    print(logreg.predict([[5.1, 3.5, 1.4, 0.2]]))
    print(logreg.predict([[6.2, 3.4, 5.4, 2.3]]))
