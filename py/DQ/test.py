import time
import missionControl
import pandas as pandas_import_var
import smssutil
from clean import PyFrame
import clean
import numpy as np
import pandas as pd
import gc as gc
import pandas as pd
import string
import random
import datetime
from annoy import AnnoyIndex
import sys
sys.path.append("C:/Users/SEMOSS/workspace/Semoss/py")

# Load df and create frame wrapper
df = pandas_import_var.read_csv(
    'C:/Users/SEMOSS/workspace/Semoss/InsightCache/ah8aNY2.csv', sep=',', encoding='utf-8')
dfw = PyFrame.makefm(df)

# test data quality
sys.path.append("C:/Users/SEMOSS/workspace/Semoss/py/DQ")
results = pd.DataFrame(
    columns=['Columns', 'Errors', 'Valid', 'Total', 'Rules', 'Description', 'toColor'])
rule = {'rule': 'Duplicates', 'col': 'Genre'}
rule = {'rule': 'Blanks/Nulls/NAs', 'col': 'Studio'}
x = missionControl.missionControl(dfw, rule, results)
print(x)

# pretty print list
# from pprint import pprint
# duplicates

####
start = time.time()
test = [str(item) for item in FRAME845444w.cache['data']]
done = time.time()
elapsed = done - start
print(elapsed)
