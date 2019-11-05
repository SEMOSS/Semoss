import numpy as np
import pandas as pd
import gc as gc
from fuzzywuzzy import fuzz
import pandas as pd
import string
import random
import datetime
from annoy import AnnoyIndex
import sys
sys.path.append("C:/Users/SEMOSS/workspace/Semoss/py")
import clean
from clean import PyFrame
import smssutil

### Load df and create frame wrapper
import pandas as pandas_import_var
df = pandas_import_var.read_csv('C:/Users/SEMOSS/workspace/Semoss/InsightCache/ah8aNY2.csv', sep=',', encoding='utf-8')
dfw = PyFrame.makefm(df)

# test data quality
import sys
sys.path.append("C:/Users/SEMOSS/workspace/Semoss/py/DQ")
import missionControl
results = pd.DataFrame(columns=['Columns', 'Errors', 'Valid', 'Total', 'Rules', 'Description', 'toColor'])
rule={'rule':'Duplicates', 'col': 'Genre'}
rule={'rule':'Blanks/Nulls/NAs', 'col': 'Studio'}
x = missionControl.missionControl(dfw, rule, results)
print(x)

## pretty print list
#from pprint import pprint
##### duplicates 

####
import time
start = time.time()
test = [str(item) for item in FRAME845444w.cache['data']]
done = time.time()
elapsed = done - start
print(elapsed)








