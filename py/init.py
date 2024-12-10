import numpy as np
import pandas as pd
import gc as gc
import sys

# workaround for issue with matplotlib.pyplot.plot() not working with python
# 3.7.3; sys.argv is assumed to have length > 0
# see https://github.com/ninia/jep/issues/187 for details
# do it only if the version is there and it is 3.6
major = sys.version_info[0]
minor = sys.version_info[1]
if major >= 3 and minor >= 7:
    sys.argv.append("")
import string
import random
import datetime
