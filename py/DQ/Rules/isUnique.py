import numpy as np
import pandas as pd

def duplicates(frameWrapper, rule):
	df = frameWrapper.cache['data']
	currCol = rule['col']
	currRule = rule['rule']
	temp = df[currCol]
	totLength = len(temp)
	totErrors = sum(np.array(temp.duplicated(keep=False)) + 0)
	totCorrect = totLength - totErrors
	toPaint = temp[temp.duplicated()]
	toPaint = list(set(toPaint))
	# Create DataFrame 
	toPaint = ', '.join(['"%s"' % w for w in toPaint])
	data = {'Columns': [currCol], 'Errors':[totErrors], 'Valid': [totCorrect], 'Total': [totLength], 'Rules': [currRule], 'Description': [''], 'toColor': [toPaint]} 
	ruleDf = pd.DataFrame(data) 
	return ruleDf 
