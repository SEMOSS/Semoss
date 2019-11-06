import pandas as pd

def validator(frameWrapper, rule):
	df = frameWrapper.cache['data']
	currCol = rule['col']
	currRule = rule['rule']
	values = rule['options']
	totLength = len(df[currCol])
	totErrors = totLength - sum(df[currCol].isin(values))
	totCorrect = totLength - totErrors
	toPaint = ', '.join(['"%s"' % w for w in values])
	# Create DataFrame 
	data = {'Columns': [currCol], 'Errors':[totErrors], 'Valid': [totCorrect], 'Total': [totLength], 'Rules': [currRule], 'Description': [''], 'toColor': [toPaint]} 
	ruleDf = pd.DataFrame(data) 
	return ruleDf 