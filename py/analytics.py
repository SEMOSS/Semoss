from fuzzywuzzy import fuzz
import string
import pandas as pd
import random
import datetime
from annoy import AnnoyIndex
import numpy as np
from pandas.api.types import is_numeric_dtype
from sklearn.linear_model import LinearRegression

### UTILITY Methods
# from importutil import reload
#sys.path.append('c:\\users\\pkapaleeswaran\\workspacej3\\py')

class PyFrame:

	x = 8
	
	def __init__(self, cache):
	    self.cache = cache

	@classmethod
	def makefm(cls, frame):
		cache = {}
		cache['data']=frame
		cache['version'] = 0
		cache['low_version'] = 0
		cache[0] = frame
		return cls(cache)
		
	@classmethod
	def makefm_csv(cls, fileName):
		frame = pd.read_csv(fileName)
		cache = {}
		cache['data']=frame
		cache['version'] = 0
		cache['low_version'] = 0
		cache[0] = frame
		return cls(cache)


	def id_generator(self, size=6, chars=string.ascii_uppercase + string.digits):
		return ''.join(random.choice(chars) for _ in range(size))

	def makeCopy(this):
		version = 0
		if(version in this.cache):
			if('high_version' in this.cache):
				version = this.cache['high_version']
			else:
				version = this.cache['version']
			version = version+1

		this.cache[version] = this.cache['data'].copy()
		this.cache['version'] = version
		this.cache['high_version'] = version
		

	# falls back to the last version
	def fallback(this):	
		version = this.cache['version']
		low_version = this.cache['low_version']
		if(version in this.cache and version > low_version):
			this.cache['high_version'] = version
			version = version - 1
			this.cache['version'] = version
			this.cache['data'] = this.cache[version]
		else:
			print ('cant fall back. In the latest')

	def get_correlation(this, columns='assign', inplace=True):
		print (columns)
		output = 'to be assigned'
		if columns == 'assign':
			output = this.cache['data'].corr().unstack().reset_index()
		else:
			print ('in the else')
			output = this.cache['data'][columns].corr().unstack().reset_index()
		output.columns = ['Column_Header_X', 'Column_Header_Y', 'Correlation']
		return output
		

	def get_regression(this, target, source):
		model = LinearRegression(fit_intercept=True)
		frame = this.cache['data']
		target = frame[target]
		if len(source) == 1:
			#print ('In Single Source Column')
			temp = source[0]
			#print('Recasting', temp)
			source =  frame[temp][:, np.newaxis]
		else:
			source = frame[source]
		model.fit(source, target)
		predict = list(model.predict(source))
		row_id = list(range(0, len(predict)))
		target = list(target)
		#print(len(row_id), len(target), len(predict))
		result = pd.DataFrame({'row':row_id, 'actual':target, 'fitted':predict})
		result.columns = ['ROW_ID', 'Actual', 'Fitted']
		return result