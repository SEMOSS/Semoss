from fuzzywuzzy import fuzz
import string
import pandas as pd
import random
import datetime
from annoy import AnnoyIndex
import numpy as np
from pandas.api.types import is_numeric_dtype

### UTILITY Methods
# from importutil import reload
#sys.path.append('c:\\users\\pkapaleeswaran\\workspacej3\\py')

class PyFrame:

	x = 7
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

	def calcRatio(self, actual_col, predicted_col):
		result = []
		#actual_col = actual_col.unique
		#predicted_col = predicted_col.unique
		for x in actual_col:
			for y in predicted_col:
				ratio = fuzz.ratio(x,y)
				ratio = 1 - (ratio / 100)
				if(ratio != 0):
					data = [x,y,ratio]
					result.append(data)
		result = pd.DataFrame(result)
		return result
		

	def match(this, actual_col, predicted_col):	
		cache = this.cache
		key_name = actual_col + "_" + predicted_col
		if( not(key_name in cache)):
			print ('building cache', key_name)
			daFrame = cache['data']
			var_name = this.calcRatio(daFrame[actual_col].unique(), daFrame[predicted_col].unique())
			var_name.columns = ['col1','col2','distance']
			cache[key_name] = var_name
		var_name = cache[key_name]
		#print(var_name.head())
		# seems like we dont need that right now
		#output = var_name[(var_name[2] > threshold) & (var_name[2] != 100)]
		return var_name

	def drop_col(this, col_name, inplace=True):
		frame = this.cache['data']
		if not inplace:
			this.makeCopy()
		frame.drop(col_name, axis =1, inplace=True)
		this.cache['data'] = frame
			
	def split(this, col_name, delim, inplace=True):
		frame = this.cache['data']
		if not inplace:
			this.makeCopy()
		col_to_split = frame[col_name]
		splitcol = col_to_split.str.split(delim, expand = True)
		for len in splitcol:
			frame[col_name + '_' + str(len)] = splitcol[len]
		this.cache['data'] = frame
		return frame

		
	def replace_val(this, col_name, old_value, new_value, regx=True, inplace=True):
		frame = this.cache['data']
		if not inplace:
			this.makeCopy()
		nf = frame.replace({col_name: old_value}, {col_name: new_value}, regex=regx, inplace=inplace)
		print('replacing inplace')
		#this.cache['data'] = nf

	def replace_val2(this, col_name, cur_value, new_col, new_value, regx= True, inplace=True):
		frame = this.cache['data']
		if not inplace:
			this.makeCopy()
		nf = frame.replace({col_name: cur_value}, {new_col: new_value}, regex=regx, inplace=inplace)
		print('replacing inplace')
		
	def upper(this, col_name, inplace=True):
		if not inplace:
			this.makeCopy()
		frame = this.cache['data']
		column = frame[col_name]
		frame[col_name] = column.str.upper()
		this.cache['data'] = frame
		
	def lower(this, col_name, inplace=True):
		if not inplace:
			this.makeCopy()
		frame = this.cache['data']
		column = frame[col_name]
		frame[col_name] = column.str.lower()
		this.cache['data'] = frame
	
	def title(this, col_name, inplace=True):
		if not inplace:
			this.makeCopy()
		frame = this.cache['data']
		column = frame[col_name]
		frame[col_name] = column.str.title()
		this.cache['data'] = frame
	
	def concat(this, col1, col2, newcol, glue='_', inplace=True):
		if not inplace:
			this.makeCopy()
		frame = this.cache['data']
		frame[newcol] = frame[col1] + glue + frame[col2]
		this.cache['data'] = frame

	def mathcat(this, operation, newcol, inplace=True):
		if not inplace:
			this.makeCopy()
		frame = this.cache['data']
		frame[newcol] = operation
		this.cache['data'] = frame

	def dupecol(this, oldcol, newcol, inplace=True):
		if not inplace:
			this.makeCopy()
		frame = this.cache['data']
		frame[newcol] = frame[oldcol]
		this.cache['data'] = frame

	# dropping row has to be done from inside java
	# newframe = mv[mv['Genre'] != 'Drama']

	# change type is also done from java
	# The actual type is sent from java 
	def change_col_type(this, col, type, inplace=True):
		frame = this.cache['data']
		if not inplace:
			this.makeCopy()
		
		frame[col] = frame[col].astype(type)
		this.cache['data'] = frame

	# Index in euclidean space tc. 
	# input is a pandas frame
	# the first column is typically the identifier
	# The remaining are the vector
	def buildnn(this, trees=10, type='euclidean'):
		frame = this.cache['data']
		cols = len(frame.columns) - 1
		t = AnnoyIndex()
		for i, row in frame:
			t.add_item(i, row[1:])
		this.cache['nn'] = t
	
	# drops non numeric data columns from the frame
	def dropalpha(this, inplace=True):
		numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
		df = this.cache['data']
		if not inplace:
			this.makeCopy()
		this.cache['data'] = df.select_dtypes(include=numerics)
		
	# drops non numeric data columns from the frame
	def dropnum(this, inplace=True):
		numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
		df = this.cache['data']
		if not inplace:
			this.makeCopy()
		this.cache['data'] = df.select_dtypes(exclude=numerics)

	def extract_num(this, col_name, newcol='assign', inplace=True):
		if(newcol == 'assign'):
			this.replace_val(col_name, '[a-zA-Z]+', '')
		else:
			this.dupecol(col_name, newcol)
			this.replace_val(newcol, '[a-zA-Z]+', '')
			

	def extract_alpha(this, col_name, newcol='assign', inplace=True):
		if(newcol == 'assign'):
			this.replace_val(col_name, '\d+', '')
		else:
			this.dupecol(col_name, newcol)
			this.replace_val(newcol, '\d+', '')

			
	def unpivot(this, valcols, idcols=['all'], inplace=True):
		frame = this.cache['data']
		if not inplace:
			this.makeCopy()
	# assimilate all the columns if the idcols = 'all'
		if idcols == ['all']:
			idcols = list(set(list(frame.columns.values)) - set(valcols))
		output = pd.melt(mv, id_vars=idcols, value_vars=valcols)
		this.cache['data'] = output
		
	def split_unpivot(this, col_name, delim, var = 'variable', inplace=True):
		frame = this.cache['data']
		if not inplace:
			this.makeCopy()
		col_to_split = frame[col_name]
		splitcol = col_to_split.str.split(delim, expand = True)
		valcols = []
		for len in splitcol:
			valcolname = col_name + '_' + str(len)
			frame[valcolname] = splitcol[len]
			valcols.append(valcolname)
		#now unpivot these columns
		# drop the col that is about to be replaced
		this.drop_col(col_name)
		print('dropped col')
		idcols = list(set(list(frame.columns.values)) - set(valcols))
		#reassign
		frame = this.cache['data']
		# change the name of variable column if one exists with same name
		if var in frame.columns:
			var = this.id_generator(4)
		output = pd.melt(frame, id_vars=idcols, value_vars=valcols, var_name=var, value_name=col_name).dropna(subset=[col_name])
		print('Dropped')
		# and finally replace
		# need a way to drop the none
		this.cache['data'] = output
		#this.cache['data'] = output[output[col_name] != 'None']
		this.drop_col(var)
		return output

	def rename_col(this, col_name, new_col_name, inplace=True):
		frame = this.cache['data']
		if not inplace:
			this.makeCopy()
		frame = frame.rename(index = str, columns={col_name : new_col_name})
		this.cache['data'] = frame
		return frame
	
		
	def countif(this, col_name, str_to_count, new_col='assign', inplace=True):
		frame = this.cache['data']
		if not inplace:
			this.makeCopy()
		# see if I need to assign a new name
		if new_col == 'assign':
			count = 0
			new_col = col_name + '_' + str_to_count + '_countif'
			#while new_col in frame.columns:
			#	count = count + 1
			#	new_col = col_name + '_' + count
		print (new_col)
		frame[new_col] = frame[col_name].str.count(str_to_count)
		this.cache['data'] = frame
	
	# val is the other columns to keep 
	def pivot(this, column_to_pivot, val='assign', inplace=True):
		frame = this.cache['data']
		if not inplace:
			this.makeCopy()
		if val == 'assign':
			frame = frame.pivot(columns=column_to_pivot)
		else:
			frame = frame.pivot(columns=column_to_pivot, values=values)
		this.cache['data'] = frame

	# val is the other columns to keep 
	# index = columns to pivot
	# Columns = Columns to show
	# Values = actual values to pivot
	#agg function = how to aggregate
	# pvt = pd.pivot_table(mv, index=['Studio', 'Genre'], columns='Nominated', values='MovieBudget' ).reset_index()
	#  pvt.columns = pvt.columns.to_series().str.join('_')
	#  pvt.reset_index()
	

		
	def is_numeric(this, col_name, inplace=True):
		frame = this.cache['data']
		return is_numeric_dtype(frame[col_name]) 
	
	def get_hist(this, col_name):
		frame = this.cache['data']	
		if this.is_numeric(col_name):
			hist = np.histogram(frame[col_name])
			return [hist[0].tolist(), hist[1].tolist()]
		else:
			keys = frame[col_name].value_counts().keys().tolist()
			values = frame[col_name].value_counts().tolist()
			return [keys, values]

	def drop_dup(this, col_name='assign', inplace=True):
		frame = this.cache['data']	
		if not inplace:
			this.makeCopy()
		if col_name == 'assign':
			frame.drop_duplicates(inplace=inplace)
		else:
			frame.drop_duplicates(col_name, inplace=inplace)

	def trim_col(this, col_name, inplace=True):
		frame = this.cache['data']	
		if not inplace:
			this.makeCopy()
		if (~this.is_numeric(col_name)):
			frame[col_name] = frame[col_name].str.strip()
	
	def stat(this, col_name):
		frame = this.cache['data']	
		return pd.DataFrame(frame[col_name].describe(include=np.number)).to_dict(orient='index')
		#return  frame[col_name].describe(include=np.number).reset_index().values.tolist()

	def sum_median(this, col_name):
		frame = this.cache['data']
		sum = frame[col_name].sum()
		median = frame[col_name].median()
		return [sum,median]

	def collapse(this, group_col, agg_col, delim:object='', other_cols='assign'):
		frame = this.cache['data']
		res_frame = frame.groupby(group_col)[agg_col].apply(lambda x: delim.join(x)).reset_index()
		res_frame = res_frame.rename(columns={agg_col:'Collapsed_' + agg_col})
		#res_frame = pd.DataFrame(res_frame.index, res_frame.values).reset_index()
		#res_frame.columns = ['COLLAPSE_' + agg_col, group_col]
		#df.columns = ['COLLAPSE' + agg_col, group_col]
		if other_cols != 'assign':
			merger = frame[other_cols]
			print(merger.columns)
			print(res_frame.columns)
			print(group_col)
			res_frame = pd.DataFrame(res_frame).merge(merger, on=group_col)
		return res_frame
		
	def join(this, input_col, new_col, delim=''):
		frame = this.cache['data']
		
		frame[new_col] = frame[input_col].apply(lambda x: delim.join(x), axis=1)
		return frame
	

		
