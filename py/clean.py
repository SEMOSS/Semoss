from fuzzywuzzy import fuzz
import string
import pandas as pd
import random
import datetime
import math
# from annoy import AnnoyIndex
import numpy as np
from pandas.api.types import is_numeric_dtype
from pandas.api.types import is_integer_dtype
from pandas.api.types import is_datetime64_dtype
import urllib.parse
from pyjarowinkler import distance
# import numba as nb
# UTILITY Methods
# from importutil import reload
# sys.path.append('c:\\users\\pkapaleeswaran\\workspacej3\\py')


class PyFrame:

    def __init__(self, cache):
        self.cache = cache

    @classmethod
    def makefm(cls, frame):
        cache = {}
        cache['data'] = frame
        cache['version'] = 0
        cache['low_version'] = 0
        cache[0] = frame
        cache['meta'] = frame.dtypes.to_dict()
        return cls(cache)

    @classmethod
    def makefm_csv(cls, fileName):
        frame = pd.read_csv(fileName)
        cache = {}
        cache['data'] = frame
        cache['version'] = 0
        cache['low_version'] = 0
        cache[0] = frame
        return cls(cache)

    def id_generator(self, size=6, chars=string.ascii_uppercase + string.digits):
        return ''.join(random.choice(chars) for _ in range(size))

    def makeCopy(this):
        version = 0
        if (version in this.cache):
            if ('high_version' in this.cache):
                version = this.cache['high_version']
            else:
                version = this.cache['version']
            version = version + 1

        this.cache[version] = this.cache['data'].copy()
        this.cache['version'] = version
        this.cache['high_version'] = version

    # falls back to the last version
    def fallback(this):
        version = this.cache['version']
        low_version = this.cache['low_version']
        if (version in this.cache and version > low_version):
            this.cache['high_version'] = version
            version = version - 1
            this.cache['version'] = version
            this.cache['data'] = this.cache[version]
        else:
            print('cant fall back. In the latest')

    def calcRatio(self, actual_col, predicted_col):
        result = []
        # actual_col = actual_col.unique
        # predicted_col = predicted_col.unique
        for x in actual_col:
            for y in predicted_col:
                ratio = fuzz.ratio(x, y)
                ratio = 1 - (ratio / 100)
                if (ratio != 0):
                    data = [x, y, ratio]
                    result.append(data)
        result = pd.DataFrame(result)
        return result

    def match(this, actual_col, predicted_col):
        # cache = this.cache
        # key_name = actual_col + "_" + predicted_col
        # if (not (key_name in cache)):
        # print('building cache', key_name)
        # daFrame = cache['data']
        # var_name = this.calcRatio(daFrame[actual_col].unique(), daFrame[predicted_col].unique())
        # var_name.columns = ['col1', 'col2', 'distance']
        # cache[key_name] = var_name
        # var_name = cache[key_name]
        # print(var_name.head())
        # seems like we dont need that right now
        # output = var_name[(var_name[2] > threshold) & (var_name[2] != 100)]

        result = []
        frame = this.cache['data']
        actual_col_values = frame[actual_col].unique()
        predicted_col_values = frame[predicted_col].unique()
        for x in actual_col_values:
            if x is np.nan:
                continue
            for y in predicted_col_values:
                if y is np.nan:
                    continue
                # ratio = fuzz.WRatio(x, y)
                ratio = distance.get_jaro_distance(
                    x, y, winkler=True, scaling=0.1)
                # ratio is 1 when values are the same
                # so we want to do the inverse
                ratio = 1 - ratio
                if ratio != 0:
                    data = [x, y, ratio]
                    result.append(data)
        result = pd.DataFrame(result, columns=['col1', 'col2', 'distance'])
        return result

    def self_match(this, actual_col):
        result = []
        frame = this.cache['data']
        # print ("Finished math")
        actual_col_values = frame[actual_col].value_counts().index.values
        for index_loop_one, x in enumerate(actual_col_values):
            if x is np.nan or x is None:
                continue
            for y in actual_col_values[(index_loop_one+1):]:
                if y is np.nan or y is None:
                    continue
                # print(x + "<<>>" + y)
                try:
                    ratio = distance.get_jaro_distance(
                        x, y, winkler=True, scaling=0.1)
                    # ratio is 1 when values are the same
                    # so we want to do the inverse
                    ratio = 1 - ratio
                    if ratio != 0:
                        data = [x, y, ratio]
                        result.append(data)
                except:
                    pass
        result = pd.DataFrame(result, columns=['col1', 'col2', 'distance'])
        return result

    def merge_match_results(this, col_name, link_frame):
        # link_frame contains col1, col2 where col1 contains instances
        # that need to be changed and col2 contains the new values for
        # those instances
        frame = this.cache['data']
        old_values = link_frame['col1']
        for index, old_value in enumerate(old_values):
            new_value = link_frame.iloc[index]['col2']
            frame.replace({col_name: old_value}, {col_name: new_value},
                          regex=False, inplace=True)
        return frame

    def drop_col(this, col_name, inplace=True):
        frame = this.cache['data']
        if not inplace:
            this.makeCopy()
        frame.drop(col_name, axis=1, inplace=True)
        this.cache['data'] = frame

    def split(this, col_name, delim, inplace=True):
        frame = this.cache['data']
        if not inplace:
            this.makeCopy()
        col_to_split = frame[col_name]
        splitcol = col_to_split.str.split(delim, expand=True)
        for len in splitcol:
            frame[col_name + '_' + str(len)] = splitcol[len]
        this.cache['data'] = frame
        return frame

    def replace_val(this, col_name, old_value, new_value, regx=True, inplace=True):
        frame = this.cache['data']
        if not inplace:
            this.makeCopy()
        nf = frame.replace({col_name: old_value}, {
                           col_name: new_value}, regex=regx, inplace=inplace)
        print('replacing inplace')

    # this.cache['data'] = nf

    def replace_val2(this, col_name, cur_value, new_col, new_value, regx=True, inplace=True):
        frame = this.cache['data']
        if not inplace:
            this.makeCopy()
        nf = frame.replace({col_name: cur_value}, {
                           new_col: new_value}, regex=regx, inplace=inplace)
        print('replacing inplace')

    def regex_replace_val(this, col_name, old_value, new_value, inplace=True):
        if not inplace:
            this.makeCopy()
        frame = this.cache['data']
        # if new_value and type of column col_name are both numeric, then convert
        # the column to str, do replace, and convert back to numeric
        # (so we have the same functionality of doing regex on nums like there is on R with gsub)
        is_timestamp = is_datetime64_dtype(frame[col_name])

        is_new_value_a_int = is_integer_dtype(type(new_value))
        is_col_a_int = is_integer_dtype(frame[col_name])
        both_integer = True if is_new_value_a_int and is_col_a_int else False

        is_new_value_a_num = is_numeric_dtype(type(new_value))
        is_col_a_num = is_numeric_dtype(frame[col_name])
        both_numeric = True if is_new_value_a_num and is_col_a_num else False

        if is_timestamp:
            frame[col_name] = frame[col_name].dt.strftime('%Y-%m-%d %H:%M:%s')
        elif both_integer:
            frame[col_name] = frame[col_name].astype(str)
        elif both_numeric:
            frame[col_name] = frame[col_name].astype(str)

        frame.replace({col_name: str(old_value)}, {
                      col_name: str(new_value)}, regex=True, inplace=inplace)

        if is_timestamp:
            frame[col_name] = pd.to_datetime(
                frame[col_name], format=['%Y-%m-%d %H:%M:%S'])
        elif both_integer:
            frame[col_name] = frame[col_name].astype('int64')
        elif both_numeric:
            frame[col_name] = pd.to_numeric(frame[col_name])
        this.cache['data'] = frame

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
# def buildnn(this, trees=10, type='euclidean'):
# frame = this.cache['data']
# cols = len(frame.columns) - 1
# #t = AnnoyIndex()
# for i, row in frame:
# t.add_item(i, row[1:])
# this.cache['nn'] = t

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
        if (newcol == 'assign'):
            this.replace_val(col_name, '[a-zA-Z]+', '')
        else:
            this.dupecol(col_name, newcol)
            this.replace_val(newcol, '[a-zA-Z]+', '')

    def extract_alpha(this, col_name, newcol='assign', inplace=True):
        if (newcol == 'assign'):
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
        frame = pd.melt(frame, id_vars=idcols, value_vars=valcols)
        print(frame.columns.values)
        this.cache['data'] = frame
        return frame

    def split_unpivot(this, col_name, delim, var='variable', inplace=True):
        frame = this.cache['data']
        if not inplace:
            this.makeCopy()
        col_to_split = frame[col_name]
        splitcol = col_to_split.str.split(delim, expand=True)
        valcols = []
        for len in splitcol:
            valcolname = col_name + '_' + str(len)
            frame[valcolname] = splitcol[len]
            valcols.append(valcolname)
        # now unpivot these columns
        # drop the col that is about to be replaced
        this.drop_col(col_name)
        print('dropped col')
        idcols = list(set(list(frame.columns.values)) - set(valcols))
        # reassign
        frame = this.cache['data']
        # change the name of variable column if one exists with same name
        if var in frame.columns:
            var = this.id_generator(4)
        output = pd.melt(frame, id_vars=idcols, value_vars=valcols, var_name=var, value_name=col_name).dropna(
            subset=[col_name])
        print('Dropped')
        # and finally replace
        # need a way to drop the none
        this.cache['data'] = output
        # this.cache['data'] = output[output[col_name] != 'None']
        this.drop_col(var)
        return output

    def rename_col(this, col_name, new_col_name, inplace=True):
        frame = this.cache['data']
        if not inplace:
            this.makeCopy()
        frame = frame.rename(index=str, columns={col_name: new_col_name})
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
        # while new_col in frame.columns:
        # count = count + 1
        # new_col = col_name + '_' + count
        print(new_col)
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
    # agg function = how to aggregate
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
            keys = keys[0:25]
            values = frame[col_name].value_counts().tolist()
            values = values[0:25]
            num_null = frame[col_name].isnull().sum()
            # hard coding at the moment to only return
            # top 25 results
            if num_null > 0:
                if len(values) < 25 or min(values) < num_null:
                    added = False
                    for index, val in enumerate(values):
                        if num_null > val:
                            keys.insert(index, 'null')
                            values.insert(index, num_null)
                            added = True
                            break
                    if not added:
                        keys.append('null')
                        values.append(num_null)
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
        this.cache['data'] = frame

    def stat(this, col_name):
        frame = this.cache['data']
        return pd.DataFrame(frame[col_name].describe(include=np.number)).to_dict(orient='index')

    # return  frame[col_name].describe(include=np.number).reset_index().values.tolist()

    def sum_median(this, col_name):
        frame = this.cache['data']
        sum = frame[col_name].sum()
        median = frame[col_name].median()
        return [sum, median]

    def collapse(this, group_col, agg_col, delim: object = '', other_cols='assign'):
        frame = this.cache['data']
        res_frame = frame.groupby(group_col)[agg_col].apply(
            lambda x: delim.join(x.astype(str))).reset_index()
        res_frame = res_frame.rename(columns={agg_col: 'Collapsed_' + agg_col})
        # res_frame = pd.DataFrame(res_frame.index, res_frame.values).reset_index()
        # res_frame.columns = ['COLLAPSE_' + agg_col, group_col]
        # df.columns = ['COLLAPSE' + agg_col, group_col]
        if other_cols != 'assign':
            merger = frame[other_cols]
            print(merger.columns)
            print(res_frame.columns)
            print(group_col)
        res_frame = pd.DataFrame(res_frame).merge(merger, on=group_col)
        this.cache['data'] = res_frame
        return res_frame

    def join(this, new_col, input_cols, delim=''):
        # Will need to join columns as strings (since you can't join non-str cols and it doesn't make sense)
        frame = this.cache['data']
        frame[new_col] = frame[input_cols].apply(
            lambda x: delim.join(x.astype(str)), axis=1)
        this.cache['data'] = frame
        return frame

    def date_add_value(this, date_column, output_column, unit_of_measure, value):
        frame = this.cache['data']
        if unit_of_measure == "day":
            frame[output_column] = frame[date_column] + pd.Timedelta(day=value)
        elif unit_of_measure == "week":
            frame[output_column] = frame[date_column] + pd.Timedelta(W=value)
        elif unit_of_measure == "month":
            frame[output_column] = frame[date_column] + pd.Timedelta(M=value)
        elif unit_of_measure == "year":
            frame[output_column] = frame[date_column] + pd.Timedelta(Y=value)
        return frame

    def date_difference_columns(this, date_column1, date_column2, unit_of_measure, output_column):
        frame = this.cache['data']
        # perform the difference operation
        frame[output_column] = frame[date_column1] - frame[date_column2]
        # get the correct unit from the object
        # add rounding based on unit of measure
        if unit_of_measure == "day":
            frame[output_column] = frame[output_column] / \
                np.timedelta64(1, 'D')
        elif unit_of_measure == "week":
            frame[output_column] = round(
                frame[output_column] / np.timedelta64(1, 'W'), 2)
        elif unit_of_measure == "month":
            frame[output_column] = round(
                frame[output_column] / np.timedelta64(1, 'M'))
        elif unit_of_measure == "year":
            frame[output_column] = round(
                frame[output_column] / np.timedelta64(1, 'Y'))
        return frame

    def date_difference_constant(this, date_column, date_constant, direction_bool, unit_of_measure, output_column):
        # direction_bool = true -> then date_column - date_constant
        # direction_bool = false -> then date_constant - date_column
        frame = this.cache['data']
        # perform the difference operation
        if direction_bool:
            frame[output_column] = frame[date_column] - \
                pd.to_datetime(date_constant)
        else:
            frame[output_column] = pd.to_datetime(
                date_constant) - frame[date_column]
        # get the correct unit from the object
        # add rounding based on unit of measure
        if unit_of_measure == "day":
            frame[output_column] = frame[output_column] / \
                np.timedelta64(1, 'D')
        elif unit_of_measure == "week":
            frame[output_column] = round(
                frame[output_column] / np.timedelta64(1, 'W'), 2)
        elif unit_of_measure == "month":
            frame[output_column] = round(
                frame[output_column] / np.timedelta64(1, 'M'))
        elif unit_of_measure == "year":
            frame[output_column] = round(
                frame[output_column] / np.timedelta64(1, 'Y'))
        return frame

    def add_date_col(this, new_col_name, inplace=True):
        frame = this.cache['data']
        if not inplace:
            this.makeCopy()
        frame[new_col_name] = pd.Series(
            [pd.to_datetime('today').date()] * len(frame))
        frame[new_col_name] = pd.to_datetime(frame[new_col_name])
        this.cache['data'] = frame
        return frame

    def add_datetime_col(this, new_col_name, inplace=True):
        frame = this.cache['data']
        if not inplace:
            this.makeCopy()
        frame[new_col_name] = pd.Series([pd.to_datetime('today')] * len(frame))
        frame[new_col_name] = pd.to_datetime(frame[new_col_name])
        this.cache['data'] = frame
        return frame

    # average across columns
    def avg_cols(this, cols_to_avg, new_col):
        frame = this.cache['data']
        frame[new_col] = frame[cols_to_avg].mean(axis=1)
        this.cache['data'] = frame

    def to_pct(this, src_col, new_col, sig_figs, by_100):
        frame = this.cache['data']
        rounded = round(frame[src_col], sig_figs)
        multiplier = 100 if by_100 else 1
        frame[new_col] = (multiplier * rounded).astype(str) + '%'
        this.cache['data'] = frame

    def to_pct_l(val, sig_figs, by_100):
        print('PCT')
        rounded = round(val, sig_figs)
        multiplier = 100 if by_100 else 1
        return str(multiplier * rounded) + '%'

    def col_division(this, numerator, denominator, new_col):
        frame = this.cache['data']
        # Only attempt to divide if both columns are numeric
        is_numerator_numeric = is_numeric_dtype(frame[numerator].dtype)
        is_denominator_numeric = is_numeric_dtype(frame[denominator].dtype)
        if is_numerator_numeric and is_denominator_numeric:
            frame[new_col] = frame[numerator] / frame[denominator]
        this.cache['data'] = frame

    def col_division_l(numerator, denominator):
        # Only attempt to divide if both columns are numeric
        is_numerator_numeric = str(numerator).isnumeric()
        is_denominator_numeric = str(denominator).isnumeric()
        if is_numerator_numeric and is_denominator_numeric and denominator != 0:
            return numerator / denominator
        else:
            return 0

    def string_trim_col(this, col, new_col, keep_or_remove, where, num_chars):
        frame = this.cache['data']
        if keep_or_remove == 'keep':
            if where == 'left':
                frame[new_col] = frame[col].str.slice(stop=num_chars)
            elif where == 'right':
                frame[new_col] = frame[col].str.slice(start=-num_chars)
        elif keep_or_remove == 'remove':
            if where == 'left':
                frame[new_col] = frame[col].str.slice(start=num_chars)
            elif where == 'right':
                frame[new_col] = frame[col].str.slice(stop=-num_chars)
        this.cache['data'] = frame

    def decode_uri(this, col_name):
        frame = this.cache['data']
    # Only handle str's, can't encode non-str's
        frame[col_name] = frame[col_name].apply(
            lambda row: urllib.parse.unquote_plus(
                row) if isinstance(row, str) else row
        )
        this.cache['data'] = frame

    def encode_uri(this, col_name):
        frame = this.cache['data']
    # Only handle str's, can't encode non-str's
        frame[col_name] = frame[col_name].apply(
            lambda row: urllib.parse.quote(
                row) if isinstance(row, str) else row
        )
        this.cache['data'] = frame

    def sig_fig_round(this, num, sf=2):
        # Called from discretize_column() to do significant figure rounding
        return round(num, sf - int(math.floor(math.log10(abs(num)))) - 1) if num != 0 else 0

    def discretize_column(this, col, new_col, breaks=None, labels=False, num_digits=None):
        frame = this.cache['data']

        # Nothing is passed, auto discretize
        if breaks is None and labels is False and num_digits is None:
            bins = np.histogram_bin_edges(frame[col])
            frame[new_col] = pd.cut(
                frame[col], bins, include_lowest=True).astype(str)

        # Just num_digits passed, round to num_digits sig figs
        elif breaks is None and labels is False and num_digits is not None:
            bins = np.histogram_bin_edges(frame[col])
            # TODO Rounding problem: something like 1,230 (3 sig figs) can round to 1,229.999...
            # TODO if a number like 1,230 is our max and we're working with 2 sig figs,
            # our max will be 1,200 and the number 1,230 will be binned as np.nan
            sig_fig_rounded_bins = list(map(this.sig_fig_round, bins))
            frame[new_col] = pd.cut(
                frame[col], sig_fig_rounded_bins, include_lowest=True).astype(str)

        # Breaks are passed, labels automatically handled if not passed
        elif breaks is not None:
            if isinstance(breaks, list):
                # Make sure breaks are increasing
                breaks = sorted(breaks)
            elif isinstance(breaks, str):
                # Parse from R form of "0:5*.5" to python equivalent (e.g. c(0:5*.5) in R to python)
                breaks_split = breaks.split('*')
                breaks_start_and_num_iterations = breaks_split[0].split(':')
                breaks_start = int(breaks_start_and_num_iterations[0])
                breaks_num_iterations = int(breaks_start_and_num_iterations[1])
                breaks_step = float(breaks_split[1])
                breaks_stop = (breaks_num_iterations *
                               breaks_step) + breaks_step
                breaks = np.arange(breaks_start, breaks_stop, breaks_step)
            frame[new_col] = pd.cut(
                frame[col], breaks, labels=labels, include_lowest=True)

        this.cache['data'] = frame

    def column_like_old(this, col, searchstr):  # this should not be used anymore
        colindex = col
        colarr = {}
        if colindex in this.cache:
            colarr = this.cache[colindex]
            colarru = this.cache[colindex + "_u"]
        else:
            mainq = this.cache['data'][col]
            # colarr = mainq.unique().astype('str')
            # change it to string
            # colarr = colarr.astype('str')
            col_cat = mainq.astype('category')
            colarr = col_cat.cat.categories.to_numpy().astype('str')
            # colarr = colarr[colarr != None]
            # remove the none types as well
            # colarru = mainq.str.upper().unique().astype('str')
            # colarru = colarr.upper()
            colarru = np.char.upper(colarr)
            # colarru = colarru[colarru != None]
            this.cache[colindex + "_u"] = colarru
            this.cache[colindex] = colarr
            col_key = colindex + "__cat"
            this.cache[col_key] = col_cat
            col_cat_codes = col_key + "__cat.code"
            col_cat_names = col_key + "__cat.categories"
            this.cache[col_cat_codes] = this.cache[col_key].cat.codes.abs()
            this.cache[col_cat_names] = this.cache[col_key].cat.categories
        # now I can search it
        # need to find a way to handle nulls / nans
        # right now this matches everywhere
        # to match to the beginning, this should be equal to 0
        output = colarr.ravel()[np.flatnonzero(
            np.char.find(colarru, str(searchstr).upper()) != -1)]
        return list(output)

    def column_like_old2(this, col, searchstr):  # this should not be used anymore
        this.idxColFilter("", col)
        output = this.cache[col].ravel()[np.flatnonzero(
            np.char.find(this.cache[col+"__u"], str(searchstr).upper()) != -1)]
        return output

    def column_like(this, filterKey, col, searchstr):
        final_key = this.idxColFilter(filterKey, col)
        output = this.cache[final_key].ravel()[np.flatnonzero(
            np.char.find(this.cache[final_key+"__u"], str(searchstr).upper()) != -1)]
        return list(output)

    def column_not_like(this, filterKey, col, searchstr):
        final_key = this.idxColFilter(filterKey, col)
        output = this.cache[final_key].flatten()[np.flatnonzero(
            np.char.find(this.cache[final_key+"__u"], str(searchstr).upper()) == -1)]
        return list(output)

    def idxColFilter(this, filterKey, col):
        ff_key = this.idxFilter(filterKey)
        col_index = ff_key + "__" + col
        col_index_u = col_index + "__u"  # upper case
        col_key = ff_key + "__" + col + "__cat"
        col_cat_codes = col_key + "__cat.code"
        col_cat_names = col_key + "__cat.categories"
        if col_key not in this.cache:
            # create this key
            # g_key is basically the group column as categories
            cat1 = this.cache[ff_key][col].astype('category')
            this.cache[col_key] = cat1
            # print("done cat")
            this.cache[col_cat_codes] = cat1.cat.codes.abs()
            # print("done abs")
            this.cache[col_cat_names] = cat1.cat.categories
            # print("done category names")
            this.cache[col_index] = cat1.cat.categories.to_numpy().astype('str')
            # print("done col index")
            this.cache[col_index_u] = np.char.upper(this.cache[col_index])
            # print("done upper")
        # print("col indexing complete")
        return col_index

    def idxFilter(this, filterKey):
        if filterKey != "":
            id = filterKey+"__f"
            if id not in this.cache:
                var1 = eval(filterKey)
                this.cache[id] = var1
        else:
            id = "this.cache['data']__f"
            if id not in this.cache:
                this.cache[id] = this.cache['data']
        # print("filter indexing complete")
        return id

    # @nb.njit
    def runGroupy(this, filterKey, groupby_list, agg_col_list, func_list, order_list):
        # I need a filter query which will always be the same
        # I need one query for every groupby - so this be an array
        # one array for groupby
        # a parallel array for column
        # another array for the calculation
        # I could as well exec the whole thing ?
        # if the groupby is composite I need to do other operations on top of it - this is going to be tricky - but we will see
        # See if filter is available in cache .. if not make it cache[Filter value] = filter
        # if not in cache .. if the groupby is a string column - usually it is - create a category column for it
        # place it into the cache - with the name filter_columnname_cat <-- not sure that would do because the filter might be playing into it
        # see if the index exists if not create and place it into cache filter_columnname_cat_idx = cat.codes
        # see if the cache has value for filter_groupcolumn_cat_idx_column_function if not make it - repeat this for every combination there is
        # Once all is done
        # you need [list(filter_groupcolumn_categories.unique()), filter_groupcolumn_cat_idx_column_function]
        # col__cat - category type column
        # col - numpy string column
        # col__cat__cat.code - category code
        # col__cat__cat.categories - actual actegories
        # col__u = upper case of the column
        # filter__f - filter key
        import numpy_groupies as npg
        retOutput = 1
        # ff_key = this.idxFilter(filterKey) # final filter key
        filtered_data = ""
        # collocating filter
        if filterKey != "":
            ff_key = filterKey+"__f"
            if ff_key not in this.cache:
                filtered_data = eval(filterKey)
                this.cache[ff_key] = filtered_data
        else:
            ff_key = "this.cache['data']__f"
            if ff_key not in this.cache:
                this.cache[ff_key] = this.cache['data']
                filtered_data = this.cache['data']

        for g, a, f in zip(groupby_list, agg_col_list, func_list):
            g_key = ff_key + "__" + g + "__cat"
            col = g
            cat_codes = g_key + "__cat.code"
            cat_names = g_key + "__cat.categories"

            if g_key not in this.cache:
                # create this key
                # g_key is basically the group column as categories
                # Creating this as a separate function since I will need it for other things as well
                # this.cache[g_key] = this.cache[filterKey][g].astype('category')
                # this.cache[cat_codes] = this.cache[g_key].cat.codes.abs()
                # this.cache[cat_names] = this.cache[g_key].cat.categories
                this.idxColFilter(filterKey, g)

            # this is what we are aggregating i.e. the column. We are indexing, because there could be a filter associated with it
            a_key = ff_key + "__" + a
            a_col = ""
            if a_key not in this.cache:
                a_col = this.cache[ff_key][a]
                this.cache[a_key] = a_col
            else:
                a_col = this.cache[a_key]
            # time to run the npg now
            out_key = g_key + "__" + a + "__" + f
            out = npg.aggregate(this.cache[cat_codes], a_col, f)
            this.cache[out_key] = out
            retOutput = [this.cache[cat_names], this.cache[out_key]]
        return retOutput[:2]
        # and then I need to do order logic

    def clearCache(this):
        imm = ['data', 'version', 'low_version', 0]
        del_keys = list()
        for key in this.cache.keys():
            if key not in imm:
                del_keys.append(key)

        for del_key in del_keys:
            del this.cache[del_key]

    def hasFrameChanged(this):
        imm = ['data', 'version', 'low_version', 0]
        cur_meta = []
        if 'meta' not in this.cache:
            cur_meta = this.cache['data'].dtypes.to_dict()
            this.cache['meta'] = cur_meta
            return "False"
        cur_meta = this.cache['data'].dtypes.to_dict()
        prev_meta = this.cache['meta']
        prev_keys = set(this.cache['meta'].keys())
        cur_keys = set(cur_meta.keys())

    # first just compare keys
        added = prev_keys - cur_keys
        removed = cur_keys - prev_keys
        shared_keys = prev_keys.intersection(cur_keys)
        # print(shared_keys)
        if (len(added) > 0 or len(removed) > 0):
            cur_meta = this.cache['data'].dtypes.to_dict()
            this.cache['meta'] = cur_meta
            return "True"
        # if not check the values next
        if (len(shared_keys) > 0):
            modified = {o: (cur_meta[o], prev_meta[o])
                        for o in shared_keys if cur_meta[o] != prev_meta[o]}
            if (len(modified) > 0):
                cur_meta = this.cache['data'].dtypes.to_dict()
                this.cache['meta'] = cur_meta
                return "True"
    # same = set(o for o in shared_keys if d1[o] == d2[o])
        return "False"

    def get_categorical_values(this):
        import numpy as np
        frame = this.cache['data']
        cols = frame.columns
        num_cols = frame._get_numeric_data().columns
        col_indices = list(set(cols) - set(num_cols))
        output = {}
        for value in col_indices:
            col_name = value
            col_items = np.unique(frame[value].astype(str))
            col_values = '('
            count = 0
            # add a check for number of items
            for item in col_items:
                if (item != 'nan'):
                    if (count != 0):
                        col_values = col_values + ", '" + item + "'"
                    else:
                        col_values = col_values + "'" + item + "'"
                    count = count + 1
                    if (count > 20):
                        break
            col_values = col_values + ')'
            output.update({col_name: col_values})
        return output
