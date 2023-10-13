package prerna.reactor.frame;

import java.io.IOException;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.DataFrameTypeEnum;
import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.reactor.imports.ImportUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

/**
 * This class lets user profile the data in a frame. It analyzes the data in a
 * frame and then provides a comprehensive summary on Min, Max, Sum, count of
 * blanks, count of unique values, Avg and count of null values in each column.
 * 
 * The input to this routine will be the current frame and a new frame will be
 * the output with the profiling data.
 *
 */

public class FrameProfileReactor extends AbstractFrameReactor{

	private static final Logger classLogger = LogManager.getLogger(FrameProfileReactor.class);
	
	private static final String DOUBLE_UNDERSCORE = "__";
	private static final String NA = "NA";
	private static final String DOUBLE_EQUALS = "==";
	
	public FrameProfileReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.FRAME.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		classLogger.info("Starting Frame Profile execution.");
		
		organizeKeys();
		ITableDataFrame actFrame = getFrame();
		String tableName = actFrame.getName();
		
		/*
		 * Get the datatype of the columns and store it in a map for further processing.
		 */
		OwlTemporalEngineMeta metaData = actFrame.getMetaData();
		Map<String, SemossDataType> headerDatatypeMap = metaData.getHeaderToTypeMap();
		String[] actFrameHeaders = actFrame.getQsHeaders();
		
		/*
		 * Create a new frame of type ITableDataFrame. When dealing with R/Python frame we 
		 * want to use the existing user connection. For this we use the FrameFactory, as this
		 * takes in the insight which contains the user space and gives back the required frame. 
		 */
		ITableDataFrame newFrame = null;
		try {
			// Pass the frame type as grid and blank in the alias param so that you get a new frame.
			newFrame = FrameFactory.getFrame(this.insight, DataFrameTypeEnum.GRID.getTypeAsString(), "");
		}catch(Exception e) {
			throw new IllegalArgumentException("Error occurred trying to create frame of type " + DataFrameTypeEnum.GRID.getTypeAsString(), e);
		}
		String newFrameAlias = newFrame.getName();
		
		
		/*
		 * The new headers and data types that will be included in the frame. This can change
		 * afterwards and is just for the first iteration of the functionality.
		 */
		String[] newHeaders = new String[] { "TABLE_NAME", "COLUMN_NAME", "NUMBER_OF_BLANKS", 
				"NUM_OF_UNIQUE_VALUES", "MIN", "AVG", "MAX", "SUM", 
				"NUMBER_OF_NULLS" };
		String[] dataTypes = new String[] { "String", "String", "Double", "Double", 
				"Double", "Double", "Double", "Double" , "Double" };
		
		/*
		 * Update metadata for new frame
		 */
		OwlTemporalEngineMeta newFrameMetadata = newFrame.getMetaData();
		String newTableName = newFrame.getName();
		/*
		 * Using below utility to fill up the metadata in the new frame.
		 */
		ImportUtility.parseTableColumnsAndTypesToFlatTable(newFrameMetadata, newHeaders, dataTypes, newTableName);
		 
		/*
		 * Iterate over the headers and profile each column based on the datatype. If
		 * column is numeric, then call getNumericalProfileDataOnFrame() because we have
		 * to perform min/max/avg etc on the column data. Else treat every other col as
		 * string and call getStringProfileDataOnFrame().
		 */
		for(String header : actFrameHeaders) {
			String[] row = null;
			String colName = header.split(DOUBLE_UNDERSCORE)[1];
			if(SemossDataType.isNotString(headerDatatypeMap.get(header))){
				row = getNumericalProfileDataOnFrame( actFrame, tableName, colName);
			}else {
				row = getStringProfileDataOnFrame(actFrame, tableName, colName); 
			}
			newFrame.addRow(row, newHeaders);
			classLogger.info("New row added to the new frame for column " + colName);
		}
		
		/*
		 * Create the NounMetadata and store it in the varStore object.
		 */
		NounMetadata noun = new NounMetadata(newFrame, PixelDataType.FRAME, PixelOperationType.FRAME);
		this.insight.getVarStore().put(newFrameAlias, noun);
		classLogger.info("New Frame created with alias and has been set in the insight**********" + newFrameAlias);
		classLogger.info("Completed execution of FrameProfileReactor.");
		return noun;
	}
	
	
	/**
	 * Profile data on columns which are of Numerical datatype in the frame.
	 * @param frame
	 * @param tableName
	 * @param colName
	 * @return
	 */
	private String[] getNumericalProfileDataOnFrame(ITableDataFrame frame, String tableName, String colName) {
		//----FORMAT------
		//"table_name", "column_name", "numOfBlanks", "numOfUniqueValues", "minValue", "averageValue", "maxValue", "sumValue", "numOfNullValues"
		//"String", 		"String", 		"Double", 		"Double",		  "Double", 	"Double", 	  "Double",   "Double" , 		"Double" 
		String[] row = new String[9];
		row[0] = tableName;
		row[1] = colName;
		row[2] = getNumOfBlanks(frame, colName);
		computeNumericalProfilingData(row, frame, colName);
		row[8] = getNumOfNullValues(frame, colName);
		return row;
	}
	
	/**
	 * Profile data on columns which are of String datatype in the frame.
	 * @param frame
	 * @param tableName
	 * @param colName
	 * @return
	 */
	private String[] getStringProfileDataOnFrame(ITableDataFrame frame, String tableName, String colName) {
		//----FORMAT------
		//"table_name", "column_name", "numOfBlanks", "numOfUniqueValues", "minValue", "averageValue", "maxValue", "sumValue", "numOfNullValues"
		//"String", 		"String", 		"Double", 		"Double",		  "Double", 	"Double", 	  "Double",   "Double" , 		"Double" 
		String[] row = new String[9];
		
		row[0] = tableName;
		row[1] = colName;
		row[2] = getNumOfBlanks(frame, colName);
		row[3] = getNumOfUniqueValues(frame, colName);
		row[4] = NA;
		row[5] = NA;
		row[6] = NA;
		row[7] = NA;
		row[8] = getNumOfNullValues(frame, colName); // Need to debug. Getting exception
		return row;
	}
	
	/*
	 * We query on the frame in the below utility methods, hence we use the query method in the frame. 
	 * This helps us to run the queries in a frametype agnostic manner.
	 */
	
	/*
	 * ----------------------------Profile routines-----------------------------
	 */
	
	/**
	 * Get the number of blank values in a column which is of String datatype
	 * @param frame
	 * @param colName
	 * @return
	 */
	private String getNumOfBlanks(ITableDataFrame frame, String colName) {
		
		SelectQueryStruct qs = new SelectQueryStruct();
		QueryFunctionSelector uniqueCountSelector = new QueryFunctionSelector();
		uniqueCountSelector.setFunction(QueryFunctionHelper.COUNT);
		QueryColumnSelector innerSelector = new QueryColumnSelector(colName);
		uniqueCountSelector.addInnerSelector(innerSelector);
		qs.addSelector(uniqueCountSelector);
		
		QueryColumnSelector colSelector = new QueryColumnSelector(colName);
		SimpleQueryFilter blankFilter = new SimpleQueryFilter(new NounMetadata(colSelector, PixelDataType.COLUMN),
				DOUBLE_EQUALS, new NounMetadata("", PixelDataType.CONST_STRING));
		qs.addExplicitFilter(blankFilter);
		
		int blankCnts = 0;
		IRawSelectWrapper blanksWrapper = null;
		try {
			blanksWrapper = frame.query(qs);
			if(blanksWrapper.hasNext()) {
				blankCnts = ((Number)blanksWrapper.next().getValues()[0]).intValue();
			}else {
				classLogger.info("Blanks Wrapper is empty. No blanks!");
			}
		}catch(Exception e) {
			classLogger.warn("Exception during execution of query." + e.getMessage());
			classLogger.error(Constants.STACKTRACE, e);
		}finally {
			//Cleanup.
			if(blanksWrapper != null) {
				try {
					blanksWrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		return Integer.toString(blankCnts);
	}
	
	/**
	 * Get the number of unique values in a column which is of String datatype.
	 * @param frame
	 * @param colName
	 * @return
	 */
	private String getNumOfUniqueValues(ITableDataFrame frame, String colName) {

		SelectQueryStruct qs_unique = new SelectQueryStruct();
		QueryFunctionSelector functionSelector = new QueryFunctionSelector();
		functionSelector.setFunction(QueryFunctionHelper.UNIQUE_COUNT);
		QueryColumnSelector inSelector = new QueryColumnSelector(colName);
		functionSelector.addInnerSelector(inSelector);
		functionSelector.setDistinct(true);
		qs_unique.addSelector(functionSelector);
		qs_unique.setQsType(SelectQueryStruct.QUERY_STRUCT_TYPE.FRAME);

		IRawSelectWrapper uniqueWrapper = null;
		Object value = null;
		try {
			uniqueWrapper = frame.query(qs_unique);
			if(uniqueWrapper.hasNext()) {
				value = uniqueWrapper.next().getValues()[0];
			}else {
				classLogger.info("Unique Wrapper is empty. No unique elements.");
			}
		} catch (Exception e) {
			classLogger.warn("Exception during execution of query." + e.getMessage());
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if (uniqueWrapper != null) {
				try {
					uniqueWrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}

		return String.valueOf(value);
	}
	
	/**
	 * Get the number of null values in a column with String datatype.
	 * @param frame
	 * @param colName
	 * @return
	 */
	private String getNumOfNullValues(ITableDataFrame frame, String colName) {

		SelectQueryStruct qs_null = new SelectQueryStruct();
		QueryFunctionSelector uniqueCountSelector = new QueryFunctionSelector();
		uniqueCountSelector.setFunction(QueryFunctionHelper.COUNT);
		QueryColumnSelector innerSelector = new QueryColumnSelector(colName);
		uniqueCountSelector.addInnerSelector(innerSelector);
		qs_null.addSelector(uniqueCountSelector);
		QueryColumnSelector col = new QueryColumnSelector(colName);
		SimpleQueryFilter nullFilter = new SimpleQueryFilter(new NounMetadata(col, PixelDataType.COLUMN), 
				DOUBLE_EQUALS,
				new NounMetadata(null, PixelDataType.NULL_VALUE));
		SimpleQueryFilter stringNullFilter = new SimpleQueryFilter(new NounMetadata(col, PixelDataType.COLUMN), 
				DOUBLE_EQUALS,
				new NounMetadata("null", PixelDataType.NULL_VALUE));
		OrQueryFilter orfilter = new OrQueryFilter();
		orfilter.addFilter(nullFilter);
		orfilter.addFilter(stringNullFilter);
		qs_null.addExplicitFilter(orfilter);

		int nullCount = 0;
		IRawSelectWrapper nullWrapper = null;
		try {
			nullWrapper = frame.query(qs_null);
			if(nullWrapper.hasNext()) {
				nullCount = ((Number) nullWrapper.next().getValues()[0]).intValue();
			}else {
				classLogger.info("Null Wrapper is empty. No null values.");
			}
		} catch (Exception e) {
			classLogger.warn("Exception during execution of query." + e.getMessage());
			classLogger.error(Constants.STACKTRACE, e);
		}finally {
			if(nullWrapper != null) {
				try {
					nullWrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}

		return Integer.toString(nullCount);
	}
	
	/*
	 * ----------------------------Profile routines for numerical columns-----------------------------
	 */
	
	/**
	 * Get unique/min/max/avg/sum over the numerical columns
	 * @param row
	 * @param frame
	 * @param colName
	 */
	private void computeNumericalProfilingData(String[] row, ITableDataFrame frame, String colName) {
		SelectQueryStruct qs = new SelectQueryStruct();
		QueryColumnSelector innerSelector = new QueryColumnSelector(colName);
		qs.addSelector(getUniqueCountFunctionSelector(innerSelector, colName));
		qs.addSelector(getMinFunctionSelector(innerSelector, colName));
		qs.addSelector(getAvgFunctionSelector(innerSelector, colName));
		qs.addSelector(getMaxFunctionSelector(innerSelector, colName));
		qs.addSelector(getSumFunctionSelector(innerSelector, colName));
		
		qs.setQsType(SelectQueryStruct.QUERY_STRUCT_TYPE.FRAME);
		IRawSelectWrapper resultWrapper = null;
		try {
			resultWrapper = frame.query(qs);
			if(resultWrapper.hasNext()) {
				IHeadersDataRow datarow = resultWrapper.next();
				Object[] dataArr = datarow.getValues();
				int offset = 3;
				for(int i = 0; i < dataArr.length; i++) {
					row[offset + i] = String.valueOf(dataArr[i]);
				}
			}else {
				classLogger.info("Wrapper for calculating unique/min/max/avg/sum is empty for column name " + colName);
			}
		}catch(Exception e) {
			classLogger.warn("Exception during execution of query." + e.getMessage());
			classLogger.error(Constants.STACKTRACE, e);
		}
	}
	
	/**
	 * Create and return a Unique Count QueryFunctionSelector
	 * @param innerSelector
	 * @param colName
	 * @return
	 */
	private static QueryFunctionSelector getUniqueCountFunctionSelector(QueryColumnSelector innerSelector, String colName) {
		QueryFunctionSelector uniquefuncSelector = new QueryFunctionSelector();
		uniquefuncSelector.setFunction(QueryFunctionHelper.UNIQUE_COUNT);
		uniquefuncSelector.addInnerSelector(innerSelector);
		return uniquefuncSelector;
	}
	
	/**
	 * Create and return a Min QueryFunctionSelector
	 * @param innerSelector
	 * @param colName
	 * @return
	 */
	private static QueryFunctionSelector getMinFunctionSelector(QueryColumnSelector innerSelector, String colName) {
		QueryFunctionSelector minFuncSelector = new QueryFunctionSelector();
		minFuncSelector.setFunction(QueryFunctionHelper.MIN);
		minFuncSelector.addInnerSelector(innerSelector);
		return minFuncSelector;
	}
	
	/**
	 * Create and return an Average QueryFunctionSelector
	 * @param innerSelector
	 * @param colName
	 * @return
	 */
	private static QueryFunctionSelector getAvgFunctionSelector(QueryColumnSelector innerSelector, String colName) {
		QueryFunctionSelector avgFuncSelector = new QueryFunctionSelector();
		avgFuncSelector.setFunction(QueryFunctionHelper.AVERAGE_1);
		avgFuncSelector.addInnerSelector(innerSelector);
		return avgFuncSelector;
	}
	
	/**
	 * Create and return a Max QueryFunctionSelector
	 * @param innerSelector
	 * @param colName
	 * @return
	 */
	private static QueryFunctionSelector getMaxFunctionSelector(QueryColumnSelector innerSelector, String colName) {
		QueryFunctionSelector maxFuncSelector = new QueryFunctionSelector();
		maxFuncSelector.setFunction(QueryFunctionHelper.MAX);
		maxFuncSelector.addInnerSelector(innerSelector);
		return maxFuncSelector;
	}
	
	/**
	 * Create and return a Sum QueryFunctionSelector
	 * @param innerSelector
	 * @param colName
	 * @return
	 */
	private static QueryFunctionSelector getSumFunctionSelector(QueryColumnSelector innerSelector, String colName) {
		QueryFunctionSelector sumFuncSelector = new QueryFunctionSelector();
		sumFuncSelector.setFunction(QueryFunctionHelper.SUM);
		sumFuncSelector.addInnerSelector(innerSelector);
		return sumFuncSelector;
	}
}
