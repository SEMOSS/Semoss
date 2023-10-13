package prerna.reactor.utils;

import java.io.IOException;
import java.util.List;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.DataFrameTypeEnum;
import prerna.algorithm.api.ITableDataFrame;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.rdbms.AbstractRdbmsFrame;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryArithmeticSelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.reactor.frame.AbstractFrameReactor;
import prerna.reactor.frame.FrameFactory;
import prerna.reactor.imports.ImportUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class DatabaseProfileReactor extends AbstractFrameReactor {

	private static final Logger classLogger = LogManager.getLogger(DatabaseProfileReactor.class);

	public DatabaseProfileReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FRAME.getKey(), ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.CONCEPTS.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		
		String databaseId = this.keyValue.get(this.keysToGet[1]);
		databaseId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), databaseId);
		if(!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), databaseId)) {
			throw new IllegalArgumentException("Database " + databaseId + " does not exist or user does not have access to database");
		}

		IDatabaseEngine database = Utility.getDatabase(databaseId);
		if(database == null) {
			throw new IllegalArgumentException("Could not find database " + databaseId);
		}
		
		// output frame
		ITableDataFrame table = null;
		try {
			table = getFrame();
			if(!(table instanceof AbstractRdbmsFrame)) {
				throw new IllegalArgumentException("Frame must be a grid to use DatabaseProfile");
			}
		} catch(NullPointerException e) {
			// ignore - make a new frame
			try {
				table = FrameFactory.getFrame(this.insight, DataFrameTypeEnum.GRID.getTypeAsString(), "");
			} catch (Exception e2) {
				throw new IllegalArgumentException("Error occurred trying to create frame of type " + DataFrameTypeEnum.GRID.getTypeAsString(), e2);
			}
		}
		AbstractRdbmsFrame frame = (AbstractRdbmsFrame) table;
		String tableName = frame.getName();

		String[] headers = new String[] { "table_name", "column_name", "numOfBlanks", "numOfUniqueValues", "minValue", "averageValue", "maxValue", "sumValue", "numOfNullValues" };
		String[] dataTypes = new String[] { "String", "String", "Double", "Double", "Double", "Double", "Double", "Double" , "Double" };
		// add headers to metadata output frame
		OwlTemporalEngineMeta metaData = frame.getMetaData();
		ImportUtility.parseTableColumnsAndTypesToFlatTable(metaData, headers, dataTypes, tableName);
		
		List<String> conceptList = getConceptList();
		// get concept properties from local master
		for(String concept : conceptList) {
			List<String> pixelSelectors = MasterDatabaseUtility.getConceptPixelSelectors(concept, databaseId);
			// the pixel selectors will already be in TABLE__COLUMN format
			for(String selector : pixelSelectors) {
				String semossName = selector;
				String parentSemossName = null;
				if(semossName.contains("__")) {
					String[] split = selector.split("__");
					semossName = split[1];
					parentSemossName = split[0];
				}
				String dataType = MasterDatabaseUtility.getBasicDataType(databaseId, semossName, parentSemossName);
				if (Utility.isNumericType(dataType)) {
					String[] row = getNumericalProfileData(database, selector);
					frame.addRow(tableName, headers, row, dataTypes);
				} else {
					String[] cells = getStringProfileData(database, selector);
					frame.addRow(tableName, headers, cells, dataTypes);
				}
			}
		}
		
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE);
	}

	private String[] getStringProfileData(IDatabaseEngine database, String selector) {
		String[] retRow = new String[9];
		if(selector.contains("__")) {
			String[] split = selector.split("__");
			// table name
			retRow[0] = split[0];
			// column name
			retRow[1] = split[1];
		} else {
			// table name
			retRow[0] = selector;
			// column name
			retRow[1] = null;
		}
		// num of blanks
		SelectQueryStruct qs2 = new SelectQueryStruct();
		SelectQueryStruct qs_nulls = new SelectQueryStruct();
		{
			QueryFunctionSelector uniqueCountSelector = new QueryFunctionSelector();
			uniqueCountSelector.setFunction(QueryFunctionHelper.COUNT);
			QueryColumnSelector innerSelector = new QueryColumnSelector(selector);
			uniqueCountSelector.addInnerSelector(innerSelector);
			qs2.addSelector(uniqueCountSelector);
			QueryColumnSelector col = new QueryColumnSelector(selector);
			SimpleQueryFilter filter = new SimpleQueryFilter(
					new NounMetadata(col, PixelDataType.COLUMN), "==",
					new NounMetadata("", PixelDataType.CONST_STRING));
			qs2.addExplicitFilter(filter);
			// nulls
			qs_nulls.addSelector(uniqueCountSelector);
			SimpleQueryFilter nulls = new SimpleQueryFilter(
					new NounMetadata(col, PixelDataType.COLUMN), "==",
					new NounMetadata(null, PixelDataType.NULL_VALUE));
			qs_nulls.addExplicitFilter(nulls);
		}
		// get blank values count
		long blankCount = 0;
		IRawSelectWrapper blankIt = null;
		try {
			blankIt = WrapperManager.getInstance().getRawWrapper(database, qs2);
			if (blankIt.hasNext()) {
				blankCount = ((Number) blankIt.next().getValues()[0]).longValue();
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(blankIt != null) {
				try {
					blankIt.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		retRow[2] = blankCount + "";

		// num of unique vals
		retRow[3] = getValue(database, selector, QueryFunctionHelper.UNIQUE_COUNT, true) + "";

		// get null values count
		long nullCount = 0;
		IRawSelectWrapper nullIt = null;
		try {
			nullIt = WrapperManager.getInstance().getRawWrapper(database, qs_nulls);
			if (nullIt.hasNext()) {
				nullCount = ((Number) nullIt.next().getValues()[0]).longValue();
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(nullIt != null) {
				try {
					nullIt.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		retRow[8] = nullCount + "";
		return retRow;
	}

	private String[] getNumericalProfileData(IDatabaseEngine database, String selector) {
		String[] retRow = new String[9];
		if(selector.contains("__")) {
			String[] split = selector.split("__");
			// table name
			retRow[0] = split[0];
			// column name
			retRow[1] = split[1];
		} else {
			// table name
			retRow[0] = selector;
			// column name
			retRow[1] = null;
		}
		// # of blanks
		retRow[2] = 0 + "";
		// create qs
		SelectQueryStruct qs2 = new SelectQueryStruct();
		SelectQueryStruct qs_nulls = new SelectQueryStruct();
		{
			// inner selector
			QueryColumnSelector innerSelector = new QueryColumnSelector(selector);
			// unique count
			QueryFunctionSelector uniqueCount = new QueryFunctionSelector();
			uniqueCount.setFunction(QueryFunctionHelper.UNIQUE_COUNT);
			uniqueCount.addInnerSelector(innerSelector);
			qs2.addSelector(uniqueCount);
			// min
			QueryFunctionSelector min = new QueryFunctionSelector();
			min.setFunction(QueryFunctionHelper.MIN);
			min.addInnerSelector(innerSelector);
			qs2.addSelector(min);
			// avg
			QueryFunctionSelector avg = new QueryFunctionSelector();
			avg.setFunction(QueryFunctionHelper.AVERAGE_1);
			avg.addInnerSelector(innerSelector);
			qs2.addSelector(avg);
			// max
			QueryFunctionSelector max = new QueryFunctionSelector();
			max.setFunction(QueryFunctionHelper.MAX);
			max.addInnerSelector(innerSelector);
			qs2.addSelector(max);
			// sum
			QueryFunctionSelector sum = new QueryFunctionSelector();
			sum.setFunction(QueryFunctionHelper.SUM);
			sum.addInnerSelector(innerSelector);
			qs2.addSelector(sum);
		}
		qs2.setQsType(SelectQueryStruct.QUERY_STRUCT_TYPE.ENGINE);
		IRawSelectWrapper it = null;
		try {
			it = WrapperManager.getInstance().getRawWrapper(database, qs2);
			while (it.hasNext()) {
				IHeadersDataRow iRow = it.next();
				Object[] values = iRow.getValues();
				// unique count
				retRow[3] = values[0] + "";
				// min
				retRow[4] = values[1] + "";
				// avg
				retRow[5] = values[2] + "";
				// max
				retRow[6] = values[3] + "";
				// sum
				retRow[7] = values[4] + "";
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(it != null) {
				try {
					it.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		// nulls
		qs_nulls = new SelectQueryStruct();
		{
			QueryArithmeticSelector arithmaticSelector = new QueryArithmeticSelector();
			arithmaticSelector.setMathExpr("-");
			QueryFunctionSelector countAllRows = new QueryFunctionSelector();
			countAllRows.setFunction(QueryFunctionHelper.COUNT);
			{
				QueryConstantSelector innerSelector = new QueryConstantSelector();
				innerSelector.setConstant(1);
				countAllRows.addInnerSelector(innerSelector);
			}
			QueryFunctionSelector countNonNulls = new QueryFunctionSelector();
			countNonNulls.setFunction(QueryFunctionHelper.COUNT);
			{
				QueryColumnSelector innerSelector = new QueryColumnSelector(selector);
				countNonNulls.addInnerSelector(innerSelector);
			}
			arithmaticSelector.setLeftSelector(countAllRows);
			arithmaticSelector.setRightSelector(countNonNulls);
			
			qs_nulls.addSelector(arithmaticSelector);
		}
		// get null values count
		long nullCount = 0;
		IRawSelectWrapper nullIt = null;
		try {
			nullIt = WrapperManager.getInstance().getRawWrapper(database, qs_nulls);
			nullCount = ((Number) nullIt.next().getValues()[0]).longValue();
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(nullIt != null) {
				try {
					nullIt.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		retRow[8] = "" + nullCount;
		return retRow;
	}

	private Object getValue(IDatabaseEngine database, String selector, String functionName, boolean distinct) {
		SelectQueryStruct qs2 = new SelectQueryStruct();
		{
			QueryFunctionSelector funSelector = new QueryFunctionSelector();
			funSelector.setFunction(functionName);
			QueryColumnSelector innerSelector = new QueryColumnSelector(selector);
			funSelector.addInnerSelector(innerSelector);
			funSelector.setDistinct(distinct);
			qs2.addSelector(funSelector);
		}
		qs2.setQsType(SelectQueryStruct.QUERY_STRUCT_TYPE.ENGINE);
		IRawSelectWrapper it = null;
		try {
			it = WrapperManager.getInstance().getRawWrapper(database, qs2);
			Object value = it.next().getValues()[0];
			return value;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(it != null) {
				try {
					it.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		return null;
	}

	/**
	 * Get the list of concepts to profile
	 * @return
	 */
	private List<String> getConceptList() {
		Vector<String> inputs = null;
		GenRowStruct valuesGrs = this.store.getNoun(keysToGet[2]);
		if (valuesGrs != null && valuesGrs.size() > 0) {
			int numInputs = valuesGrs.size();
			inputs = new Vector<String>();
			for (int i = 0; i < numInputs; i++) {
				String input = valuesGrs.get(i).toString();
				inputs.add(input);
			}
		}
		return inputs;
	}
}
