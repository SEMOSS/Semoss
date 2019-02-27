package prerna.sablecc2.reactor.utils;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAppUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.h2.H2Frame;
import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryArithmeticSelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.AbstractFrameReactor;
import prerna.util.Utility;

public class DatabaseProfileReactor extends AbstractFrameReactor {

	public DatabaseProfileReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FRAME.getKey(), ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.CONCEPTS.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		
		String engineId = this.keyValue.get(this.keysToGet[1]);
		if(AbstractSecurityUtils.securityEnabled()) {
			engineId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), engineId);
			if(!SecurityAppUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
				throw new IllegalArgumentException("Database " + engineId + " does not exist or user does not have access to database");
			}
		} else {
			engineId = MasterDatabaseUtility.testEngineIdIfAlias(engineId);
			if(!MasterDatabaseUtility.getAllEngineIds().contains(engineId)) {
				throw new IllegalArgumentException("Database " + engineId + " does not exist");
			}
		}

		// output frame
		ITableDataFrame table = getFrame();
		if(!(table instanceof H2Frame)) {
			throw new IllegalArgumentException("Frame must be a grid to use DatabaseProfile");
		}
		H2Frame frame = (H2Frame) table;
		String tableName = frame.getName();

		String[] headers = new String[] { "table_name", "column_name", "numOfBlanks", "numOfUniqueValues", "minValue", "averageValue", "maxValue", "sumValue", "numOfNullValues" };
		String[] dataTypes = new String[] { "String", "String", "Double", "Double", "Double", "Double", "Double", "Double" , "Double" };
		// add headers to metadata output frame
		OwlTemporalEngineMeta metaData = frame.getMetaData();
		for (int i = 0; i < headers.length; i++) {
			String alias = headers[i];
			String dataType = dataTypes[i];
			String uniqueHeader = tableName + "__" + alias;
			metaData.addProperty(tableName, uniqueHeader);
			// metaData.setQueryStructNameToProperty(uniqueHeader, source,
			// qsName);
			metaData.setAliasToProperty(uniqueHeader, alias);
			metaData.setDataTypeToProperty(uniqueHeader, dataType);
		}
		
		IEngine engine = Utility.getEngine(engineId);
		if(engine == null) {
			throw new IllegalArgumentException("Could not find database " + engineId);
		}
		
		List<String> conceptList = getConceptList();
		// get concept properties from local master
		Map<String, List<String>> conceptMap = MasterDatabaseUtility.getConceptProperties(conceptList, engineId);
		for (String concept : conceptList) {
			// first add concept profile data
			String primKey = AbstractQueryStruct.PRIM_KEY_PLACEHOLDER;
			String conceptDataType = null;
			conceptDataType = MasterDatabaseUtility.getBasicDataType(engineId,  concept, null);
			// concept data type now get profile data based on type
			if (Utility.isNumericType(conceptDataType)) {
				String[] row = getNumericalProfileData(engine, concept, primKey);
				frame.addRow(tableName, row, headers, dataTypes);
			} else {
				if (Utility.isStringType(conceptDataType)) {
					String[] cells = getStringProfileData(engine, concept, primKey);
					frame.addRow(tableName, cells, headers, dataTypes);
				}
			}
			if (conceptMap != null) {
				// now get property profile data
				List<String> properties = conceptMap.get(concept);
				if (properties != null) {
					for (String prop : properties) {
						// check property data type
						String dataType = MasterDatabaseUtility.getBasicDataType(engineId, prop, concept);
						if (Utility.isNumericType(dataType)) {
							String[] cells = getNumericalProfileData(engine, concept, prop);
							frame.addRow(tableName, cells, headers, dataTypes);
						} else {
							if (Utility.isStringType(dataType)) {
								String[] cells = getStringProfileData(engine, concept, prop);
								frame.addRow(tableName, cells, headers, dataTypes);
							}
						}
					}
				}
			}
		}
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE);
	}

	private String[] getStringProfileData(IEngine engine, String concept, String primKey) {
		String[] retRow = new String[9];
		// table name
		retRow[0] = concept;
		// column name
		if (primKey.equals(AbstractQueryStruct.PRIM_KEY_PLACEHOLDER)) {
			// we dont have it.. so query for it
			String tableURI = engine.getConceptPhysicalUriFromConceptualUri(concept);
			// since we also have the URI, just store the primary key as well
			// will most likely be used
			primKey = Utility.getClassName(tableURI);
			retRow[1] = primKey;
		} else {
			// property
			retRow[1] = primKey;
		}
		// num of blanks
		SelectQueryStruct qs2 = new SelectQueryStruct();
		SelectQueryStruct qs_nulls = new SelectQueryStruct();
		{
			QueryFunctionSelector uniqueCountSelector = new QueryFunctionSelector();
			uniqueCountSelector.setFunction(QueryFunctionHelper.COUNT);
			QueryColumnSelector innerSelector = new QueryColumnSelector();
			innerSelector.setTable(concept);
			innerSelector.setColumn(primKey);
			uniqueCountSelector.addInnerSelector(innerSelector);
			qs2.addSelector(uniqueCountSelector);
			QueryColumnSelector col = new QueryColumnSelector();
			col.setTable(concept);
			col.setColumn(primKey);
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
		Iterator<IHeadersDataRow> blankIt = WrapperManager.getInstance().getRawWrapper(engine, qs2);
		long blankCount = 0;
		if (blankIt.hasNext()) {
			blankCount = ((Number) blankIt.next().getValues()[0]).longValue();
		}
		retRow[2] = blankCount + "";

		// num of unique vals
		retRow[3] = getValue(engine, concept, primKey, QueryFunctionHelper.UNIQUE_COUNT, true) + "";

		// get null values count
		Iterator<IHeadersDataRow> nullIt = WrapperManager.getInstance().getRawWrapper(engine, qs_nulls);
		long nullCount = 0;
		if (nullIt.hasNext()) {
			nullCount = ((Number) nullIt.next().getValues()[0]).longValue();
		}
		retRow[8] = nullCount + "";
		return retRow;
	}

	private String[] getNumericalProfileData(IEngine engine, String concept, String prop) {
		String[] retRow = new String[9];
		// table name
		retRow[0] = concept;
		// column name
		retRow[1] = prop;
		// # of blanks
		retRow[2] = 0 + "";
		// create qs
		SelectQueryStruct qs2 = new SelectQueryStruct();
		SelectQueryStruct qs_nulls = new SelectQueryStruct();
		{
			// inner selector
			QueryColumnSelector innerSelector = new QueryColumnSelector();
			innerSelector.setTable(concept);
			innerSelector.setColumn(prop);
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
		Iterator<IHeadersDataRow> it = WrapperManager.getInstance().getRawWrapper(engine, qs2);
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
				QueryColumnSelector innerSelector = new QueryColumnSelector();
				innerSelector.setTable(concept);
				innerSelector.setColumn(prop);
				countNonNulls.addInnerSelector(innerSelector);
			}
			arithmaticSelector.setLeftSelector(countAllRows);
			arithmaticSelector.setRightSelector(countNonNulls);
			
			qs_nulls.addSelector(arithmaticSelector);
		}
		// get null values count
		Iterator<IHeadersDataRow> nullIt = WrapperManager.getInstance().getRawWrapper(engine, qs_nulls);
		long nullCount = ((Number) nullIt.next().getValues()[0]).longValue();
		retRow[8] = "" + nullCount;
		return retRow;
	}

	private Object getValue(IEngine engine, String concept, String prop, String functionName, boolean distinct) {
		SelectQueryStruct qs2 = new SelectQueryStruct();
		{
			QueryFunctionSelector funSelector = new QueryFunctionSelector();
			funSelector.setFunction(functionName);
			QueryColumnSelector innerSelector = new QueryColumnSelector();
			innerSelector.setTable(concept);
			innerSelector.setColumn(prop);
			funSelector.addInnerSelector(innerSelector);
			funSelector.setDistinct(distinct);
			qs2.addSelector(funSelector);
		}
		qs2.setQsType(SelectQueryStruct.QUERY_STRUCT_TYPE.ENGINE);
		Iterator<IHeadersDataRow> it = WrapperManager.getInstance().getRawWrapper(engine, qs2);
		Object value = it.next().getValues()[0];
		return value;
	}

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
