package prerna.sablecc2.reactor.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.h2.H2Frame;
import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.ENGINE_TYPE;
import prerna.engine.api.IHeadersDataRow;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.query.querystruct.QueryStruct2;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Utility;

public class DatabaseProfileReactor extends AbstractReactor {

	public DatabaseProfileReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.CONCEPTS.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		// output frame
		String[] headers = new String[] { "table_name", "column_name", "numOfBlanks", "numOfUniqueValues", "min", "average", "max", "sum" };
		String[] dataTypes = new String[] { "String", "String", "Double", "Double", "Double", "Double", "Double", "Double" };
		H2Frame frame = (H2Frame) this.insight.getDataMaker();
		String tableName = frame.getTableName();
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
		
		String dbName = this.keyValue.get(this.keysToGet[0]);
		IEngine engine = Utility.getEngine(dbName);
		if (engine != null) {
			List<String> conceptList = getConceptList();
			// get concept properties from local master
			Map<String, HashMap> dbMap = MasterDatabaseUtility.getConceptProperties(conceptList, dbName);
			HashMap<String, ArrayList<String>> conceptMap = dbMap.get(dbName);
			for (int i = 0; i < conceptList.size(); i++) {
				// first add concept profile data
				String concept = conceptList.get(i);
				String primKey = null;
				String conceptDataType = null;
				// determine concept prim key if applicable and dataType
				ENGINE_TYPE engineType = engine.getEngineType();
				if (engineType.equals(ENGINE_TYPE.SESAME)) {
					conceptDataType = MasterDatabaseUtility.getBasicDataType(dbName, concept, null);
					primKey = concept;
				} else if (engineType.equals(ENGINE_TYPE.RDBMS)) {
					// get primary key for concept
					String conceptualURI = "http://semoss.org/ontologies/Concept/" + concept;
					String tableURI = engine.getPhysicalUriFromConceptualUri(conceptualURI);
					primKey = Utility.getClassName(tableURI);
					conceptDataType = MasterDatabaseUtility.getBasicDataType(dbName, primKey, concept);
				}
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

				// now get property profile data
				if (conceptMap != null && conceptMap.containsKey(concept)) {
					ArrayList<String> properties = conceptMap.get(concept);
					for (String prop : properties) {
						// check property data type
						String dataType = MasterDatabaseUtility.getBasicDataType(dbName, prop, concept);
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
		String[] retRow = new String[8];
		// table name
		retRow[0] = concept;
		// column name
		retRow[1] = primKey;
		// num of blanks
		QueryStruct2 qs2 = new QueryStruct2();
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
		}
		Iterator<IHeadersDataRow> blankIt = WrapperManager.getInstance().getRawWrapper(engine, qs2);
		long blankCount = ((Number) blankIt.next().getValues()[0]).longValue();
		retRow[2] = blankCount + "";
		// num of unique vals
		retRow[3] = getValue(engine, concept, primKey, QueryFunctionHelper.UNIQUE_COUNT, true) + "";		
		return retRow;
	}

	private String[] getNumericalProfileData(IEngine engine, String concept, String prop) {
		String[] retRow = new String[8];
		// table name
		retRow[0] = concept;
		// column name
		retRow[1] = prop;
		// # of blanks
		retRow[2] = 0 + "";
		// create qs
		QueryStruct2 qs2 = new QueryStruct2();
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
		qs2.setQsType(QueryStruct2.QUERY_STRUCT_TYPE.ENGINE);
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
		return retRow;
	}

	private Object getValue(IEngine engine, String concept, String prop, String functionName, boolean distinct) {
		QueryStruct2 qs2 = new QueryStruct2();
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
		qs2.setQsType(QueryStruct2.QUERY_STRUCT_TYPE.ENGINE);
		Iterator<IHeadersDataRow> it = WrapperManager.getInstance().getRawWrapper(engine, qs2);
		Object value = it.next().getValues()[0];
		return value;
	}

	private List<String> getConceptList() {
		Vector<String> inputs = null;
		GenRowStruct valuesGrs = this.store.getNoun(keysToGet[1]);
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
