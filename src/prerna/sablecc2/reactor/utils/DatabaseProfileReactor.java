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
		// add headers to metadata
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
				// concept data type
				if (Utility.isNumericType(conceptDataType)) {
					String[] cells = new String[8];
					// table name
					cells[0] = concept;
					// column name
					cells[1] = primKey;
					// num of blanks
					cells[2] = 0 + "";
					// num of unique vals
					cells[3] = getValue(engine, concept, primKey, QueryFunctionHelper.UNIQUE_COUNT) + "";
					// min
					cells[4] = getValue(engine, concept, primKey, QueryFunctionHelper.MIN) + "";
					// average
					cells[5] = getValue(engine, concept, primKey, QueryFunctionHelper.AVERAGE_1) + "";
					// max
					cells[6] = getValue(engine, concept, primKey, QueryFunctionHelper.MAX) + "";
					// sum
					cells[7] = getValue(engine, concept, primKey, QueryFunctionHelper.SUM) + "";
					// add data to frame
					frame.addRow(tableName, cells, headers, dataTypes);
				} else {
					if (Utility.isStringType(conceptDataType)) {
						String[] cells = new String[8];
						// table name
						cells[0] = concept;
						// column name
						cells[1] = primKey;
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
						cells[2] = blankCount + "";
						// num of unique vals
						cells[3] = getValue(engine, concept, primKey, QueryFunctionHelper.UNIQUE_COUNT) + "";
						// add data to frame
						frame.addRow(tableName, cells, headers, dataTypes);
					}
				}

				// now get prop profile data
				if (conceptMap != null && conceptMap.containsKey(concept)) {
					ArrayList<String> properties = conceptMap.get(concept);
					for (String prop : properties) {
						String dataType = MasterDatabaseUtility.getBasicDataType(dbName, prop, concept);
						if (Utility.isNumericType(dataType)) {
							// will need to get min, average, max, sum
							String[] cells = new String[8];
							// table name
							cells[0] = concept;
							// column name
							cells[1] = prop;
							// # of blanks
							cells[2] = 0 + "";
							// # of unique values
							cells[3] = getValue(engine, concept, prop, QueryFunctionHelper.UNIQUE_COUNT) + "";
							// min
							cells[4] = getValue(engine, concept, prop, QueryFunctionHelper.MIN) + "";
							// average
							cells[5] = getValue(engine, concept, prop, QueryFunctionHelper.AVERAGE_1) + "";
							// max
							cells[6] = getValue(engine, concept, prop, QueryFunctionHelper.MAX) + "";
							// sum
							cells[7] = getValue(engine, concept, prop, QueryFunctionHelper.SUM) + "";
							// add data to frame
							frame.addRow(tableName, cells, headers, dataTypes);
						} else {
							// assume property is string type
							if (Utility.isStringType(dataType)) {
								String[] cells = new String[8];
								// table name
								cells[0] = concept;
								// column name
								cells[1] = prop;
								// # of blanks
								QueryStruct2 qs2 = new QueryStruct2();
								{
									QueryFunctionSelector uniqueCountSelector = new QueryFunctionSelector();
									uniqueCountSelector.setFunction(QueryFunctionHelper.COUNT);
									QueryColumnSelector innerSelector = new QueryColumnSelector();
									innerSelector.setTable(concept);
									innerSelector.setColumn(prop);
									uniqueCountSelector.addInnerSelector(innerSelector);
									qs2.addSelector(uniqueCountSelector);
									QueryColumnSelector col = new QueryColumnSelector();
									col.setTable(concept);
									col.setColumn(prop);
									SimpleQueryFilter filter = new SimpleQueryFilter(
											new NounMetadata(col, PixelDataType.COLUMN), "==",
											new NounMetadata("", PixelDataType.CONST_STRING));
									qs2.addExplicitFilter(filter);
								}
								Iterator<IHeadersDataRow> blankIt = WrapperManager.getInstance().getRawWrapper(engine, qs2);
								long blankCount = ((Number) blankIt.next().getValues()[0]).longValue();
								cells[2] = blankCount + "";
								// # of unique values
								cells[3] = getValue(engine, concept, prop, QueryFunctionHelper.UNIQUE_COUNT) + "";
								frame.addRow(tableName, cells, headers, dataTypes);
							}
						}
					}
				}
			}
		}
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE);
	}

	private Object getValue(IEngine engine, String concept, String prop, String functionName) {
		QueryStruct2 qs2 = new QueryStruct2();
		{
			QueryFunctionSelector funSelector = new QueryFunctionSelector();
			funSelector.setFunction(functionName);
			QueryColumnSelector innerSelector = new QueryColumnSelector();
			innerSelector.setTable(concept);
			innerSelector.setColumn(prop);
			funSelector.addInnerSelector(innerSelector);
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
