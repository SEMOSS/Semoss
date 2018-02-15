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
import prerna.engine.api.IHeadersDataRow;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.query.querystruct.QueryStruct2;
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
				String concept = conceptList.get(i);
				// TODO get col name for concepts
				String conceptColName = concept;
				// concept data
				String conceptDataType = MasterDatabaseUtility.getBasicDataType(dbName, conceptColName, concept);
				if (Utility.isNumericType(conceptDataType)) {
					String[] cells = new String[8];
					// table name
					cells[0] = concept;
					// column name
					cells[1] = concept;
					// num of blanks
					cells[2] = 0 + "";
					// num of unique vals
					cells[3] = getValue(engine, concept, concept, QueryFunctionHelper.UNIQUE_COUNT) + "";
					// min
					cells[4] = getValue(engine, concept, conceptColName, QueryFunctionHelper.MIN) + "";
					// average
					cells[5] = getValue(engine, concept, conceptColName, QueryFunctionHelper.AVERAGE_1) + "";
					// max
					cells[6] = getValue(engine, concept, conceptColName, QueryFunctionHelper.MAX) + "";
					// sum
					cells[7] = getValue(engine, concept, conceptColName, QueryFunctionHelper.SUM) + "";
					// add data to frame
					frame.addRow(tableName, cells, headers, dataTypes);
				} else {
					if (Utility.isStringType(conceptDataType)) {
						String[] cells = new String[8];
						// table name
						cells[0] = concept;
						// column name
						cells[1] = concept;
						// num of blanks
						cells[2] = null;
						// num of unique vals
						cells[3] = getValue(engine, concept, concept, QueryFunctionHelper.UNIQUE_COUNT) + "";
						// add data to frame
						frame.addRow(tableName, cells, headers, dataTypes);
					}
				}
				// prop data
				
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
							// TODO # of blanks
							QueryStruct2 qs2 = new QueryStruct2();
//							{
//								QueryFunctionSelector uniqueCountSelector = new QueryFunctionSelector();
//								uniqueCountSelector.setFunction(QueryFunctionHelper.UNIQUE_COUNT);
//								QueryColumnSelector innerSelector = new QueryColumnSelector();
//
//								innerSelector.setTable(concept);
//								innerSelector.setColumn(prop);
//								uniqueCountSelector.addInnerSelector(innerSelector);
//								qs2.addSelector(uniqueCountSelector);
//								
//								QueryColumnSelector col = new QueryColumnSelector();
//								col.setTable(concept);
//								col.setColumn(prop);
//								SimpleQueryFilter filter = new SimpleQueryFilter(new NounMetadata(col, PixelDataType.COLUMN), "==", new NounMetadata("\'\'", PixelDataType.CONST_STRING));
//								qs2.addExplicitFilter(filter);
//							}
//							Iterator<IHeadersDataRow> blankIt = WrapperManager.getInstance().getRawWrapper(engine, qs2);
//							long blankCount = ((Number) blankIt.next().getValues()[0]).longValue();
							cells[2] = null;
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
							// assume string
							if (Utility.isStringType(dataType)) {
								String[] cells = new String[8];
								// table name
								cells[0] = concept;
								// column name
								cells[1] = prop;
								// # TODO of blanks
								cells[2] = null;
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
//			funSelector.setDistinct(true);
			QueryColumnSelector innerSelector = new QueryColumnSelector();

			innerSelector.setTable(concept);
			innerSelector.setColumn(prop);
			funSelector.addInnerSelector(innerSelector);
			qs2.addSelector(funSelector);
		}
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
