package prerna.sablecc2.reactor.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

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
		String[] dataTypes = new String[] { "String", "String", "DOUBLE", "DOUBLE", "DOUBLE", "DOUBLE", "DOUBLE", "DOUBLE" };
		H2Frame frame = new H2Frame(headers, dataTypes);
		String dbName = this.keyValue.get(this.keysToGet[0]);
		IEngine engine = Utility.getEngine(dbName);
		if (engine != null) {
			List<String> conceptList = getConceptList();

			// get concept properties from local master
			Map<String, HashMap> dbMap = MasterDatabaseUtility.getConceptProperties(conceptList, dbName);
			HashMap<String, ArrayList<String>> conceptMap = dbMap.get(dbName);
			for (int i = 0; i < conceptList.size(); i++) {
				String concept = conceptList.get(i);
				if (conceptMap.containsKey(concept)) {
					ArrayList<String> properties = conceptMap.get(concept);
					for (String prop : properties) {
						String dataType = MasterDatabaseUtility.getBasicDataType(dbName, prop, concept);
						if (Utility.isNumericType(dataType)) {
							// will need to get min, average, max, sum
							Object[] cells = new Object[8];
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
							cells[2] = 0;
							// # of unique values
							 qs2 = new QueryStruct2();
							{
								QueryFunctionSelector uniqueCountSelector = new QueryFunctionSelector();
								uniqueCountSelector.setFunction(QueryFunctionHelper.UNIQUE_COUNT);
								uniqueCountSelector.setDistinct(true);
								QueryColumnSelector innerSelector = new QueryColumnSelector();

								innerSelector.setTable(concept);
								innerSelector.setColumn(prop);
								uniqueCountSelector.addInnerSelector(innerSelector);
								qs2.addSelector(uniqueCountSelector);
							}
							Iterator<IHeadersDataRow> uIt = WrapperManager.getInstance().getRawWrapper(engine, qs2);
							long uniqueNRow = ((Number) uIt.next().getValues()[0]).longValue();
							cells[3] = uniqueNRow;

							// min
							qs2 = new QueryStruct2();
							{
								QueryFunctionSelector uniqueCountSelector = new QueryFunctionSelector();
								uniqueCountSelector.setFunction(QueryFunctionHelper.MIN);
								QueryColumnSelector innerSelector = new QueryColumnSelector();

								innerSelector.setTable(concept);
								innerSelector.setColumn(prop);
								uniqueCountSelector.addInnerSelector(innerSelector);
								qs2.addSelector(uniqueCountSelector);
							}
							Iterator<IHeadersDataRow> minIt = WrapperManager.getInstance().getRawWrapper(engine, qs2);
							long min = ((Number) minIt.next().getValues()[0]).longValue();
							cells[4] = min;

							// average
							qs2 = new QueryStruct2();
							{
								QueryFunctionSelector uniqueCountSelector = new QueryFunctionSelector();
								uniqueCountSelector.setFunction(QueryFunctionHelper.AVERAGE_1);
								QueryColumnSelector innerSelector = new QueryColumnSelector();

								innerSelector.setTable(concept);
								innerSelector.setColumn(prop);
								uniqueCountSelector.addInnerSelector(innerSelector);
								qs2.addSelector(uniqueCountSelector);
							}
							Iterator<IHeadersDataRow> avgIt = WrapperManager.getInstance().getRawWrapper(engine, qs2);
							long avg = ((Number) avgIt.next().getValues()[0]).longValue();
							cells[5] = avg;

							// max
							qs2 = new QueryStruct2();
							{
								QueryFunctionSelector uniqueCountSelector = new QueryFunctionSelector();
								uniqueCountSelector.setFunction(QueryFunctionHelper.MAX);
								QueryColumnSelector innerSelector = new QueryColumnSelector();

								innerSelector.setTable(concept);
								innerSelector.setColumn(prop);
								uniqueCountSelector.addInnerSelector(innerSelector);
								qs2.addSelector(uniqueCountSelector);
							}
							Iterator<IHeadersDataRow> maxIt = WrapperManager.getInstance().getRawWrapper(engine, qs2);
							long max = ((Number) maxIt.next().getValues()[0]).longValue();
							cells[6] = max;

							// sum
							qs2 = new QueryStruct2();
							{
								QueryFunctionSelector uniqueCountSelector = new QueryFunctionSelector();
								uniqueCountSelector.setFunction(QueryFunctionHelper.SUM);
								QueryColumnSelector innerSelector = new QueryColumnSelector();

								innerSelector.setTable(concept);
								innerSelector.setColumn(prop);
								uniqueCountSelector.addInnerSelector(innerSelector);
								qs2.addSelector(uniqueCountSelector);
							}
							Iterator<IHeadersDataRow> sumIt = WrapperManager.getInstance().getRawWrapper(engine, qs2);
							long sum = ((Number) sumIt.next().getValues()[0]).longValue();
							cells[7] = sum;
							// add data to frame
							frame.addRow(cells, headers);
						} else {
							// assume string
							if (Utility.isStringType(dataType)) {
								Object[] cells = new Object[8];
								// table name
								cells[0] = concept;
								// column name
								cells[1] = prop;
								// # TODO of blanks
								cells[2] = null;
								// # of unique values
								QueryStruct2 qs2 = new QueryStruct2();
								{
									QueryFunctionSelector uniqueCountSel = new QueryFunctionSelector();
									uniqueCountSel.setFunction(QueryFunctionHelper.UNIQUE_COUNT);
									uniqueCountSel.setDistinct(true);
									QueryColumnSelector innerSelector = new QueryColumnSelector();
									innerSelector.setTable(concept);
									innerSelector.setColumn(prop);
									uniqueCountSel.addInnerSelector(innerSelector);
									qs2.addSelector(uniqueCountSel);
								}
								Iterator<IHeadersDataRow> uIt = WrapperManager.getInstance().getRawWrapper(engine, qs2);
								long uniqueNRow = ((Number) uIt.next().getValues()[0]).longValue();
								cells[3] = uniqueNRow;
								frame.addRow(cells, headers);
							}
						}
					}
				}
			}		}
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME);
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
