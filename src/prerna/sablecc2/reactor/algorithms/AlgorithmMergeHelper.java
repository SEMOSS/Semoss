package prerna.sablecc2.reactor.algorithms;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.algorithm.api.ITableDataFrame;
import prerna.query.querystruct.AlgorithmQueryStruct;
import prerna.query.querystruct.QueryStruct2;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.Join;
import prerna.sablecc2.reactor.imports.IImporter;
import prerna.sablecc2.reactor.imports.ImportFactory;

public class AlgorithmMergeHelper {

	private AlgorithmMergeHelper() {
		
	}
	
	public static void mergeSimpleAlgResult(ITableDataFrame dataFrame, String existingUniqueColName, String algorithmColName, String algorithmColType, AlgorithmSingleColStore results) {
		// set the headers in the result so the iterator it generates is accurate
		String[] cleanHeaders = new String[2];
		//merge data to frame
		AlgorithmQueryStruct qs = new AlgorithmQueryStruct();
		// fill in QS with new header info
		Map<String, String> dataTypes = new HashMap<String, String>();
		QueryColumnSelector instanceSelector = new QueryColumnSelector();
		if(existingUniqueColName.contains("__")) {
			String[] split = existingUniqueColName.split("__");
			instanceSelector.setTable(split[0]);
			instanceSelector.setColumn(split[1]);
			dataTypes.put(split[1], dataFrame.getMetaData().getHeaderTypeAsString(existingUniqueColName, split[0]));
			cleanHeaders[0] = split[1];
		} else {
			instanceSelector.setTable(existingUniqueColName);
			instanceSelector.setColumn(QueryStruct2.PRIM_KEY_PLACEHOLDER);
			dataTypes.put(existingUniqueColName, dataFrame.getMetaData().getHeaderTypeAsString(existingUniqueColName, null));
			cleanHeaders[0] = existingUniqueColName;
		}
		
		QueryColumnSelector instanceClusterSelector = new QueryColumnSelector();
		if(algorithmColName.contains("__")) {
			String[] split = algorithmColName.split("__");
			instanceClusterSelector.setTable(split[0]);
			instanceClusterSelector.setColumn(split[1]);
			dataTypes.put(split[1], algorithmColType);
			cleanHeaders[1] = split[1];
		} else {
			instanceClusterSelector.setTable(algorithmColName);
			instanceClusterSelector.setColumn(QueryStruct2.PRIM_KEY_PLACEHOLDER);
			dataTypes.put(algorithmColName, algorithmColType);
			cleanHeaders[1] = algorithmColName;
		}

		qs.addSelector(instanceSelector);
		qs.addSelector(instanceClusterSelector);
		qs.setColumnTypes(dataTypes);
		
		results.setHeaders(cleanHeaders);
		IImporter importer = ImportFactory.getImporter(dataFrame, qs, results);
		List<Join> joins = new ArrayList<Join>();
		Join j = new Join(existingUniqueColName, "left.outer.join", existingUniqueColName);
		joins.add(j);
		importer.mergeData(joins);
	}
}
