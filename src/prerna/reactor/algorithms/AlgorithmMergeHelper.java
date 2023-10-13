package prerna.reactor.algorithms;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.query.querystruct.LambdaQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.reactor.imports.IImporter;
import prerna.reactor.imports.ImportFactory;
import prerna.sablecc2.om.Join;
import prerna.sablecc2.om.execptions.SemossPixelException;

public class AlgorithmMergeHelper {

	private AlgorithmMergeHelper() {
		
	}
	
	public static ITableDataFrame mergeSimpleAlgResult(ITableDataFrame dataFrame, String colNmae, String algorithmColName, String algorithmColType, AlgorithmSingleColStore results) {
		// set the headers in the result so the iterator it generates is accurate
		String[] cleanHeaders = new String[2];
		SemossDataType[] types = new SemossDataType[2];
		//merge data to frame
		LambdaQueryStruct qs = new LambdaQueryStruct();
		// fill in QS with new header info
		Map<String, String> dataTypes = new HashMap<String, String>();
		QueryColumnSelector instanceSelector = new QueryColumnSelector();
		String existingUniqueColName = dataFrame.getMetaData().getUniqueNameFromAlias(colNmae);
		if(existingUniqueColName == null) {
			existingUniqueColName = colNmae;
		}
		if(existingUniqueColName.contains("__")) {
			String[] split = existingUniqueColName.split("__");
			instanceSelector.setTable(split[0]);
			instanceSelector.setColumn(split[1]);
			dataTypes.put(split[1], dataFrame.getMetaData().getHeaderTypeAsString(existingUniqueColName, split[0]));
			cleanHeaders[0] = split[1];
		} else {
			instanceSelector.setTable(existingUniqueColName);
			instanceSelector.setColumn(SelectQueryStruct.PRIM_KEY_PLACEHOLDER);
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
			instanceClusterSelector.setColumn(SelectQueryStruct.PRIM_KEY_PLACEHOLDER);
			dataTypes.put(algorithmColName, algorithmColType);
			cleanHeaders[1] = algorithmColName;
		}

		qs.addSelector(instanceSelector);
		qs.addSelector(instanceClusterSelector);
		qs.addRelation(existingUniqueColName, algorithmColName, "left.outer.join");
		qs.setColumnTypes(dataTypes);
		
		results.setHeaders(cleanHeaders);
		types[0] = SemossDataType.convertStringToDataType(dataTypes.get(cleanHeaders[0]));
		types[1] = SemossDataType.convertStringToDataType(dataTypes.get(cleanHeaders[1]));
		results.setTypes(types);
		
		IImporter importer = ImportFactory.getImporter(dataFrame, qs, results);
		List<Join> joins = new ArrayList<Join>();
		Join j = new Join(existingUniqueColName, "left.outer.join", existingUniqueColName);
		joins.add(j);
		try {
			return importer.mergeData(joins);
		} catch (Exception e) {
			e.printStackTrace();
			throw new SemossPixelException(e.getMessage());
		}
	}
}
