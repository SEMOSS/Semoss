package prerna.sablecc2.reactor.imports;

import java.util.Map;

import prerna.algorithm.api.ITableDataFrame;
import prerna.query.interpreters.QueryStruct2;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.reactor.AbstractReactor;

public class ImportDataReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		// this is greedy execution
		// will not return anything
		// but will update the frame in the pksl planner
		QueryStruct2 queryStruct = getQueryStruct();
		ITableDataFrame frame = (ITableDataFrame) this.planner.getProperty("FRAME", "FRAME");
		
		Extractor extractor = new Extractor(queryStruct);
		Map<String, Object> extractedData = extractor.extractData();
		Importer importer = (Importer) ImportFactory.getImporter(frame);
		
		//set values into the curReactor
		importer.put("G", frame);
		for(String key : extractedData.keySet()) {
			Object data = extractedData.get(key);
			importer.put(key, data);
		}
		importer.process();
		
		ITableDataFrame importedFrame = (ITableDataFrame)importer.getValue("G");
		System.out.println("IMPORTED FRAME CREATED WITH ROW COUNT: "+importedFrame.getNumRows());
		this.planner.addProperty("FRAME", "FRAME", importedFrame);
		
		return new NounMetadata(importedFrame, PkslDataTypes.FRAME);
	}

	private QueryStruct2 getQueryStruct() {
		GenRowStruct allNouns = getNounStore().getNoun("QUERYSTRUCT");
		QueryStruct2 queryStruct = null;
		if(allNouns != null) {
			NounMetadata object = (NounMetadata)allNouns.getNoun(0);
			return (QueryStruct2)object.getValue();
		} else {
			NounMetadata result = this.planner.getVariableValue("$RESULT");
			if(result.getNounName().equals("QUERYSTRUCT")) {
				queryStruct = (QueryStruct2)result.getValue();
			}
		}
		return queryStruct;
	}
}


