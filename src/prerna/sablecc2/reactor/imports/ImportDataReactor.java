package prerna.sablecc2.reactor.imports;

import java.util.Iterator;
import java.util.List;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.querystruct.QueryStruct2;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.rdf.query.builder.SQLInterpreter2;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Utility;

public class ImportDataReactor extends AbstractReactor {

	@Override
	public void In() {
		curNoun("all");
	}

	@Override
	public Object Out() {
		return parentReactor;
	}
	
	public Object reduce(Iterator it) {
		return Out();
	}
	
	@Override
	public Object execute() {
		// this is greedy execution
		// will not return anything
		// but will update the frame in the pksl planner
		QueryStruct2 queryStruct = getQueryStruct();
		String engineName = queryStruct.getEngineName();
		ITableDataFrame frame = (ITableDataFrame)this.planner.getProperty("FRAME", "FRAME");
		
		Importer importer = (Importer) ImportFactory.getImporter(frame);
		
		IEngine engine = Utility.getEngine(engineName.trim());
		SQLInterpreter2 interp = new SQLInterpreter2(engine);
		interp.setQueryStruct(queryStruct);
		String importQuery = interp.composeQuery();
		IRawSelectWrapper iterator = WrapperManager.getInstance().getRawWrapper(engine, importQuery); //we can only import from a db...or can we import from a frame?
		
		//set values into the curReactor
		importer.put("G", frame);
		importer.put(PKQLEnum.API + "_EDGE_HASH", queryStruct.getReturnConnectionsHash());
		importer.put(PKQLEnum.API + "_QUERY_NUM_CELLS", 1.0);
		importer.put(PKQLEnum.API + "_ENGINE", engineName.trim());
		importer.put(PKQLEnum.API, iterator);
		importer.process();
		ITableDataFrame importedFrame = (ITableDataFrame)importer.getValue("G");
		System.out.println("IMPORTED FRAME CREATED WITH ROW COUNT: "+importedFrame.getNumRows());
		this.planner.addProperty("FRAME", "FRAME", importedFrame);
		
		return null;
	}

	private QueryStruct2 getQueryStruct() {
		GenRowStruct allNouns = getNounStore().getNoun("QUERYSTRUCT");
		QueryStruct2 queryStruct = null;
		if(allNouns != null) {
			NounMetadata object = (NounMetadata)allNouns.getNoun(0);
			return (QueryStruct2)object.getValue();
		} else {
			NounMetadata result = this.planner.getVariable("$RESULT");
			if(result.getNounName().equals("QUERYSTRUCT")) {
				queryStruct = (QueryStruct2)result.getValue();
			}
		}
		return queryStruct;
	}
	
	@Override
	public List<NounMetadata> getOutputs() {
		// no output
		return null;
	}

}


