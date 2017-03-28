package prerna.sablecc2.reactor.imports;


import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.rdf.query.builder.SQLInterpreter2;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Utility;

import java.util.Iterator;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.querystruct.QueryStruct2;

public class ImportDataReactor extends AbstractReactor {


	@Override
	public void In() {
		curNoun("all");
	}

	@Override
	//greedy execution
	public Object Out() {
		importToFrame();
		return parentReactor;
	}
	
	public void updatePlan() {
		//there is no
	}

	
	public Object reduce(Iterator it) {
		return Out();
	}

	@Override
	protected void mergeUp() {
		//this reactor should not need to merge up
		if(parentReactor != null) {
			
		}
	}
	
	private void importToFrame()  {		
		
		QueryStruct2 queryStruct = getQueryStruct();
		String engineName = queryStruct.getEngineName();
		ITableDataFrame frame = (ITableDataFrame)this.planner.getProperty("FRAME", "FRAME");
		
		Importer importer = (Importer) ImportFactory.getImporter(frame);
		
		IEngine engine = Utility.getEngine(engineName.trim());
		SQLInterpreter2 interp = new SQLInterpreter2(engine);
		interp.setQueryStruct(queryStruct);
		String importQuery = interp.composeQuery();
		IRawSelectWrapper iterator = WrapperManager.getInstance().getRawWrapper(engine, importQuery);
		
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
	}

	@Override
	public Vector<NounMetadata> getInputs() {
		return null;
	}
	
	private QueryStruct2 getQueryStruct() {
		GenRowStruct allNouns = getNounStore().getNoun("QUERYSTRUCT");
		QueryStruct2 queryStruct = null;
		if(allNouns != null) {
			NounMetadata object = (NounMetadata)allNouns.get(0);
			return (QueryStruct2)object.getValue();
		} else {
			NounMetadata result = this.planner.getVariable("$RESULT");
			if(result.getNounName().equals("QUERYSTRUCT")) {
				queryStruct = (QueryStruct2)result.getValue();
			}
		}
		return queryStruct;
	}
}


