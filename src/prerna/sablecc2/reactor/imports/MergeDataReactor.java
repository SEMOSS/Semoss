package prerna.sablecc2.reactor.imports;


import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.rdf.query.builder.SQLInterpreter2;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.Join;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Utility;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.querystruct.QueryStruct2;

public class MergeDataReactor extends AbstractReactor {


	@Override
	public void In() {
		curNoun("all");
	}

	@Override
	public Object Out() {
		importToFrame();
		return null;
	}
	
	@Override
	public void updatePlan() {

	}


	@Override
	protected void mergeUp() {

	}
	
	private void importToFrame()  {
		//get the inputs
		
		QueryStruct2 queryStruct = getQueryStruct();
		String engineName = queryStruct.getEngineName();
		
		GenRowStruct allNouns = getNounStore().getNoun(NounStore.all); //should be only joins
		ITableDataFrame frame = (ITableDataFrame)this.planner.getProperty("FRAME", "FRAME");
		
		Importer curReactor = ImportFactory.getImporter(frame);
		
		SQLInterpreter2 interp;
		Iterator<IHeadersDataRow> iterator;
		if(engineName != null) {
			IEngine engine = Utility.getEngine(engineName.trim());
			interp = new SQLInterpreter2(engine);
			interp.setQueryStruct(queryStruct);
			String importQuery = interp.composeQuery();
			iterator = WrapperManager.getInstance().getRawWrapper(engine, importQuery);
			curReactor.put(PKQLEnum.API + "_ENGINE", engineName.trim());
		} else {
			interp = new SQLInterpreter2();
			interp.setQueryStruct(queryStruct);
			String query = interp.composeQuery();
			iterator = frame.query(query);
		}
		
		//set values into the curReactor
		curReactor.put("G", frame);
		curReactor.put(PKQLEnum.API + "_EDGE_HASH", queryStruct.getReturnConnectionsHash());
		curReactor.put(PKQLEnum.API + "_QUERY_NUM_CELLS", 1.0);
		
		curReactor.put(PKQLEnum.API, iterator);
		if(allNouns != null) {
			Vector<Map<String, String>> joinCols = getJoinCols(allNouns);
			curReactor.put(PKQLEnum.JOINS, joinCols);
			
		}
		curReactor.process();
		ITableDataFrame importedFrame = (ITableDataFrame)curReactor.getValue("G");
		System.out.println("IMPORTED FRAME CREATED WITH ROW COUNT: "+importedFrame.getNumRows());
		this.planner.addProperty("FRAME", "FRAME", importedFrame);
			
		
	}

	private QueryStruct2 getQueryStruct() {
		GenRowStruct allNouns = getNounStore().getNoun("QUERYSTRUCT");
		QueryStruct2 queryStruct = null;
		if(allNouns != null) {
			NounMetadata object = (NounMetadata)allNouns.get(0);
			return (QueryStruct2)object.getValue();
		} 
		
		return queryStruct;
	}
	
	@Override
	public List<NounMetadata> getInputs() {
		return null;
	}
	
	private Vector<Map<String,String>> getJoinCols(GenRowStruct joins) {
		
		Vector<Map<String, String>> joinCols = new Vector<>();
		for(int i = 0; i < joins.size(); i++) {
			if(joins.get(i) instanceof Join) {
				Join join = (Join)joins.get(i);
				String toCol = join.getQualifier();
				String fromCol = join.getSelector();
				String joinType = join.getJoinType();
				
				Map<String, String> joinMap = new HashMap<>(1);
				joinMap.put(PKQLEnum.TO_COL, toCol);
				joinMap.put(PKQLEnum.FROM_COL, fromCol);
				joinMap.put(PKQLEnum.REL_TYPE, joinType);
				
				joinCols.add(joinMap);
			}
		}
		return joinCols;
	}
}


