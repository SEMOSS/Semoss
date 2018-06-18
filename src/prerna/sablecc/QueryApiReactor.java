package prerna.sablecc;

import java.util.Iterator;

import prerna.engine.api.IEngine;
import prerna.engine.impl.rdf.AbstractApiReactor;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc.meta.IPkqlMetadata;
import prerna.util.Utility;

public class QueryApiReactor extends AbstractApiReactor {
	
	@Override
	public Iterator process() {
		super.process();
		
		// if the engine is in DIHelper, it will grab it
		// otherwise, it will load the engine using the smss and return it
		// we grab the engine from the AbstractApiReactor
		IEngine engine = Utility.getEngine(this.engineName);

		// logic that if a person is trying to query an engine
		// and if the query struct is empty
		// just pull all the information from the engine
		// we grab the qs from the AbstractApiReactor
//		if(this.qs.isEmpty()) {
//			// we cannot override the existing query struct since
//			// it is used in APIReactor to get the edgeHash
//			// so we need to update the existing one to get the new values
//			// TODO should expose this on the QueryStruct object itself
//			QueryStruct newQs = engine.getDatabaseQueryStruct();
//			this.qs.selectors = newQs.selectors;
//			this.qs.relations = newQs.relations;
//			this.qs.andfilters = newQs.andfilters;
//			
//			// need to set a new edge hash using this information!!!
//			// since the abstract api reactor will set one where the edge hash
//			// will be empty
//			Map<String, Set<String>> edgeHash = this.qs.getReturnConnectionsHash();
//			// we store the edge hash in myStore
//			this.put("EDGE_HASH", edgeHash);
//		}
//		this.qs.print();
		
		Iterator thisIterator = null;
		// in order to support legacy GDM insights
		// need to determine if we should get a select wrapper or a cheater wrapper
		// we grab this boolean from the AbstractApiReactor
//		if(this.useCheater) {
//			IQueryInterpreter2 interp = engine.getQueryInterpreter2();
//			interp.setQueryStruct(this.qs);
//			String query = interp.composeQuery();
//			thisIterator = WrapperManager.getInstance().getChWrapper(engine, query);
//		} else {
			// return the raw wrapper
			long startTime = System.currentTimeMillis();
			thisIterator = WrapperManager.getInstance().getRawWrapper(engine, this.qs);
			long endTime = System.currentTimeMillis();
			
			System.out.println("Query execution time = " + (endTime - startTime) + " ms");
//		}
		
		// now also perform a count on the query to determine if it is the right size
		// currently only use this in H2Importing
		// TODO: move this logic out
//		if(myStore.get("G") != null && (myStore.get("G") instanceof ITableDataFrame)  ) {
//			ITableDataFrame frame = (ITableDataFrame) myStore.get("G");
//			// only need to run query for count if frame is not in memory
//			if(frame instanceof H2Frame && ((H2Frame) frame).isInMem()) {
//				// modify the qs that this is using
//				qs.setPerformCount(QueryStruct.COUNT_CELLS);
//				IRawSelectWrapper countIt = WrapperManager.getInstance().getRawWrapper(engine, this.qs);
//				if(countIt.hasNext()) {
//					Object numCells = countIt.next().getValues()[0]; 
//					System.out.println("QUERY CONTAINS NUM_CELLS = " + numCells);
//					
//					this.put("QUERY_NUM_CELLS", numCells);
//				}
//			}
//		}
		
		this.put((String) getValue(PKQLEnum.API), thisIterator);
		this.put("RESPONSE", "success");
		this.put("STATUS", PKQLRunner.STATUS.SUCCESS);
		
		return null;
	}

	@Override
	public IPkqlMetadata getPkqlMetadata() {
		// TODO Auto-generated method stub
		return null;
	}

	///////////////// TEST ITERATOR CREATION //////////////////////
//	public static void main(String[] args) {
//		TestUtilityMethods.loadDIHelper();
//
//		// load the database
//		String engineProp = "C:\\workspace\\Semoss_Dev\\db\\Movie_RDBMS.smss";
//		RDBMSNativeEngine coreEngine = new RDBMSNativeEngine();
//		coreEngine.setEngineName("Movie_RDBMS");
//		coreEngine.openDB(engineProp);
//		coreEngine.setEngineName("Movie_RDBMS");
//		DIHelper.getInstance().setLocalProperty("Movie_RDBMS", coreEngine);
//		
//		// testing using the api from the entire engine
//		QueryAPI api = new QueryAPI();
//		api.put("G", new H2Frame());
//		api.put("QUERY_STRUCT", coreEngine.getDatabaseQueryStruct());
//		api.put("ENGINE", coreEngine.getEngineName());
//		api.process();
//		Iterator<IHeadersDataRow> it = (Iterator<IHeadersDataRow>) api.getValue(PKQLEnum.API);
//		while(it.hasNext()) {
//			IHeadersDataRow row = it.next();
//			System.out.println(Arrays.toString(row.getValues()));
//		}
//	}
}
