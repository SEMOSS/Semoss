package prerna.sablecc;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.ds.QueryStruct;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdf.AbstractApiReactor;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.rdf.query.builder.IQueryInterpreter;
import prerna.util.Utility;

public class SearchQueryApiReactor extends AbstractApiReactor {
	
	@Override
	public Iterator process() {
		super.process();
		
		// if the engine is in DIHelper, it will grab it
		// otherwise, it will load the engine using the smss and return it
		// we grab the engine from the AbstractApiReactor
		IEngine engine = Utility.getEngine(this.engine);

		// logic that if a person is trying to query an engine
		// and if the query struct is empty
		// just pull all the information from the engine
		// we grab the qs from the AbstractApiReactor
		if(this.qs.isEmpty()) {
			// we cannot override the existing query struct since
			// it is used in APIReactor to get the edgeHash
			// so we need to update the existing one to get the new values
			// TODO should expose this on the QueryStruct object itself
			QueryStruct newQs = engine.getDatabaseQueryStruct();
			this.qs.selectors = newQs.selectors;
			this.qs.relations = newQs.relations;
			this.qs.andfilters = newQs.andfilters;
			
			// need to set a new edge hash using this information!!!
			// since the abstract api reactor will set one where the edge hash
			// will be empty
			Map<String, Set<String>> edgeHash = this.qs.getReturnConnectionsHash();
			// we store the edge hash in myStore
			this.put("EDGE_HASH", edgeHash);
		} else {
			boolean countSet = setCount();
			if(!countSet) {
				setOrderBy();
			}
		}
				
		this.qs.print();
		
		this.put((String) getValue(PKQLEnum.API), this.qs);
		this.put("RESPONSE", "success");
		this.put("STATUS", PKQLRunner.STATUS.SUCCESS);
		
		return null;
	}

	private void setOrderBy() {
		//we want to set the order by column as the first selector
		if(myStore.containsKey(PKQLEnum.COL_CSV) && ((Vector)myStore.get(PKQLEnum.COL_CSV)).size() > 0) {
			List<String> selectors = (Vector<String>) myStore.get(PKQLEnum.COL_CSV);
			String orderBy = selectors.get(0);
			if(orderBy.contains("__")){
				String concept = orderBy.substring(0, orderBy.indexOf("__"));
				String property = orderBy.substring(orderBy.indexOf("__")+2);
				this.qs.setOrderBy(concept, property);
			}
			else
			{
				this.qs.setOrderBy(orderBy, null);
			}
		}
	}
	
	private boolean setCount() {
		if(this.mapOptions != null) {
			String getCount = mapOptions.get("getCount").toString();
			if(getCount.toLowerCase().equals("true")) {
				this.qs.setPerformCount(true);
				return true;
			} else {
				this.qs.setPerformCount(false);
				return false;
			}
		}
		return false;
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
