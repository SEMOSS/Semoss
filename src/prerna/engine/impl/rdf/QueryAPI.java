package prerna.engine.impl.rdf;

import java.util.Arrays;
import java.util.Hashtable;
import java.util.Iterator;

import prerna.ds.QueryStruct;
import prerna.engine.api.IApi;
import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.rdf.query.builder.IQueryInterpreter;
import prerna.test.TestUtilityMethods;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class QueryAPI implements IApi {
	
	Hashtable <String, Object> values = new Hashtable<String, Object>();
	String [] params = {"QUERY_STRUCT", "ENGINE"};
	public static final String USE_CHEATER = "useCheater";
	
	@Override
	public Iterator process() {
		// get the query struct
		QueryStruct qs = (QueryStruct) values.get(params[0]); 
		// get the engine
		String engineName = (values.get(params[1]) + "").trim();
		// if the engine is in DIHelper, it will grab it
		// otherwise, it will load the engine using the smss and return it
		IEngine engine = Utility.getEngine(engineName);

		// logic that if a person is trying to query an engine
		// and if the query struct is empty
		// just pull all the information from the engine
		if(qs.isEmpty()) {
			// we cannot override the existing query struct since
			// it is used in APIReactor to get the edgeHash
			// so we need to update the existing one to get the new values
			// TODO should expose this on the QueryStruct object itself
			QueryStruct newQs = engine.getDatabaseQueryStruct();
			qs.selectors = newQs.selectors;
			qs.relations = newQs.relations;
			qs.andfilters = newQs.andfilters;
		}
		qs.print();
		IQueryInterpreter interp = engine.getQueryInterpreter();
		interp.setQueryStruct(qs);
		String query = interp.composeQuery();

		// in order to support legacy GDM insights
		// need to determine if we should get a select wrapper or a cheater wrapper
		if(values.containsKey(QueryAPI.USE_CHEATER) && (boolean) values.get(QueryAPI.USE_CHEATER)) {
			return WrapperManager.getInstance().getChWrapper(engine, query);
		} else {
			// return the raw wrapper
			return WrapperManager.getInstance().getRawWrapper(engine, query);
		}
	}
	
	@Override
	public void set(String key, Object value) {
		values.put(key, value);
	}

	@Override
	public String[] getParams() {
		return params;
	}
	
	///////////////// TEST ITERATOR CREATION //////////////////////
	public static void main(String[] args) {
		TestUtilityMethods.loadDIHelper();

		// load the database
		String engineProp = "C:\\workspace\\Semoss_Dev\\db\\Movie_RDBMS.smss";
		RDBMSNativeEngine coreEngine = new RDBMSNativeEngine();
		coreEngine.setEngineName("Movie_RDBMS");
		coreEngine.openDB(engineProp);
		coreEngine.setEngineName("Movie_RDBMS");
		DIHelper.getInstance().setLocalProperty("Movie_RDBMS", coreEngine);
		
		// testing using the api from the entire engine
		QueryAPI api = new QueryAPI();
		api.set("QUERY_STRUCT", coreEngine.getDatabaseQueryStruct());
		api.set("ENGINE", coreEngine.getEngineName());
		Iterator<IHeadersDataRow> it = api.process();
		
		while(it.hasNext()) {
			IHeadersDataRow row = it.next();
			System.out.println(Arrays.toString(row.getValues()));
		}
	}
}
