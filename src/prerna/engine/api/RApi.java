package prerna.engine.api;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

import prerna.ds.QueryStruct;
import prerna.engine.impl.r.RRunner;

/**
 * Used in ApiReactor to create an R dataframe based off of only columns selected by the user
 * @author kepark
 *
 */
public class RApi implements IApi{
	
	Hashtable <String, Object> values = new Hashtable<String, Object>();
	String [] params = {"QUERY_STRUCT", "R_RUNNER", "TABLE_NAME"};

	@Override
	public String[] getParams() {
		return params;
	}

	@Override
	public void set(String key, Object value) {
		values.put(key, value);
	}

	@Override
	public Iterator process() {
		QueryStruct qs = (QueryStruct) values.get(params[0]); 
		Hashtable<String, Vector<String>> selectors = qs.getSelectors();
		String columns = "";
		for(String column : selectors.keySet()) {
			columns += column + ",";
		}
//		SQLInterpreter interp = new SQLInterpreter();
//		interp.setQueryStruct(qs); // currently constructs query as SELECT  M.PRIM_KEY_PLACEHOLDER AS MovieBudget , T.PRIM_KEY_PLACEHOLDER AS Title  FROM MovieBudget  M , Title, need to rework interpreter to be able to set table
		String query = "SELECT " + columns.substring(0, columns.lastIndexOf(",")) + " FROM " + values.get(params[2]); // appending table name to end of query; see if there's a better way to set table/froms for interpreter
		String script = "dataframe <- dbGetQuery(conn, '" + query + "'); "; // R script that stores query result in R variable named dataframe
		
		RRunner r = (RRunner)values.get(params[1]);
		r.evaluateScript(script);
		if (r.getScriptRanSuccessfully()) {
			r.setDataframeExists(true);
		}
		
		return null; // until H2Frame can run a query on itself, can't generate a wrapper from query
	}

}
