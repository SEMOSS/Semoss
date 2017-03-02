package prerna.sablecc;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.rosuda.REngine.Rserve.RserveException;

import prerna.ds.h2.H2Frame;
import prerna.engine.impl.r.RRunner;
import prerna.engine.impl.rdf.AbstractApiReactor;
import prerna.sablecc.meta.IPkqlMetadata;

/**
 * Used in ApiReactor to create an R dataframe based off of only columns selected by the user
 * @author kepark
 *
 */
public class RApiReactor extends AbstractApiReactor {
	
	private String tableName = null;
	private RRunner rRunner = null;

	@Override
	public Iterator process() {
		if (myStore.get(PKQLEnum.G) instanceof H2Frame) {
			super.process();
			H2Frame frame = (H2Frame) myStore.get(PKQLEnum.G);
			try {
				this.tableName = frame.getDatabaseMetaData().get("tableName");
				this.rRunner = frame.getRRunner();
			} catch (RserveException e) {
				myStore.put("RESPONSE", "Error: R server is down.");
				myStore.put("STATUS", PKQLRunner.STATUS.ERROR);
				e.printStackTrace();
			} catch (SQLException e) {
				myStore.put("RESPONSE", "Error: Invalid database connection.");
				myStore.put("STATUS", PKQLRunner.STATUS.ERROR);
				e.printStackTrace();
			}
		} else {
			myStore.put("RESPONSE", "Error: Dataframe must be in Grid format.");
			myStore.put("STATUS", PKQLRunner.STATUS.ERROR);
			return null;
		} 
		
		
		Map<String, List<String>> selectors = this.qs.getSelectors();
		String columns = "";
		for(String column : selectors.keySet()) {
			columns += column + ",";
		}
//		String connectionScript = "conn<-dbConnect(drv,'" + r.getUrl() + "','" + r.getUsername() + "','')";
//		r.evaluateScript(connectionScript);
//		SQLInterpreter interp = new SQLInterpreter();
//		interp.setQueryStruct(qs); // currently constructs query as SELECT  M.PRIM_KEY_PLACEHOLDER AS MovieBudget , T.PRIM_KEY_PLACEHOLDER AS Title  FROM MovieBudget  M , Title, need to rework interpreter to be able to set table
		String query = "SELECT " + columns.substring(0, columns.lastIndexOf(",")) + " FROM " + this.tableName; // appending table name to end of query; see if there's a better way to set table/froms for interpreter
		String queryScript = "dataframe<-as.data.frame(unclass(dbGetQuery(conn,'" + query + "')));"; // R script that stores query result in R variable named dataframe
		
		this.rRunner.evaluateScript(queryScript);
		if (rRunner.getScriptRanSuccessfully()) {
			rRunner.setDataframeExists(true);
		}
		
		myStore.put("RESPONSE", "success");
		myStore.put("STATUS", PKQLRunner.STATUS.SUCCESS);
		
		return null; // until H2Frame can run a query on itself, can't generate a wrapper from query
	}

	@Override
	public IPkqlMetadata getPkqlMetadata() {
		// TODO Auto-generated method stub
		return null;
	}
}
