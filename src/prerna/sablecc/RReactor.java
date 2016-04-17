package prerna.sablecc;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

import org.rosuda.REngine.REXP;
import org.rosuda.REngine.REXPMismatchException;
import org.rosuda.REngine.RList;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RserveException;

import prerna.ds.H2.TinkerH2Frame;
import prerna.sablecc.PKQLEnum.PKQLReactor;
import prerna.sablecc.PKQLEnum.PKQLToken;
import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * Reacts to PKQL commands invoking R scripts
 * @author kepark
 *
 */
public class RReactor extends AbstractReactor{
	
	public RReactor() {
		String[] thisReacts = {PKQLToken.CODE.toString()};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLReactor.R_OP.toString();
	}
	
	/**
	 * Test with H2Test
	 * @param args
	 */
	public static void main(String[] args) {
		String workingDir = "C:\\Users\\kepark\\workspaceKep\\SEMOSS-Dev".replace("\\", "/");
		String library = "RJDBC";
		String script = "drv <- JDBC('org.h2.Driver', '" + workingDir + "/RDFGraphLib/h2-1.4.185.jar', identifier.quote='`');" 
				+ "conn <- dbConnect(drv, 'jdbc:h2:tcp://192.168.56.1:9999/mem:test', 'sa', ''); "
				+ "d <- dbReadTable(conn, 'TINKERTABLE2');"
//				+ "d$size <- c('testValue');"
//				+ "dbWriteTable(conn, 'TINKERTABLE2', d, overwrite=TRUE);"
				;
		RReactor testReactor = new RReactor();
		try {
			RList list = testReactor.evaluateScript(script, library).asList();
			List headers = list.names;
			System.out.println(headers.toString());
			for(int i = 0; i < headers.size(); i++) {
				System.out.print(list.at(i).asString() + ", ");
			}
		} catch (RserveException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (REXPMismatchException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public Iterator process() {
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER).replace("\\", "/");;
		String library = "RJDBC";
		String driver = "org.h2.Driver";
		String jar = "h2-1.4.185.jar"; // TODO: create an enum of available drivers and the necessary jar for each
		String nodeStr = (String) myStore.get(whoAmI);
		String userScript = (String) myStore.get(PKQLToken.CODE.toString());
		String url = "";
		String username = "";
		
		// Error handling
		if (userScript == null || userScript.length() < 1) {
			myStore.put(nodeStr, "Error: No R script found.");
			myStore.put("STATUS", "error");
			return null;
		}
		if (myStore.get(PKQLEnum.G) instanceof TinkerH2Frame) {
			TinkerH2Frame frame = (TinkerH2Frame)myStore.get(PKQLEnum.G);
			try {
				url = frame.getJDBCURL().trim();
				username = frame.getUserName().trim();
			} catch (SQLException e) {
				myStore.put(nodeStr, "Error: Invalid database connection.");
				myStore.put("STATUS", "error");
			}
		} else {
			myStore.put(nodeStr, "Error: Dataframe is not Grid.");
			myStore.put("STATUS", "error");
			return null;
		}
		// End Error handling
		
//		String[] scriptArray = new String[4];
//		scriptArray[0] = "drv <- JDBC('" + driver + "', '" + workingDir + "/RDFGraphLib/" + jar + "', identifier.quote='`'); " ;
//		scriptArray[1] = "conn <- dbConnect(drv, '" + url + "', '" + username + "', ''); ";
//		scriptArray[2] = "dbListTables(conn); ";//"d <- dbReadTable(conn, 'TINKERTABLE2'); ";
//		scriptArray[3] = userScript;
		String script = "drv <- JDBC('" + driver + "', '" + workingDir + "/RDFGraphLib/" + jar + "', identifier.quote='`'); " 
				+ "conn <- dbConnect(drv, '" + url + "', '" + username + "', ''); "
//				+ "d <- dbReadTable(conn, 'TINKERFRAME2'); "; // TODO: allow user to select dataframe name rather than "d"
				;											 // TODO: find a way to get table name
		
		REXP rResult;
		try {
			rResult = evaluateScript(script + userScript, library);
//			rResult = evaluateScript(script + userScript, library);
			String result = rResult.asString();
			myStore.put(nodeStr, result);
			myStore.put("STATUS", "success");
		} catch (RserveException e) {
			myStore.put(nodeStr, "Error: Invalid R script or R server is down.");
			myStore.put("STATUS", "error");
		} catch (REXPMismatchException e) {
			myStore.put(nodeStr, "null");
			myStore.put("STATUS", "success");
		}
		
		return null;
	}
	
	/**
	 * Executes R script using optional library if necessary for script to run and returns output from R
	 * @param library String of library name; null if no library is needed to execute script
	 * @param script String of R script to be evaluated
	 * @return REXP object containing R output
	 * @throws RserveException Usually thrown when a script causes an error, or when Rserve is off
	 */
	public REXP evaluateScript(String script, String library) throws RserveException {
		REXP x = null;
		RConnection c;
		c = new RConnection();
		try {
			if(library != null) {
				c.voidEval("library(" + library + ")");
			}
			x = c.eval(script); // makes it so that the return from the final R script statement is returned
		} finally {
			c.finalize();
		}
		return x;
	}
	
	/**
	 * Same as evaulateScript, but useful for debugging as it can run one line of R script at a time
	 * @param scriptArray String Array of scripts to be evaulated
	 * @return REXP object containing R output
	 * @throws RserveException Usually thrown when a script causes an error, or when Rserve is off
	 */
	public REXP evaluateScriptArray(String[] scriptArray, String library) throws RserveException {
		REXP x = null;
		RConnection c = new RConnection();
		try {
			if(library != null) {
				c.voidEval("library(" + library + ")");
			}
			for(String s : scriptArray) {
				x = c.eval(s); // makes it so that the return from the final R script statement is returned
			}
		} finally {
			c.finalize();
		}
		return x;
	}

}
