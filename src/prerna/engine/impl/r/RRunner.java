package prerna.engine.impl.r;

import java.sql.SQLException;
import java.util.HashMap;

import org.h2.tools.Server;
import org.rosuda.REngine.REXP;
import org.rosuda.REngine.REXPMismatchException;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RserveException;

import prerna.util.Constants;
import prerna.util.DIHelper;

public class RRunner {
	
	private RConnection conn = null;
	private Server server = null;
	private String tableName = "";
	private String username = "";
	private boolean dataframeExists = false;
	private boolean scriptRanSuccessfully = false;
	
	/**
	 * Runs start method
	 * @param databaseMetaData Must contain "username" and "tableName" from H2Frame
	 * @throws RserveException Thrown when RConnection fails, usually because Rserve was not started locally
	 * @throws SQLException Thrown when Server could not be created
	 */
	public RRunner(HashMap<String, String> databaseMetaData) throws RserveException, SQLException {
		this.tableName = databaseMetaData.get("tableName");
		this.username = databaseMetaData.get("username");
		start();
	}
	
	/**
	 * Sets the RConnection and server to the values provided in method arguments, then runs start method
	 * @param databaseMetaData Must contain "username" and "tableName" from H2Frame
	 * @param conn
	 * @param server
	 * @throws RserveException Thrown when RConnection fails, usually because Rserve was not started locally
	 * @throws SQLException Thrown when Server could not be created
	 */
	public RRunner(HashMap<String, String> databaseMetaData, RConnection conn, Server server) throws RserveException, SQLException {
		this.tableName = databaseMetaData.get("tableName");
		this.username = databaseMetaData.get("username");		
		this.conn = conn;
		this.server = server;
		start();
	}
	
	/**
	 * Creates the RConnection and TCP Server if nonexistent, then starts the server and initializes the RJDBC connection
	 * @throws RserveException Thrown when RConnection fails, usually because Rserve was not started locally
	 * @throws SQLException Thrown when Server could not be created
	 */
	public String start() throws RserveException, SQLException {
		if (conn == null) {
			conn = new RConnection();
		} 
		if (server == null) {
			server = Server.createTcpServer("-tcpPort", "9999");
		}
		server.stop();
		server.start();
		return initializeRJDBCConnection();
	}
	
	/**
	 * Runs the script necessary to load the data from the H2Frame into R
	 * @param databaseMetaData Must contain "username" and "tableName" from H2Frame
	 * @throws RserveException 
	 */
	private String initializeRJDBCConnection() {
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER).replace("\\", "/");;
		String library = "RJDBC";
		String driver = "org.h2.Driver";
		String jar = "h2-1.4.185.jar"; // TODO: create an enum of available drivers and the necessary jar for each
		String url = "jdbc:h2:" + server.getURL() + "/mem:test:LOG=0;CACHE_SIZE=65536;LOCK_MODE=1;UNDO_LOG=0";
		String script = "drv <- JDBC('" + driver + "', '" + workingDir + "/RDFGraphLib/" + jar + "', identifier.quote='`'); " 
				+ "conn <- dbConnect(drv, '" + url + "', '" + username + "', ''); ";
		try {
			loadPackage(library);
		} catch (RserveException e) {
			e.printStackTrace();
			conn.finalize();
			conn = null;
			return "Error: RJDBC package must be installed locally.";
		}
		evaluateScript(script);
		if (scriptRanSuccessfully) {
			return "Success: RJDBC connection created.";
		}
		
		return "Error: Could not create RJDBC connection.";
	}
	
	public String createDefaultDataframe() throws RserveException {
		String script = "dataframe <- dbReadTable(conn, '" + tableName + "'); ";
		evaluateScript(script);
		return "Success: dataframe created.";
	}
	
	public void loadPackage(String library) throws RserveException {
		conn.voidEval("library(" + library + ")");
	}
	
	/**
	 * Executes R script using optional library if necessary for script to run and returns output from R
	 * @param library String of library name; null if no library is needed to execute script
	 * @param script String of R script to be evaluated
	 * @return REXP object containing R output
	 * @throws RserveException 
	 */
	public Object evaluateScript(String script) {
		REXP r = null;
		Object result = null;
//		script = "try(" + script + ",silent=TRUE)";
		try {
//			r = conn.parseAndEval(script);
			scriptRanSuccessfully = false;
			script = "paste(capture.output(print(" + script + ")),collapse='\\n')";
			r = conn.eval(script);
			scriptRanSuccessfully = true;
//			if (r.inherits("try-error")) {
//				result = "Error: ";
//				scriptRanSuccessfully = false;
//			}
//			if (r.isList()) {
//				RList list = r.asList();
//				if (list.size() != 0) {
//					int headerIndex = 0;
//					HashMap<String, String> dataframe = new HashMap<String, String>();
//					for(String header : list.keys()) {
//						dataframe.put(header, Arrays.toString(list.at(headerIndex).asStrings()));
//						headerIndex++;
//					}
//					result = dataframe;
//				} else {
					r = conn.eval(script);
					result = r.asString();
//				}
//			} else if (r.isVector()) {
//				result = r.asStrings();
//			} else {
//				result = r.asString();
//			}
		} catch (RserveException e) {
			e.printStackTrace();
			try {
				r = conn.eval("geterrmessage()");
				result = r.asString();
			} catch (RserveException | REXPMismatchException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		} catch (REXPMismatchException e) {
			e.printStackTrace();
		}
//		} catch (REXPMismatchException | REngineException e) {
//			e.printStackTrace();
//			conn.finalize();
//			conn = null;
//		}
		
		return result;
	}
		
	/**
	 * Closes RConnection and TCP Server
	 */
	public void close() {
		conn.finalize();
		conn = null;
		server.stop();
		server = null;
		dataframeExists = false;
	}
	
	public void setDataframeExists(boolean dataframeExists) {
		this.dataframeExists = dataframeExists;
	}
	
	public boolean getDataframeExists() {
		return this.dataframeExists;
	}
	
	public boolean getScriptRanSuccessfully() {
		return this.scriptRanSuccessfully;
	}

}
