package prerna.cache;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.rosuda.JRI.Rengine;
import org.rosuda.REngine.Rserve.RConnection;

import prerna.engine.impl.r.RSingleton;
import prerna.sablecc.PKQLRunner;
import prerna.util.Utility;

@Deprecated
public final class RCacheUtility {

	protected static final Logger LOGGER = LogManager.getLogger(RCacheUtility.class.getName());

	private RCacheUtility() {
		
	}
	
	public static void generateNewRSessionAndLoadWorkspace(PKQLRunner pkqlRunner, String rFilePath, boolean useJri) {
		String rScript = "load(\"" + rFilePath.replace("\\", "/") + "\")";
		if(useJri) {
			Rengine retEngine = Rengine.getMainEngine();
			LOGGER.info("Connection right now is set to.. " + retEngine);
			if(retEngine == null) {
				try {
					// start the R Engine
					retEngine = new Rengine(null, true, null);
					LOGGER.info("Successfully created engine.. ");
		
					// load all the libraries
					Object ret = retEngine.eval("library(splitstackshape);");
					if(ret == null) {
						throw new ClassNotFoundException("Package splitstackshape could not be found!");
					}
					// data table
					ret = retEngine.eval("library(data.table);");
					if(ret == null) {
						throw new ClassNotFoundException("Package data.table could not be found!");
					}
					// reshape2
					ret = retEngine.eval("library(reshape2);");
					if(ret == null) {
						throw new ClassNotFoundException("Package reshape2 could not be found!");
					}
					// rjdbc
					ret = retEngine.eval("library(RJDBC);");
					if(ret == null) {
						throw new ClassNotFoundException("Package RJDBC could not be found!");
					}
					// stringr
					ret = retEngine.eval("library(stringr);");
					if(ret == null) {
						throw new ClassNotFoundException("Package stringr could not be found!");
					}
				} catch(ClassNotFoundException e) {
					System.out.println("ERROR ::: " + e.getMessage() + "\nMake sure you have all the following libraries installed:\n"
							+ "1)splitstackshape\n"
							+ "2)data.table\n"
							+ "3)reshape2\n"
							+ "4)RJDBC*\n"
							+ "5)stringr\n\n"
							+ "*Please note RJDBC might require JAVA_HOME environment path to be defined on your system.");
					e.printStackTrace();
				}
			}
			// evaluate the rScript to load the workspace
			retEngine.eval(rScript);
			// store in pkql runner
//			pkqlRunner.setVariableValue(AbstractRJavaReactor.R_ENGINE, retEngine);
		} else {
			RConnection retCon = null;
			String port = null;
			LOGGER.info("Connection right now is set to.. " + retCon);
			try {
				RConnection masterCon = RSingleton.getConnection();
				port = Utility.findOpenPort();
				
				LOGGER.info("Starting it on port.. " + port);
				// need to find a way to get a common name
				masterCon.eval("library(Rserve); Rserve(port = " + port + ")");
				retCon = new RConnection("127.0.0.1", Integer.parseInt(port));
				// load all the libraries
				retCon.eval("library(splitstackshape);");
				// data table
				retCon.eval("library(data.table);");
				// reshape2
				retCon.eval("library(reshape2);");
				// rjdbc
				retCon.eval("library(RJDBC);");
				// stringr
				retCon.eval("library(stringr)");

				// evaluate the rScript to load the workspace
				retCon.eval(rScript);
			} catch (Exception e) {
				System.out.println("ERROR ::: Could not find connection.\nPlease make sure RServe is running and the following libraries are installed:\n"
						+ "1)Rserve\n"
						+ "2)splitstackshape\n"
						+ "3)data.table\n"
						+ "4)reshape2\n"
						+ "5)RJDBC*\n"
						+ "6)stringr\n\n"
						+ "*Please note RJDBC might require JAVA_HOME environment path to be defined on your system.");
				e.printStackTrace();
			}
			// store in pkql runner
//			pkqlRunner.setVariableValue(AbstractRJavaReactor.R_CONN, retCon);
//			pkqlRunner.setVariableValue(AbstractRJavaReactor.R_PORT, port);
		}
		
	}
	
}
