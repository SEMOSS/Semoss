package prerna.sablecc2.reactor.frame.r.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.rosuda.REngine.REXPMismatchException;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RserveException;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.impl.r.RUserRserve;

public class RJavaUserRserveTranslator extends AbstractRJavaTranslator{
	
	RConnection rcon;
	
	@Override
	public void initREnv() {
		try {
			rcon.eval("if(!exists(\"" + this.env + "\")) {" + this.env  + "<- new.env();}");
		} catch (RserveException e) {
			throw new IllegalArgumentException("Failed to establish R environment.");
		}
	}

	@Override
	public void startR() {
		if (rConnIsDefined()) {
			logger.info("Using existing R connection for user " + getUserInfo());
			rcon = this.insight.getUser().getRConn();
		} else {
			logger.info("Establishing new R connection for user " + getUserInfo());
			establishNewRConnection();
		}
	}
	
	private String getUserInfo() {
		if (userIsDefined()) {
			List<String> userNames = new ArrayList<>();
			User user = this.insight.getUser();
			for (AuthProvider provider : user.getLogins()) {
				AccessToken token = user.getAccessToken(provider);
				userNames.add(token.getName());
			}
			return String.join(";", userNames);
		} else {
			return "anonymous";
		}
	}
	
	private boolean userIsDefined() {
		return this.insight != null && this.insight.getUser() != null;
	}
	
	private boolean rConnIsDefined() {
		return userIsDefined() && this.insight.getUser().getRConn() != null;
	}
	
	private void establishNewRConnection() {
		try {
			rcon = RUserRserve.createConnection();
			
			// load all the libraries
			// split stack shape
			rcon.eval("library(splitstackshape);");
			logger.info("Loaded packages splitstackshape");
			
			// data table
			rcon.eval("library(data.table);");
			logger.info("Loaded packages data.table");
			
			// reshape2
			rcon.eval("library(reshape2);");
			logger.info("Loaded packages reshape2");
			
			// stringr
			rcon.eval("library(stringr)");
			logger.info("Loaded packages stringr");
			
			// lubridate
			rcon.eval("library(lubridate);");
			logger.info("Loaded packages lubridate");
			
			// dplyr
			// TODO >>>timb: RUSER - uncomment this
			// rcon.eval("library(dplyr);");
			// logger.info("Loaded packages dplyr");
			
			// initialize the r environment
			initREnv();
			setMemoryLimit();
			
			// Set the R connection on the user object
			if (userIsDefined()) {
				this.insight.getUser().setRConn(rcon);
			}
		} catch (Exception e) {
			throw new IllegalArgumentException("ERROR ::: Could not find connection.\nPlease make sure Rserve is running and the following libraries are installed:\n"
							+ "1)splitstackshape\n 2)data.table\n 3)reshape2\n 4)stringr\n 5)lubridate\n 6)dplyr", e);
		}
	}

	private boolean isHealthy() {
		boolean isHealthy = false; // Healthy skepticism
		try {
			Object heartBeat = rcon.eval("1+2");
			if (heartBeat instanceof org.rosuda.REngine.REXP) {
				if (((org.rosuda.REngine.REXP) heartBeat).asDouble() == 3.0) {
					logger.info("R health check passed");
					isHealthy = true;
				}
			}
		} catch (RserveException | REXPMismatchException e) {
			logger.warn("R health check failed", e);
		}
		return isHealthy;
	}
	
	private String handleRException(Exception e) {
		String message;
		if (isHealthy()) {
			logger.warn("Script failed but R is healthy", e);
			message = "R is working properly, but an error occurred running the script.";
		} else {
			logger.info("R health check failed; attempting to establish a new R connection", e);
			establishNewRConnection();
			message = "R was not working properly but has succesfully recovered; however, your data in R has been lost.";
		}
		return message;
	}
	
	@Override
	public Object executeR(String rScript) {
		try {
			logger.info("Running R: " + rScript);
			return rcon.eval(rScript);
		} catch (RserveException e) {
			String message = handleRException(e);
			throw new IllegalArgumentException(message);
		}
	}

	@Override
	public void executeEmptyR(String rScript) {
		try {
			logger.info("Running R: " + rScript);
			rcon.voidEval(rScript);
		} catch (RserveException e) {
			String message = handleRException(e);
			throw new IllegalArgumentException(message);
		}
	}

	@Override
	public String getString(String script) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String[] getStringArray(String script) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getInt(String script) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int[] getIntArray(String rScript) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public double getDouble(String rScript) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double[] getDoubleArray(String rScript) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public double[][] getDoubleMatrix(String rScript) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean getBoolean(String rScript) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Object getFactor(String rScript) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setConnection(RConnection connection) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setPort(String port) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void endR() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void stopRProcess() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getHistogramBreaksAndCounts(String script) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, Object> flushFrameAsTable(String framename, String[] colNames) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object[] getDataRow(String rScript, String[] headerOrdering) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Object[]> getBulkDataRow(String rScript, String[] headerOrdering) {
		// TODO Auto-generated method stub
		return null;
	}
	

}
