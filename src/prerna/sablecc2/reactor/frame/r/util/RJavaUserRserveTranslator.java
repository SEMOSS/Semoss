package prerna.sablecc2.reactor.frame.r.util;

import java.util.List;
import java.util.Map;

import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RserveException;

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
		if (this.insight != null && this.insight.getUser() != null) {
			logger.info("User found for R");
			if (this.insight.getUser().getRConn() != null) {
				logger.info("R connection found for user");
				rcon = this.insight.getUser().getRConn();
			} else {
				logger.info("Establishing R connection for user");
				establishNewRConnection();
				this.insight.getUser().setRConn(rcon);
			}
		} else {
			logger.warn("User not found for R");
			establishNewRConnection();
		}
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
			rcon.eval("library(dplyr);");
			logger.info("Loaded packages dplyr");
			
			// initialize the r environment
			initREnv();
			setMemoryLimit();
		} catch (Exception e) {
			throw new IllegalArgumentException("ERROR ::: Could not find connection.\nPlease make sure Rserve is running and the following libraries are installed:\n"
							+ "1)splitstackshape\n 2)data.table\n 3)reshape2\n 4)stringr\n 5)lubridate\n 6)dplyr", e);
		}
	}

	@Override
	public Object executeR(String rScript) {
		try {
			logger.info("Running R: " + rScript);
			return rcon.eval(rScript);
		} catch (Exception e) {
			logger.warn(e);
		}
		return null;
	}

	@Override
	public void executeEmptyR(String rScript) {
		try {
			logger.info("Running R: " + rScript);
			rcon.voidEval(rScript);
		} catch (Exception e) {
			logger.warn(e);
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
