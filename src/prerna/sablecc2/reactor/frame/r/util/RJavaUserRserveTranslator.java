package prerna.sablecc2.reactor.frame.r.util;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.rosuda.REngine.REXP;
import org.rosuda.REngine.REXPMismatchException;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RserveException;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.impl.r.RUserRserve;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class RJavaUserRserveTranslator extends AbstractRJavaTranslator {
	
	private static final long R_TIMEOUT = 7L;
	private static final TimeUnit R_TIMEOUT_UNIT = TimeUnit.HOURS;
	
	private static final String R_FOLDER = (DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/" + "R" + "/" + "Temp" + "/").replace('\\', '/');
	private static final String R_DATA_EXT = ".RData";
	private static final boolean ENABLE_RECOVERY = true; // TODO >>>timb: R - eventually make this configurable
	
	private RConnection anonymousRcon;
	private String rDataFile; // TODO >>>timb: R - move this to the user object?
	
	private static Object anonymousMonitor = new Object();
	
	@Override
	public void startR() {
		synchronized (getMonitor()) {
			
			// Define the RData file
			if (userIsDefined()) {
				rDataFile = R_FOLDER + getUserInfo() + R_DATA_EXT;
			} else {
				rDataFile = R_FOLDER + "anonymous" + R_DATA_EXT;
			}
			
			// Define the r connection
			if (rConnIsDefined()) {
				logger.info("Using existing R connection for user " + getUserInfo());
			} else {
				logger.info("Establishing new R connection for user " + getUserInfo());
				establishNewRConnection();
			}
		}
	}
	
	private RConnection rcon() {
		if (rConnIsDefined()) {
			return this.insight.getUser().getRConn();
		} else {
			return anonymousRcon;
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
			return String.join("-", userNames);
		} else {
			return "anonymous";
		}
	}
	
	private boolean userIsDefined() {
		logger.info("User is " + this.insight.getUser().toString()); // TODO >>>timb: R - remove this debug log
		return this.insight != null && this.insight.getUser() != null;
	}
	
	private boolean rConnIsDefined() {
		logger.info("RConn is " + this.insight.getUser().getRConn()); // TODO >>>timb: R - remove this debug log
		return userIsDefined() && this.insight.getUser().getRConn() != null;
	}
		
	private void establishNewRConnection() {
		try {
			RConnection rcon = RUserRserve.createConnection();
			
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
//			rcon.eval("library(dplyr);");
//			logger.info("Loaded packages dplyr");
						
			// Set the R connection on the user object
			if (userIsDefined()) {
				this.insight.getUser().setRConn(rcon);
				logger.info("Set user object " + this.insight.getUser().toString()); // TODO >>>timb: R - remove this debug log
			} else {
				anonymousRcon = rcon;
			}
			
			// initialize the r environment
			initREnv();
			setMemoryLimit();
		} catch (Exception e) {
			throw new IllegalArgumentException("ERROR ::: Could not find connection.\nPlease make sure Rserve is running and the following libraries are installed:\n"
							+ "1)splitstackshape\n 2)data.table\n 3)reshape2\n 4)stringr\n 5)lubridate\n 6)dplyr", e);
		}
	}
	
	@Override
	public void initREnv() {
		try {
			rcon().eval("if(!exists(\"" + this.env + "\")) {" + this.env  + "<- new.env();}");
		} catch (RserveException e) {
			throw new IllegalArgumentException("Failed to establish R environment.");
		}
	}

	private boolean isHealthy() {
		return RUserRserve.isHealthy(rcon(), logger);
	}
		
	@Override
	public Object executeR(String rScript) {
		return rconEval(rScript);
	}
	
	// TODO >>>timb: R - retest the sync stuff
		
	private REXP rconEval(String rScript) {
		synchronized (getMonitor()) {
			if (isHealthy()) {
				logger.info("Running R: " + rScript);
				ExecutorService executor = Executors.newSingleThreadExecutor(); // TODO >>>timb: R - need to refactor these execs
				try {
					Future<REXP> future = executor.submit(new Callable<REXP>() {
						@Override
						public REXP call() throws Exception {
							if (ENABLE_RECOVERY) saveImage();
							return rcon().eval(rScript);
						}
					});
					try {
						return future.get(R_TIMEOUT, R_TIMEOUT_UNIT);
					} catch (TimeoutException | InterruptedException e) {
						throw new IllegalArgumentException("Timout occured when running R script.", e);
					} catch (ExecutionException e) {
						throw new IllegalArgumentException("Failed to run R script.", e);
					} catch (CancellationException e) {
						throw new IllegalArgumentException("R script was cancelled.", e);
					} finally {
						logger.warn(future.isCancelled() + "<- WHETHER CANCELLED"); // TODO >>>timb: R - remove
					}
				} finally {
					executor.shutdownNow();
				}
			} else {
				establishNewRConnection();
				throw new IllegalArgumentException("R was not working properly but has succesfully recovered; however, your data in R has been lost.");
			}
		}
	}

	@Override
	public void executeEmptyR(String rScript) {
		synchronized (getMonitor()) {
			if (isHealthy()) {
				logger.info("Running R: " + rScript);
					ExecutorService executor = Executors.newSingleThreadExecutor();
					try {
						Future<Void> future = executor.submit(new Callable<Void>() {
							@Override
							public Void call() throws Exception {
								if (ENABLE_RECOVERY) saveImage();
								rcon().voidEval(rScript);
								return null;
							}
						});
						try {
							future.get(R_TIMEOUT, R_TIMEOUT_UNIT);
						} catch (TimeoutException | InterruptedException e) {
							throw new IllegalArgumentException("Timout occured when running R script.", e);
						} catch (ExecutionException e) {
							throw new IllegalArgumentException("Failed to run R script.", e);
						} catch (CancellationException e) {
							throw new IllegalArgumentException("R script was cancelled.", e);
						}
					} finally {
						executor.shutdownNow();
					}
			} else {
				establishNewRConnection();
				throw new IllegalArgumentException("R was not working properly but has succesfully recovered; however, your data in R has been lost.");
			}
		}
	}
	
	@Override
	public boolean cancelExecution() {
		logger.info("Cancelling R execution.");
		boolean cancelled = false;
		if (rcon() != null) {
			try {
				logger.info("Closing R connection.");
//				this.insight.getUser().setRConnCancelled(true); TODO >>>:timb R - need 

				if (rcon().close()) {
					logger.info("Creating a new R connection.");
					establishNewRConnection();
					if (ENABLE_RECOVERY) {
						logger.info("Loading session.");
						loadImage(); // TODO >>>timb: R - if this works, go ahead and add to recovery
					}
					cancelled = true;
				}

			} catch (Exception e) {
				logger.warn("Failed to cancel R execution.", e);
			}
		} else {
			logger.warn("No R connections found; nothing to cancel.");
		}
		return cancelled;
	}

	private Object getMonitor() {
		return userIsDefined() ? this.insight.getUser() : anonymousMonitor;
	}
	
	private void saveImage() throws RserveException { // TODO >>>timb: R - need to delete these files when done
		if (rDataFile == null) throw new IllegalArgumentException("Cannot save workspace image, as the RData file location is not defined.");
		if (!new File(rDataFile).getParentFile().exists()) throw new IllegalArgumentException("Cannot save workspace image, as the RData file folder is not defined.");
		rcon().voidEval("save.image(file = \""+ rDataFile + "\")");
	}
	
	private void loadImage() throws RserveException {
		if (rDataFile == null) throw new IllegalArgumentException("Cannot load workspace image, as the RData file location is not defined.");
		if (!new File(rDataFile).exists()) throw new IllegalArgumentException("Cannot load workspace image, as the RData file is not defined.");
		rcon().voidEval("load(\"" + rDataFile + "\")");
	}
	
	@Override
	public String getString(String rScript) {
		try {
			return rconEval(rScript).asString();
		} catch (REXPMismatchException e) {
			throw new IllegalArgumentException("R did not evaluate to a string.");
		}
	}

	@Override
	public String[] getStringArray(String rScript) {
		try {
			return rconEval(rScript).asStrings();
		} catch (REXPMismatchException e) {
			throw new IllegalArgumentException("R did not evaluate to a string array.");
		}
	}

	@Override
	public int getInt(String rScript) {
		try {
			return rconEval(rScript).asInteger();
		} catch (REXPMismatchException e) {
			throw new IllegalArgumentException("R did not evaluate to an integer.");
		}
	}

	@Override
	public int[] getIntArray(String rScript) {
		try {
			return rconEval(rScript).asIntegers();
		} catch (REXPMismatchException e) {
			throw new IllegalArgumentException("R did not evaluate to an integer array.");
		}
	}

	@Override
	public double getDouble(String rScript) {
		try {
			return rconEval(rScript).asDouble();
		} catch (REXPMismatchException e) {
			throw new IllegalArgumentException("R did not evaluate to a double.");
		}
	}

	@Override
	public double[] getDoubleArray(String rScript) {
		try {
			return rconEval(rScript).asDoubles();
		} catch (REXPMismatchException e) {
			throw new IllegalArgumentException("R did not evaluate to a double array.");
		}
	}

	@Override
	public double[][] getDoubleMatrix(String rScript) {
		try {
			return rconEval(rScript).asDoubleMatrix();
		} catch (REXPMismatchException e) {
			throw new IllegalArgumentException("R did not evaluate to a double matrix.");
		}
	}

	@Override
	public boolean getBoolean(String rScript) {
		try {
			return rconEval(rScript).asInteger() == 1;
		} catch (REXPMismatchException e) {
			throw new IllegalArgumentException("R did not evaluate to a inter.");
		}
	}

	@Override
	public Object getFactor(String rScript) {
		try {
			return rconEval(rScript).asFactor();
		} catch (REXPMismatchException e) {
			throw new IllegalArgumentException("R did not evaluate to a factor.");
		}
	}

	public RConnection getConnection() {
		if (userIsDefined()) {
			return this.insight.getUser().getRConn();
		}
			else{
				return null;
			}
	}
	
	@Override
	public void setConnection(RConnection connection) {
		anonymousRcon = connection;
		if (userIsDefined()) {
			this.insight.getUser().setRConn(connection);
		}
	}

	@Override
	public void setPort(String port) {
		// This is not necessary as port is hard coded to 6311
	}

	@Override
	public void endR() {
		try {
			RUserRserve.stopRserve();
		} catch (Exception e) {
			logger.warn("Unable to shut down R.", e);
		}
	}

	@Override
	public void stopRProcess() {
		try {
			rcon().detach();
		} catch (RserveException e) {
			logger.warn("Unable to stop R process.", e);
		}
	}

	@Override
	public Map<String, Object> getHistogramBreaksAndCounts(String script) {
		try {
			double[] breaks;
			@SuppressWarnings("unchecked")
			Map<String, Object> histJ = (Map<String, Object>) (rconEval(script).asNativeJavaObject());
			if (histJ.get("breaks") instanceof int[]) {
				int[] breaksInt = (int[]) histJ.get("breaks");
				breaks = Arrays.stream(breaksInt).asDoubleStream().toArray();
			} else {
				breaks = (double[]) histJ.get("breaks");
			}
			int[] counts = (int[]) histJ.get("counts");

			Map<String, Object> retMap = new HashMap<String, Object>();
			retMap.put("breaks", breaks);
			retMap.put("counts", counts);
			return retMap;
		} catch (REXPMismatchException e) {
			throw new IllegalArgumentException("R did not evaluate to the proper data type.", e);
		}
	}

	@Override
	public Map<String, Object> flushFrameAsTable(String framename, String[] colNames) {
		List<Object[]> dataMatrix = new ArrayList<Object[]>();

		int numCols = colNames.length;
		for (int i = 0; i < numCols; i++) {
			String script = framename + "$" + colNames[i];
			REXP val = rconEval(script);

			if (val.isNumeric()) {
				// for a double array
				try {
					double[] rows = val.asDoubles();
					int numRows = rows.length;
					if (dataMatrix.isEmpty()) {
						initEmptyMatrix(dataMatrix, numRows, numCols);
					}
					for (int j = 0; j < numRows; j++) {
						dataMatrix.get(j)[i] = rows[j];
					}
					continue;
				} catch (REXPMismatchException e) {
					logger.debug(e);
				}
				// in case values cannot be doubles
				try {
					int[] rows = val.asIntegers();
					int numRows = rows.length;
					if (dataMatrix.isEmpty()) {
						initEmptyMatrix(dataMatrix, numRows, numCols);
					}
					for (int j = 0; j < numRows; j++) {
						dataMatrix.get(j)[i] = rows[j];
					}
					continue;
				} catch (REXPMismatchException e) {
					logger.debug(e);
				}
				// in case values cannot be put into an array
				// for an integer
				try {
					int row = val.asInteger();
					if (dataMatrix.isEmpty()) {
						initEmptyMatrix(dataMatrix, 1, numCols);
					}
					dataMatrix.get(0)[i] = row;
					continue;
				} catch (REXPMismatchException e) {
					logger.debug(e);
				}

			} else {
				// for a string array
				try {
					String[] rows = val.asStrings();
					int numRows = rows.length;
					if (dataMatrix.isEmpty()) {
						initEmptyMatrix(dataMatrix, numRows, numCols);
					}
					for (int j = 0; j < numRows; j++) {
						dataMatrix.get(j)[i] = rows[j];
					}
					continue;
				} catch (REXPMismatchException e) {
					logger.debug(e);
				}
				// for a string
				try {
					String row = val.asString();
					if (dataMatrix.isEmpty()) {
						initEmptyMatrix(dataMatrix, 1, numCols);
					}
					dataMatrix.get(0)[i] = row;
					continue;
				} catch (REXPMismatchException e) {
					logger.debug(e);
				}
			}
		}

		Map<String, Object> retMap = new HashMap<String, Object>();
		retMap.put("headers", colNames);
		retMap.put("data", dataMatrix);

		return retMap;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Object[] getDataRow(String rScript, String[] headerOrdering) {
		REXP rs = rconEval(rScript);
		Object[] retArray = null;
		Object result = null;
		try {
			result = rs.asNativeJavaObject();
		} catch (REXPMismatchException e) {
			throw new IllegalArgumentException("R did not evaluate to the proper data type.", e);
		}
		if(result instanceof Map) {
			retArray =  processMapReturn((Map<String, Object>) result, headerOrdering).get(0);
		} else if(result instanceof List) {
			String[] returnNames = null;
			try {
				Object namesAttr = rs.getAttribute("names").asNativeJavaObject();
				if(namesAttr instanceof String[]) {
					returnNames = (String[]) namesAttr;
				} else {
					// assume it is single string
					returnNames = new String[]{namesAttr.toString()};
				}
			} catch (REXPMismatchException e) {
				throw new IllegalArgumentException("R did not evaluate to the proper data type.", e);
			}
			retArray = (Object[]) processListReturn((List<Object[]>) result, headerOrdering, returnNames).get(0);
		} else {
			throw new IllegalArgumentException("Unknown data type returned from R");
		}
		
		return retArray;
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<Object[]> getBulkDataRow(String rScript, String[] headerOrdering) {
		REXP rs = rconEval(rScript);
		Object result = null;
		try {
			result = rs.asNativeJavaObject();
		} catch (REXPMismatchException e) {
			throw new IllegalArgumentException("R did not evaluate to the proper data type.", e);
		}
		if(result instanceof Map) {
			return processMapReturn((Map<String, Object>) result, headerOrdering);
		} else if(result instanceof List) {
			String[] returnNames = null;
			try {
				Object namesAttr = rs.getAttribute("names").asNativeJavaObject();
				if(namesAttr instanceof String[]) {
					returnNames = (String[]) namesAttr;
				} else {
					// assume it is single string
					returnNames = new String[]{namesAttr.toString()};
				}
			} catch (REXPMismatchException e) {
				throw new IllegalArgumentException("R did not evaluate to the proper data type.", e);
			}
			return processListReturn((List<Object[]>) result, headerOrdering, returnNames);
		} else {
			throw new IllegalArgumentException("Unknown data type returned from R");
		}
	}
	
	private List<Object[]> processMapReturn(Map<String, Object> result,  String[] headerOrdering) {
		List<Object[]> retArr = new Vector<Object[]>(500);
		int numColumns = headerOrdering.length;
		for(int idx = 0; idx < numColumns; idx++) {
			Object val = result.get(headerOrdering[idx]);

			if(val instanceof Object[]) {
				Object[] data = (Object[]) val;
				if(retArr.size() == 0) {
					for(int i = 0; i < data.length; i++) {
						Object[] values = new Object[numColumns];
						values[idx] = data[i];
						retArr.add(values);
					}
				} else {
					for(int i = 0; i < data.length; i++) {
						Object[] values = retArr.get(i);
						values[idx] = data[i];
					}
				}
			} else if(val instanceof double[]) {
				double[] data = (double[]) val;
				if(retArr.size() == 0) {
					for(int i = 0; i < data.length; i++) {
						Object[] values = new Object[numColumns];
						values[idx] = data[i];
						retArr.add(values);
					}
				} else {
					for(int i = 0; i < data.length; i++) {
						Object[] values = retArr.get(i);
						values[idx] = data[i];
					}
				}
			} else if(val instanceof int[]) {
				int[] data = (int[]) val;
				if(retArr.size() == 0) {
					for(int i = 0; i < data.length; i++) {
						Object[] values = new Object[numColumns];
						values[idx] = data[i];
						retArr.add(values);
					}
				} else {
					for(int i = 0; i < data.length; i++) {
						Object[] values = retArr.get(i);
						values[idx] = data[i];
					}
				}
			} else if (val instanceof String) {
				String data = (String) val;
				if (retArr.size() == 0) {
					Object[] values = new Object[numColumns];
					values[idx] = data;
					retArr.add(values);
				} else {
					Object[] values = retArr.get(0);
					values[idx] = data;
				}
			} else if (val instanceof Double) {
				Double data = (Double) val;
				if (retArr.size() == 0) {
					Object [] values = new Object[numColumns];
					values[idx] = data;
					retArr.add(values);
				} else {
					Object[] values = retArr.get(0);
					values [idx] = data;
				}	
			} else if (val instanceof Integer){
				Integer data = (Integer) val;
				if (retArr.size() == 0) {
					Object [] values = new Object [numColumns];
					values[idx] = data;
					retArr.add(values);
				} else {
					Object [] values = retArr.get(0);
					values [idx] = data;
				}
			} else {
				throw new IllegalArgumentException("Could not identify the return type for this iterator.");
			}
		}
		return retArr;
	}
	
	private List<Object[]> processListReturn(List<Object[]> result, String[] headerOrdering, String[] returnNames) {
		List<Object[]> retArr = new Vector<Object[]>(500);

		// match the returns based on index
		int numHeaders = headerOrdering.length;
		int[] headerIndex = new int[numHeaders];
		for(int i = 0; i < numHeaders; i++) {
			headerIndex[i] = ArrayUtilityMethods.arrayContainsValueAtIndex(returnNames, headerOrdering[i]);
		}
		
		for(int i = 0; i < numHeaders; i++) {
			// grab the right column index
			int columnIndex = headerIndex[i];
			// each column comes back as an array
			// need to first initize my return matrix
			Object col = result.get(columnIndex);
			if(col instanceof Object[]) {
				Object[] columnResults = (Object[]) col;
				int numResults = columnResults.length;
				if(retArr.size() == 0) {
					for(int j = 0; j < numResults; j++) {
						Object[] values = new Object[numHeaders];
						values[i] = columnResults[j];
						retArr.add(values);
					}
				} else {
					for(int j = 0; j < numResults; j++) {       
						Object[] values = retArr.get(j);
						values[i] = columnResults[j];
					}
				}
			} else if(col instanceof double[]) {
				double[] columnResults = (double[]) col;
				int numResults = columnResults.length;
				if(retArr.size() == 0) {
					for(int j = 0; j < numResults; j++) {
						Object[] values = new Object[numHeaders];
						values[i] = columnResults[j];
						retArr.add(values);
					}
				} else {
					for(int j = 0; j < numResults; j++) {
						Object[] values = retArr.get(j);
						values[i] = columnResults[j];
					}
				}
			} else if(col instanceof int[]) {
				int[] columnResults = (int[]) col;
				int numResults = columnResults.length;
				if(retArr.size() == 0) {
					for(int j = 0; j < numResults; j++) {
						Object[] values = new Object[numHeaders];
						values[i] = columnResults[j];
						retArr.add(values);
					}
				} else {
					for(int j = 0; j < numResults; j++) {
						Object[] values = retArr.get(j);
						values[i] = columnResults[j];
					}
				}
			}
		}
		
		return retArr;
	}	

}
