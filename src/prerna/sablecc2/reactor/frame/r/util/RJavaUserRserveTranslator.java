package prerna.sablecc2.reactor.frame.r.util;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.rosuda.REngine.REXP;
import org.rosuda.REngine.REXPMismatchException;
import org.rosuda.REngine.Rserve.RConnection;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.WorkspaceAssetUtils;
import prerna.engine.impl.r.IRUserConnection;
import prerna.engine.impl.r.RserveUtil;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class RJavaUserRserveTranslator extends AbstractRJavaTranslator {

	private IRUserConnection rcon;
	
	
	////////////////////////////////////////
	// Starting R
	////////////////////////////////////////
	@Override
	public void startR() {
		
		// Try to start R
		try {
			
			// First define the rcon 
			if (userRconIsDefined()) {
				rcon = this.insight.getUser().getRcon();
			} else {
				if (userIsDefined()) {
					this.insight.getUser().setRcon(IRUserConnection.getRUserConnection(getRDataFile()));
					rcon = this.insight.getUser().getRcon();
				} else {
					rcon = IRUserConnection.getRUserConnection();
				}
				
				// Then initialize
				rcon.initializeConnection();
				rcon.loadDefaultPackages();
				initREnv(); // TODO>>>timb: R - why do we need these? They are not called in recover methods of the RUserConnection framework (later)
				setMemoryLimit();
			}
		} catch (Exception e) {
			
			// If r fails to start, don't preserve the improperly loaded object
			if (rcon != null) {
				try {rcon.stopR();} catch (Exception ignore) {}
			}
			throw new IllegalArgumentException("Failed to start R.", e);
		}
	}

	private boolean userRconIsDefined() {
		return userIsDefined() && this.insight.getUser().getRcon() != null;
	}
	
	private boolean userIsDefined() {
		return this.insight != null && this.insight.getUser() != null;
	}
	
	private String getUserInfo() {
		User user = this.insight.getUser();
		AuthProvider primaryProvider = user.getPrimaryLogin();
		AccessToken token = user.getAccessToken(primaryProvider);
		String userInfo = primaryProvider.name() + "___" + token.getName().replaceAll(" ", "_");
		return userInfo; 
	}
	
	private String getRDataFile() {
		if (userIsDefined()) {
			if (Boolean.parseBoolean(DIHelper.getInstance().getProperty(Constants.USER_WORKSPACE))) {
				String assetDirectory = WorkspaceAssetUtils.getUserAssetRootDirectory(this.insight.getUser(), this.insight.getUser().getPrimaryLogin());
				if (assetDirectory != null && new File(assetDirectory).isDirectory()) {
					String rDataDirectory = assetDirectory + "/" + "RData";
					new File(rDataDirectory).mkdir();
					return RserveUtil.getRDataFile(rDataDirectory, getUserInfo());
				}
			}
			return RserveUtil.getRDataFile(getUserInfo());
		} else {
			return RserveUtil.getRDataFile("anonymous");
		}
	}
	
	@Override
	public void initREnv() {
		try {
			rcon.eval("if(!exists(\"" + this.env + "\")) {" + this.env  + "<- new.env();}");
		} catch (Exception e) {
			throw new IllegalArgumentException("Failed to initialize R environment.", e);
		}
	}
	
	
	////////////////////////////////////////
	// R evaluations
	////////////////////////////////////////
	@Override
	public Object executeR(String rScript) {
		return rcon.eval(rScript);
	}
	
	@Override
	public void executeEmptyR(String rScript) {
		rcon.voidEval(rScript);
	}
	
	
	////////////////////////////////////////
	// Cancellation
	////////////////////////////////////////
	@Override
	public boolean cancelExecution() {
		return false;
		// TODO >>>timb: R - need to complete cancellation here (later)
	}
	
	
	////////////////////////////////////////
	// Raw R connection
	////////////////////////////////////////
	// TODO >>>timb: R - should get rid of this (later)
	@Deprecated
	public RConnection getConnection() {
		return rcon.getRConnection();
	}
	
	
	////////////////////////////////////////
	// Data types
	////////////////////////////////////////
	@Override
	public String getString(String rScript) {
		try {
			return rcon.eval(rScript).asString();
		} catch (REXPMismatchException e) {
			throw new IllegalArgumentException("R did not evaluate to a string.");
		}
	}

	@Override
	public String[] getStringArray(String rScript) {
		try {
			return rcon.eval(rScript).asStrings();
		} catch (REXPMismatchException e) {
			throw new IllegalArgumentException("R did not evaluate to a string array.");
		}
	}

	@Override
	public int getInt(String rScript) {
		try {
			return rcon.eval(rScript).asInteger();
		} catch (REXPMismatchException e) {
			throw new IllegalArgumentException("R did not evaluate to an integer.");
		}
	}

	@Override
	public int[] getIntArray(String rScript) {
		try {
			return rcon.eval(rScript).asIntegers();
		} catch (REXPMismatchException e) {
			throw new IllegalArgumentException("R did not evaluate to an integer array.");
		}
	}

	@Override
	public double getDouble(String rScript) {
		try {
			return rcon.eval(rScript).asDouble();
		} catch (REXPMismatchException e) {
			throw new IllegalArgumentException("R did not evaluate to a double.");
		}
	}

	@Override
	public double[] getDoubleArray(String rScript) {
		try {
			return rcon.eval(rScript).asDoubles();
		} catch (REXPMismatchException e) {
			throw new IllegalArgumentException("R did not evaluate to a double array.");
		}
	}

	@Override
	public double[][] getDoubleMatrix(String rScript) {
		try {
			return rcon.eval(rScript).asDoubleMatrix();
		} catch (REXPMismatchException e) {
			throw new IllegalArgumentException("R did not evaluate to a double matrix.");
		}
	}

	@Override
	public boolean getBoolean(String rScript) {
		try {
			return rcon.eval(rScript).asInteger() == 1;
		} catch (REXPMismatchException e) {
			throw new IllegalArgumentException("R did not evaluate to a inter.");
		}
	}

	@Override
	public Object getFactor(String rScript) {
		try {
			return rcon.eval(rScript).asFactor();
		} catch (REXPMismatchException e) {
			throw new IllegalArgumentException("R did not evaluate to a factor.");
		}
	}

	@Override
	public void setConnection(RConnection connection) {
		// This is not necessary as connection is allocated per user
	}

	@Override
	public void setPort(String port) {
		// This is not necessary as port is allocated per user
	}

	@Override
	/**
	 * Stops all r processes.
	 */
	public void endR() {
		try {
			IRUserConnection.endR();
		} catch (Exception e) {
			logger.warn("Unable to end R.", e);
		}
	}

	@Override
	public void stopRProcess() {
		try {
			rcon.detach();
		} catch (Exception e) {
			logger.warn("Unable to stop R process.", e);
		}
	}

	@Override
	public Map<String, Object> getHistogramBreaksAndCounts(String script) {
		try {
			double[] breaks;
			@SuppressWarnings("unchecked")
			Map<String, Object> histJ = (Map<String, Object>) (rcon.eval(script).asNativeJavaObject());
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
			REXP val = rcon.eval(script);

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
		REXP rs = rcon.eval(rScript);
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
		REXP rs = rcon.eval(rScript);
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
