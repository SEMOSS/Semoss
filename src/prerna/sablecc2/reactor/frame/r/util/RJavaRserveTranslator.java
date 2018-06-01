package prerna.sablecc2.reactor.frame.r.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.rosuda.REngine.REXP;
import org.rosuda.REngine.REXPMismatchException;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RserveException;

import prerna.engine.impl.r.RSingleton;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.runtime.AbstractBaseRClass;
import prerna.util.ArrayUtilityMethods;

public class RJavaRserveTranslator extends AbstractRJavaTranslator {

	RConnection retCon;
	String port;

	/**
	 * Constructor only accesssible through the package
	 * Please use the insight object or the RJavaTranslatorFactory
	 * to get the correct instance
	 */
	RJavaRserveTranslator() {

	}

	/**
	 * This will start R, only if it has not already been started
	 * In this case we are establishing a connection for Rserve
	 */
	@Override
	public void startR() {
		if(this.insight != null) {
			NounMetadata noun = (NounMetadata) this.insight.getVarStore().get(R_CONN);
			if (noun != null) {
				retCon = (RConnection) this.insight.getVarStore().get(R_CONN).getValue();
			}
			NounMetadata nounPort = this.insight.getVarStore().get(R_PORT);
			if (nounPort != null) {
				port = (String) nounPort.getValue();
			}
		}
		logger.info("Connection right now is set to.. " + retCon);
		if (this.retCon == null) {
			try {
				this.retCon = RSingleton.getConnection();
//				port = Utility.findOpenPort();
//				logger.info("Starting it on port.. " + port);
//				// need to find a way to get a common name
//				masterCon.eval("library(Rserve); Rserve(port = " + port + ")");
//				retCon = new RConnection("127.0.0.1", Integer.parseInt(port));
				
				// load all the libraries
				retCon.eval("library(splitstackshape);");
				logger.info("Loaded packages splitstackshape");
				// data table
				retCon.eval("library(data.table);");
				logger.info("Loaded packages data.table");
				// reshape2
				retCon.eval("library(reshape2);");
				logger.info("Loaded packages reshape2");
				// stringr
				retCon.eval("library(stringr)");
				logger.info("Loaded packages stringr");
				// lubridate
				retCon.eval("library(lubridate);");
				logger.info("Loaded packages lubridate");
				// dplyr
				retCon.eval("library(dplyr);");
				logger.info("Loaded packages dplyr");
				
				if(this.insight != null) {
					this.insight.getVarStore().put(AbstractBaseRClass.R_CONN, new NounMetadata(retCon, PixelDataType.R_CONNECTION));
					this.insight.getVarStore().put(AbstractBaseRClass.R_PORT, new NounMetadata(port, PixelDataType.CONST_STRING));
				}
				
				// initialize the r environment
				initREnv();
			} catch (Exception e) {
				System.out.println(
						"ERROR ::: Could not find connection.\nPlease make sure RServe is running and the following libraries are installed:\n"
								+ "1)splitstackshape\n 2)data.table\n 3)reshape2\n 4)stringr\n 5)lubridate\n 6)dplyr");
				e.printStackTrace();
				throw new IllegalArgumentException(
						"ERROR ::: Could not find connection.\nPlease make sure RServe is running and the following libraries are installed:\n"
								+ "1)splitstackshape\n 2)data.table\n 3)reshape2\n 4)stringr\n 5)lubridate\n 6)dplyr");
			}
		}
	}

	@Override
	public Object executeR(String rScript) {
		try {
			return retCon.eval(rScript);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	@Override
	public void executeEmptyR(String rScript) {
		try {
			retCon.voidEval(rScript);
		} catch (RserveException e) {
			e.printStackTrace();
		}
	}

	@Override
	public String getString(String script) {
		try {
			return retCon.eval(script).asString();
		} catch (RserveException e) {
			e.printStackTrace();
		} catch (REXPMismatchException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public String[] getStringArray(String script) {
		try {
			return retCon.eval(script).asStrings();
		} catch (RserveException e) {
			e.printStackTrace();
		} catch (REXPMismatchException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public int getInt(String script) {
		int number = 0;
		try {
			number = retCon.eval(script).asInteger();
			return number;
		} catch (RserveException e) {
			e.printStackTrace();
		} catch (REXPMismatchException e) {
			e.printStackTrace();
		}
		return number;
	}

	@Override
	public int[] getIntArray(String script) {
		try {
			return retCon.eval(script).asIntegers();
		} catch (RserveException e) {
			e.printStackTrace();
		} catch (REXPMismatchException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public double getDouble(String script) {
		double number = 0;
		try {
			number = retCon.eval(script).asDouble();
			return number;
		} catch (RserveException e) {
			e.printStackTrace();
		} catch (REXPMismatchException e) {
			e.printStackTrace();
		}
		return number;
	}

	@Override
	public double[] getDoubleArray(String script) {
		try {
			return retCon.eval(script).asDoubles();
		} catch (RserveException e) {
			e.printStackTrace();
		} catch (REXPMismatchException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	@Override
	public double[][] getDoubleMatrix(String script) {
		try {
			return retCon.eval(script).asDoubleMatrix();
		} catch (RserveException e) {
			e.printStackTrace();
		} catch (REXPMismatchException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public Object getFactor(String script) {
		try {
			return retCon.eval(script).asFactor();
		} catch (RserveException e) {
			e.printStackTrace();
		} catch (REXPMismatchException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	@Override
	public boolean getBoolean(String script) {
		// 1 = TRUE, 0 = FALSE
		try {
			REXP val = retCon.eval(script);
			if(val != null) {
				return (val.asInteger() == 1);
			}
		} catch (RserveException e) {
			e.printStackTrace();
		} catch (REXPMismatchException e) {
			e.printStackTrace();
		}
		return false;
	}
	
	@Override
	public Map<String, Object> getHistogramBreaksAndCounts(String script) {
		try {
			Map<String, Object> histJ = (Map<String, Object>) (retCon.eval(script).asNativeJavaObject());
			double[] breaks = (double[]) histJ.get("breaks");
			int[] counts = (int[]) histJ.get("counts");
			
			Map<String, Object> retMap = new HashMap<String, Object>();
			retMap.put("breaks", breaks);
			retMap.put("counts", counts);
			return retMap;
		} catch (RserveException e) {
			e.printStackTrace();
		} catch (REXPMismatchException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public Map<String, Object> flushFrameAsTable(String framename, String[] colNames) {
		List<Object[]> dataMatrix = new ArrayList<Object[]>();
		
		int numCols = colNames.length;
		for (int i = 0; i < numCols; i++) {
			String script = framename + "$" + colNames[i];
			REXP val = (REXP) executeR(script);

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
				} catch (REXPMismatchException rme) {
					rme.printStackTrace();
				}
				//in case values cannot be doubles
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
				} catch (REXPMismatchException rme) {
					rme.printStackTrace();
				}
				//in case values cannot be put into an array
				//for an integer
				try {
					int row = val.asInteger();
					if (dataMatrix.isEmpty()) {
						initEmptyMatrix(dataMatrix, 1, numCols);
					}
					dataMatrix.get(0)[i] = row;
					continue;
				} catch (REXPMismatchException rme) {
					rme.printStackTrace();
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
				} catch (REXPMismatchException rme) {
					rme.printStackTrace();
				}
				//for a string
				try {
					String row = val.asString();
					if (dataMatrix.isEmpty()) {
						initEmptyMatrix(dataMatrix, 1, numCols);
					}
					dataMatrix.get(0)[i] = row;
					continue;
				} catch (REXPMismatchException rme) {
					rme.printStackTrace();
				}
			}
		}
		
		Map<String, Object> retMap = new HashMap<String, Object>();
		retMap.put("headers", colNames);
		retMap.put("data", dataMatrix);
		
		return retMap;
	}
	
	@Override
	public List<Object[]> getBulkDataRow(String rScript, String[] headerOrdering) {
		REXP rs = (REXP) executeR(rScript);
		Object result = null;
		try {
			result = rs.asNativeJavaObject();
		} catch (REXPMismatchException e) {
			e.printStackTrace();
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
				e.printStackTrace();
			}
			return processListReturn((List) result, headerOrdering, returnNames);
		} else {
			throw new IllegalArgumentException("Unknown data type returned from R");
		}
	}
	
	@Override
	public Object[] getDataRow(String rScript, String[] headerOrdering) {
		REXP rs = (REXP) executeR(rScript);
		Object[] retArray = null;
		Object result = null;
		try {
			result = rs.asNativeJavaObject();
		} catch (REXPMismatchException e) {
			e.printStackTrace();
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
				e.printStackTrace();
			}
			retArray = (Object[]) processListReturn((List) result, headerOrdering, returnNames).get(0);
		} else {
			throw new IllegalArgumentException("Unknown data type returned from R");
		}
		
		return retArray;
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
				logger.info("ERROR ::: Could not identify the return type for this iterator!!!");
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
	
	@Override
	public void setConnection(RConnection connection) {
		if (connection != null) {
			this.retCon = connection;
		}
	}
	
	public RConnection getConnection() {
		return this.retCon;
	}

	@Override
	public void setPort(String port) {
		if (this.port != null) {
			this.port = port;
		}
	}
	
	public String getPort() {
		return this.port;
	}

	@Override
	public void endR() {
		// only have 1 connection
		// do not do this..
//		try {
//			if(retCon != null) {
//				retCon.shutdown();
//			}
//			// clean up other things
//			System.out.println("R Shutdown!!");
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
	}

	@Override
	public void initREnv() {
		try {
			if(this.retCon != null) {
				this.retCon.eval("if(!exists(\"" + this.env + "\")) {" + this.env  + "<- new.env();}");
			}
		} catch (RserveException e) {
			e.printStackTrace();
		}
	}
}
