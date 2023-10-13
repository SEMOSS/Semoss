package prerna.reactor.frame.r.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rosuda.REngine.REXP;
import org.rosuda.REngine.REXPMismatchException;
import org.rosuda.REngine.RFactor;
import org.rosuda.REngine.RList;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RserveException;

import prerna.engine.impl.r.RSingleton;
import prerna.reactor.runtime.AbstractBaseRClass;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class RJavaRserveTranslator extends AbstractRJavaTranslator {

	private static final Logger logger = LogManager.getLogger(RJavaRserveTranslator.class);

	private static ConcurrentMap<String, ReentrantLock> genEngineLock = new ConcurrentHashMap<>();
	
	RConnection retCon;
	String port;

	/**
	 * Constructor only accessible through the package
	 * Please use the insight object or the RJavaTranslatorFactory
	 * to get the correct instance
	 */
	public RJavaRserveTranslator() {

	}
	
	/**
	 * Create a new engine if it doesn't exist
	 * @return
	 */
	private static synchronized RConnection generateConnection() {
		return RSingleton.getConnection();
	}
	
	private static synchronized ReentrantLock getConnectionLock(String id) {
		genEngineLock.putIfAbsent(id, new ReentrantLock());
		return genEngineLock.get(id);
	}

	/**
	 * This will start R, only if it has not already been started
	 * In this case we are establishing a connection for Rserve
	 */
	@Override
	public void startR() {
		if(this.retCon == null && this.insight != null) {
			NounMetadata noun = this.insight.getVarStore().get(R_CONN);
			if (noun != null) {
				retCon = (RConnection) this.insight.getVarStore().get(R_CONN).getValue();
			}
			NounMetadata nounPort = this.insight.getVarStore().get(R_PORT);
			if (nounPort != null) {
				port = (String) nounPort.getValue();
			}
		}
		
		if(this.retCon == null) {
			this.retCon = RSingleton.getRCon();
			if(this.retCon == null) {
				logger.info("R Connection has not been defined yet...");
			}
		} else {
			logger.info("Retrieving existing R Connection...");
		}
		
		if (this.retCon == null) {
			try {
				ReentrantLock lock = getConnectionLock("genId");
				try {
					lock.lock();
					if(this.retCon == null) {
						logger.info("Starting R Connection... ");
						this.retCon = generateConnection();
						logger.info("Successfully created R Connection... ");
	
						// port = Utility.findOpenPort();
						// logger.info("Starting it on port.. " + port);
						// // need to find a way to get a common name
						// masterCon.eval("library(Rserve); Rserve(port = " + port + ")");
						// retCon = new RConnection("127.0.0.1", Integer.parseInt(port));
	
						// load all the libraries
						retCon.eval("suppressPackageStartupMessages(library(splitstackshape));");
						logger.info("Loaded packages splitstackshape");
					
						// data table
							
						retCon.eval("suppressPackageStartupMessages(library(data.table));");
						logger.info("Loaded packages data.table");
						// reshape2
						retCon.eval("suppressPackageStartupMessages(library(reshape2));");
						logger.info("Loaded packages reshape2");
						// stringr
						retCon.eval("suppressPackageStartupMessages(library(stringr));");
						logger.info("Loaded packages stringr");
						// lubridate
						retCon.eval("suppressPackageStartupMessages(library(lubridate));");
						logger.info("Loaded packages lubridate");
						// dplyr
						retCon.eval("suppressPackageStartupMessages(library(dplyr));");
						logger.info("Loaded packages dplyr");
	
						if(this.insight != null) {
							this.insight.getVarStore().put(AbstractBaseRClass.R_CONN, new NounMetadata(retCon, PixelDataType.R_CONNECTION));
							this.insight.getVarStore().put(AbstractBaseRClass.R_PORT, new NounMetadata(port, PixelDataType.CONST_STRING));
						}
	
						// initialize the r environment
						setMemoryLimit();
					}
				} catch (Exception e) {
					logger.error(
							"ERROR ::: Could not find connection.\nPlease make sure RServe is running and the following libraries are installed:\n "
									+ "1)splitstackshape\n 2)data.table\n 3)reshape2\n 4)stringr\n 5)lubridate\n 6)dplyr");
					logger.error(Constants.STACKTRACE, e);
					throw new IllegalArgumentException(
							"ERROR ::: Could not find connection.\nPlease make sure RServe is running and the following libraries are installed:\n "
									+ "1)splitstackshape\n 2)data.table\n 3)reshape2\n 4)stringr\n 5)lubridate\n 6)dplyr");
				} finally {
					lock.unlock();
				}
			} catch(Exception e) {
				logger.error(Constants.STACKTRACE, e);
			}
		}
		initREnv();
	}

	/**
	 * Run r synchronized
	 * @param rScript
	 * @return
	 * @throws RserveException 
	 */
	private synchronized REXP evalRSync(String rScript) throws RserveException {
		ReentrantLock lock = getConnectionLock("execR");
		lock.lock();
		try {
			logger.debug("Running rscript > " + rScript);
			return retCon.eval(rScript);
		} finally {
			lock.unlock();
		}
	}
	
	/**
	 * Run r synchronized void
	 * @param rScript
	 * @return
	 * @throws RserveException 
	 */
	private synchronized void voidEvalRSync(String rScript) throws RserveException {
		ReentrantLock lock = getConnectionLock("execR");
		lock.lock();
		try {
			logger.debug("Running rscript > " + rScript);
			retCon.voidEval(rScript);
		} finally {
			lock.unlock();
		}
	}
	
	@Override
	public synchronized Object executeR(String rScript) {
		try {
			// escape quotes
			//rScript = rScript.replaceAll("\"", "\\\"");
			//rScript = rScript.replace("'", "\\'");
			rScript = encapsulateForEnv(rScript);
			logger.debug("Running rscript > " + rScript);
			return evalRSync(rScript);
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		}
		return null;
	}
	
	@Override
	public synchronized void executeEmptyR(String rScript) {
		try {
			//rScript = rScript.replaceAll("\"", "\\\"");
			//rScript = rScript.replace("'", "\\'");
			rScript = encapsulateForEnv(rScript);
			logger.debug("Running rscript > " + rScript);
			voidEvalRSync(rScript);
		} catch (RserveException e) {
			logger.error(Constants.STACKTRACE, e);
		}
	}
	
	@Override
	public boolean cancelExecution() {
		// TODO >>>timb: R - need to complete cancel exec (later)
		return false;
	}

	@Override
	public String getString(String script) {
		try {
			script = encapsulateForEnv(script);
			return evalRSync(script).asString();
		} catch (RserveException re) {
			logger.error(Constants.STACKTRACE, re);
		} catch (REXPMismatchException e) {
			logger.error(Constants.STACKTRACE, e);
		}
		return null;
	}

	@Override
	public String[] getStringArray(String script) {
		try {
			script = encapsulateForEnv(script);
			return evalRSync(script).asStrings();
		} catch (RserveException re) {
			logger.error(Constants.STACKTRACE, re);
		} catch (REXPMismatchException e) {
			logger.error(Constants.STACKTRACE, e);
		}
		return null;
	}
	
	/**
	 * This method is used to get the column types of a frame
	 * 
	 * @param frameName
	 */
	@Override
	public String[] getColumnTypes(String frameName) {
		String script = "sapply(" + frameName + ", class)";
		script = encapsulateForEnv(script);
		try {
			REXP val = evalRSync(script);
			try {
				 return val.asStrings();
			} catch (REXPMismatchException e) {
				// ignore
			}
			
			try {
				RList list = val.asList();
				int size = list.size();
				String[] arr = new String[size];
				for(int i = 0; i < size; i++) {
					Object v = list.get(i);
					if(v instanceof REXP) {
						try {
							REXP vRexp = (REXP) v;
							arr[i] = vRexp.asString();
						}  catch (REXPMismatchException e) {
							// could be an array
							// actually, seems like RServe 
							// will give you the first element
							// of an array if you use asString
							// even if asStrings() works
							REXP vRexp = (REXP) v;
							arr[i] = vRexp.asStrings()[0];
						}
					} else {
						arr[i] = v.toString();
					}
				}
				
				return arr;
			} catch (REXPMismatchException e) {
				logger.error(Constants.STACKTRACE, e);
			}
		} catch (RserveException e) {
			logger.error(Constants.STACKTRACE, e);
		}
		return null;
	}

	@Override
	public int getInt(String script) {
		int number = 0;
		try {
			script = encapsulateForEnv(script);
			number = evalRSync(script).asInteger();
			return number;
		} catch (RserveException re) {
			logger.error(Constants.STACKTRACE, re);
		} catch (REXPMismatchException e) {
			logger.error(Constants.STACKTRACE, e);
		}
		return number;
	}

	@Override
	public int[] getIntArray(String script) {
		try {
			script = encapsulateForEnv(script);
			return evalRSync(script).asIntegers();
		} catch (RserveException re) {
			logger.error(Constants.STACKTRACE, re);
		} catch (REXPMismatchException e) {
			logger.error(Constants.STACKTRACE, e);
		}
		return null;
	}

	@Override
	public double getDouble(String script) {
		double number = 0;
		try {
			script = encapsulateForEnv(script);
			number = evalRSync(script).asDouble();
			return number;
		} catch (RserveException re) {
			logger.error(Constants.STACKTRACE, re);
		} catch (REXPMismatchException e) {
			logger.error(Constants.STACKTRACE, e);
		}
		return number;
	}

	@Override
	public double[] getDoubleArray(String script) {
		try {
			script = encapsulateForEnv(script);
			return evalRSync(script).asDoubles();
		} catch (RserveException re) {
			logger.error(Constants.STACKTRACE, re);
		} catch (REXPMismatchException e) {
			logger.error(Constants.STACKTRACE, e);
		}
		return null;
	}
	
	@Override
	public double[][] getDoubleMatrix(String script) {
		try {
			script = encapsulateForEnv(script);
			return evalRSync(script).asDoubleMatrix();
		} catch (RserveException re) {
			logger.error(Constants.STACKTRACE, re);
		} catch (REXPMismatchException e) {
			logger.error(Constants.STACKTRACE, e);
		}
		return null;
	}

	@Override
	public Object getFactor(String script) {
		try {
			script = encapsulateForEnv(script);
			return evalRSync(script).asFactor();
		} catch (RserveException re) {
			logger.error(Constants.STACKTRACE, re);
		} catch (REXPMismatchException e) {
			logger.error(Constants.STACKTRACE, e);
		}
		return null;
	}
	
	@Override
	public boolean getBoolean(String script) {
		// 1 = TRUE, 0 = FALSE
		try {
			script = encapsulateForEnv(script);
			REXP val = evalRSync(script);
			if(val != null) {
				return (val.asInteger() == 1);
			}
		} catch (RserveException re) {
			logger.error(Constants.STACKTRACE, re);
		} catch (REXPMismatchException e) {
			logger.error(Constants.STACKTRACE, e);
		}
		return false;
	}
	
	@Override
	public Map<String, Object> getHistogramBreaksAndCounts(String script) {
		try {
			script = encapsulateForEnv(script);
			double[] breaks;
			Map<String, Object> histJ = (Map<String, Object>) (evalRSync(script).asNativeJavaObject());
			if (histJ.get("breaks") instanceof int[]){
				int[] breaksInt = (int[]) histJ.get("breaks");
				breaks = Arrays.stream(breaksInt).asDoubleStream().toArray();
			} else { 
			breaks = (double[]) histJ.get("breaks");
			}
			int[] counts = (int[]) histJ.get("counts");
			
			Map<String, Object> retMap = new HashMap<>();
			retMap.put("breaks", breaks);
			retMap.put("counts", counts);
			return retMap;
		} catch (RserveException re) {
			logger.error(Constants.STACKTRACE, re);
		} catch (REXPMismatchException e) {
			logger.error(Constants.STACKTRACE, e);
		}
		return null;
	}

	@Override
	public Map<String, Object> flushFrameAsTable(String framename, String[] colNames) {
		List<Object[]> dataMatrix = new ArrayList<>();
		
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
					logger.error(Constants.STACKTRACE, rme);
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
					logger.error(Constants.STACKTRACE, rme);
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
					logger.error(Constants.STACKTRACE, rme);
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
					logger.error(Constants.STACKTRACE, rme);
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
					logger.error(Constants.STACKTRACE, rme);
				}
			}
		}
		
		Map<String, Object> retMap = new HashMap<>();
		retMap.put("headers", colNames);
		retMap.put("data", dataMatrix);
		
		return retMap;
	}
	
	@Override
	public Object[] getDataRow(String rScript, String[] headerOrdering) {
		return getBulkDataRow(rScript, headerOrdering).get(0);
	}
	
	@Override
	public List<Object[]> getBulkDataRow(String rScript, String[] headerOrdering) {
		List<Object[]> retArr = new Vector<>(500);

		REXP rs = (REXP) executeR(rScript);
		try {
			RList result = rs.asList();
			
			// match the returns based on index
			int numColumns = headerOrdering.length;
			int[] headerIndex = new int[numColumns];
			for(int i = 0; i < numColumns; i++) {
				headerIndex[i] = result.names.indexOf(headerOrdering[i]);
			}
			
			for(int colNum = 0; colNum < numColumns; colNum++) {
				// grab the right column index
				int columnIndex = headerIndex[colNum];
				// each column comes back as an array
				// need to first initialize my return matrix
				REXP val = (REXP) result.get(columnIndex);
				if(val.isFactor()) {
					RFactor data = val.asFactor();
					if(retArr.isEmpty()) {
						for(int i = 0; i < data.size(); i++) {
							Object[] values = new Object[numColumns];
							values[colNum] = data.at(i);
							retArr.add(values);
						}
					} else {
						for(int i = 0; i < data.size(); i++) {
							Object[] values = retArr.get(i);
							values[colNum] = data.at(i);
						}
					}
				} else if(val.isInteger()) {
					// see if the integer is pointing to a factor
					// NOTE ::: CAN NEVER GET .asFactor() to return true when it is a factor...
					RList attributeList = null;
					boolean isFactor = false;
					if(val._attr() != null) {
						attributeList = val._attr().asList();
						if(attributeList != null) {
							isFactor = attributeList.names.contains("levels");
						}
					}
					int[] data = val.asIntegers();
					boolean[] na = val.isNA();
					if(isFactor && attributeList != null) {
						String[] levels = ((REXP) attributeList.get("levels")).asStrings();
						if(retArr.isEmpty()) {
							for(int i = 0; i < data.length; i++) {
								Object[] values = new Object[numColumns];
								if(na[i]) {
									// keep it as null
									retArr.add(values);
									continue;
								}
								values[colNum] = levels[data[i]-1];
								retArr.add(values);
							}
						} else {
							for(int i = 0; i < data.length; i++) {
								Object[] values = retArr.get(i);
								if(na[i]) {
									// keep it as null
									continue;
								}
								values[colNum] = levels[data[i]-1];
							}
						}
					} else {
						if(retArr.isEmpty()) {
							for(int i = 0; i < data.length; i++) {
								Object[] values = new Object[numColumns];
								if(na[i]) {
									// keep it as null
									retArr.add(values);
									continue;
								}
								values[colNum] = data[i];
								retArr.add(values);
							}
						} else {
							for(int i = 0; i < data.length; i++) {
								Object[] values = retArr.get(i);
								if(na[i]) {
									// keep it as null
									continue;
								}
								values[colNum] = data[i];
							}
						}
					}
				} else if(val.isNumeric()) {
					double[] data = val.asDoubles();
					boolean[] na = val.isNA();
					if(retArr.isEmpty()) {
						for(int i = 0; i < data.length; i++) {
							Object[] values = new Object[numColumns];
							if(na[i]) {
								// keep it as null
								retArr.add(values);
								continue;
							}
							values[colNum] = data[i];
							retArr.add(values);
						}
					} else {
						for(int i = 0; i < data.length; i++) {
							Object[] values = retArr.get(i);
							if(na[i]) {
								// keep it as null
								continue;
							}
							values[colNum] = data[i];
						}
					}
				} else if(val.isString()) {
					String[] data = val.asStrings();
					if(retArr.isEmpty()) {
						for(int i = 0; i < data.length; i++) {
							Object[] values = new Object[numColumns];
							values[colNum] = data[i];
							retArr.add(values);
						}
					} else {
						for(int i = 0; i < data.length; i++) {
							Object[] values = retArr.get(i);
							values[colNum] = data[i];
						}
					}
				}
			}
		} catch(REXPMismatchException e) {
			throw new IllegalArgumentException("R did not evaluate to the proper data type.", e);
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
	public void stopRProcess() {
		try {
			this.retCon.detach();
		} catch (RserveException e) {
			logger.warn("Unable to stop R process.", e);
		}
	}

	@Override
	public Object executeRDirect(String rScript) {
		try {
			return evalRSync(rScript);
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		}
		return null;
	}
	
	@Override
	public synchronized void executeEmptyRDirect(String rScript) {
		try {
			voidEvalRSync(rScript);
		} catch (RserveException e) {
			logger.error(Constants.STACKTRACE, e);
		}
	}
	
	public static void main(String [] args)
	{
		RJavaRserveTranslator rt = new RJavaRserveTranslator();
		rt.env = "monkeyBoy";
		rt.startR();
		Object obj = rt.executeR("2+2");
		System.out.println(obj);
		rt.stopRProcess();
	}

}
