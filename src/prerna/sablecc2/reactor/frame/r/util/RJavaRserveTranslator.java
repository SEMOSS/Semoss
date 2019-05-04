package prerna.sablecc2.reactor.frame.r.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

import org.rosuda.REngine.REXP;
import org.rosuda.REngine.REXPMismatchException;
import org.rosuda.REngine.RFactor;
import org.rosuda.REngine.RList;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RserveException;

import prerna.engine.impl.r.RSingleton;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.runtime.AbstractBaseRClass;

public class RJavaRserveTranslator extends AbstractRJavaTranslator {

	private static ConcurrentMap<String, ReentrantLock> genEngineLock = new ConcurrentHashMap<String, ReentrantLock>();
	
	RConnection retCon;
	String port;

	/**
	 * Constructor only accessible through the package
	 * Please use the insight object or the RJavaTranslatorFactory
	 * to get the correct instance
	 */
	RJavaRserveTranslator() {

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
				lock.lock();
				try {
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
//						initREnv();
						setMemoryLimit();
					}
				} catch (Exception e) {
					System.out.println(
							"ERROR ::: Could not find connection.\nPlease make sure RServe is running and the following libraries are installed:\n"
									+ "1)splitstackshape\n 2)data.table\n 3)reshape2\n 4)stringr\n 5)lubridate\n 6)dplyr");
					e.printStackTrace();
					throw new IllegalArgumentException(
							"ERROR ::: Could not find connection.\nPlease make sure RServe is running and the following libraries are installed:\n"
									+ "1)splitstackshape\n 2)data.table\n 3)reshape2\n 4)stringr\n 5)lubridate\n 6)dplyr");
				} finally {
					lock.unlock();
				}
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
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
			retCon.voidEval(rScript);
		} finally {
			lock.unlock();
		}
	}
	
	@Override
	public synchronized Object executeR(String rScript) {
		try {
			System.out.println("executeR: " + rScript);
			return evalRSync(rScript);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	@Override
	public synchronized void executeEmptyR(String rScript) {
		try {
			System.out.println("executeR: " + rScript);
			voidEvalRSync(rScript);
		} catch (RserveException e) {
			e.printStackTrace();
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
			return evalRSync(script).asString();
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
			return evalRSync(script).asStrings();
		} catch (RserveException e) {
			e.printStackTrace();
		} catch (REXPMismatchException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * This method is used to get the column types of a frame
	 * 
	 * @param frameName
	 */
	public String[] getColumnTypes(String frameName) {
		String script = "sapply(" + frameName + ", class);";
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
				e.printStackTrace();
			}
		} catch (RserveException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public int getInt(String script) {
		int number = 0;
		try {
			number = evalRSync(script).asInteger();
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
			return evalRSync(script).asIntegers();
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
			number = evalRSync(script).asDouble();
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
			return evalRSync(script).asDoubles();
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
			return evalRSync(script).asDoubleMatrix();
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
			return evalRSync(script).asFactor();
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
			REXP val = evalRSync(script);
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
			double[] breaks;
			Map<String, Object> histJ = (Map<String, Object>) (evalRSync(script).asNativeJavaObject());
			if (histJ.get("breaks") instanceof int[]){
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
	public Object[] getDataRow(String rScript, String[] headerOrdering) {
		return getBulkDataRow(rScript, headerOrdering).get(0);
	}
	
	@Override
	public List<Object[]> getBulkDataRow(String rScript, String[] headerOrdering) {
		List<Object[]> retArr = new Vector<Object[]>(500);

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
					if(retArr.size() == 0) {
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
					if(isFactor) {
						String[] levels = ((REXP) attributeList.get("levels")).asStrings();
						if(retArr.size() == 0) {
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
						if(retArr.size() == 0) {
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
					if(retArr.size() == 0) {
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
					if(retArr.size() == 0) {
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
				} else if(val.isFactor()) {
					RFactor data = val.asFactor();
					if(retArr.size() == 0) {
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
	public void initREnv() {
		try {
			if(this.retCon != null) {
				this.evalRSync("if(!exists(\"" + this.env + "\")) {" + this.env  + "<- new.env();}");
			}
		} catch (RserveException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void stopRProcess() {
		try {
			this.retCon.detach();
		} catch (RserveException e) {
			logger.warn("Unable to stop R process.", e);
		}
	}
}
