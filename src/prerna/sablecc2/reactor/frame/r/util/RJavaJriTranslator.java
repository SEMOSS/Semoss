package prerna.sablecc2.reactor.frame.r.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.rosuda.JRI.REXP;
import org.rosuda.JRI.RFactor;
import org.rosuda.JRI.RVector;
import org.rosuda.JRI.Rengine;
import org.rosuda.REngine.Rserve.RConnection;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class RJavaJriTranslator extends AbstractRJavaTranslator {

	Rengine engine;

	/**
	 * Constructor only accesssible through the package
	 * Please use the insight object or the RJavaTranslatorFactory
	 * to get the correct instance
	 */
	RJavaJriTranslator() {
		
	}
	
	/**
	 * This will start R, only if it has not already been started
	 * In this case we are starting an engine for JRI
	 */
	@Override
	public void startR() {
		Rengine retEngine = Rengine.getMainEngine();
		if(retEngine == null && this.insight != null) {
			if(this.insight.getVarStore().containsKey(R_ENGINE)) {
				retEngine = (Rengine) this.insight.getVarStore().get(R_ENGINE).getValue();
			}
		}
		if(retEngine == null) {
			logger.info("R Connection has not been defined yet");
		} else {
			logger.info("Connection right now is set to: " + retEngine);
		}
		
		String OS = java.lang.System.getProperty("os.name").toLowerCase();
		if(retEngine == null) {
			try {
				// start the R Engine
				if(OS.contains("mac")) {
					retEngine = new Rengine(new String[]{"--vanilla"}, true, null);
				} else {
					retEngine = new Rengine(null, true, null);
				}
				logger.info("Successfully created engine.. ");

				// load all the libraries
				Object ret = retEngine.eval("library(splitstackshape);");
				if(ret == null) {
					throw new ClassNotFoundException("Package splitstackshape could not be found!");
				} else {
					logger.info("Successfully loaded packages splitstackshape");
				}
				// data table
				ret = retEngine.eval("library(data.table);");
				if(ret == null) {
					throw new ClassNotFoundException("Package data.table could not be found!");
				} else {
					logger.info("Successfully loaded packages data.table");
				}
				// reshape2
				ret = retEngine.eval("library(reshape2);");
				if(ret == null) {
					throw new ClassNotFoundException("Package reshape2 could not be found!");
				} else {
					logger.info("Successfully loaded packages reshape2");
				}
				// stringr
				ret = retEngine.eval("library(stringr);");
				if(ret == null) {
					throw new ClassNotFoundException("Package stringr could not be found!");
				} else {
					logger.info("Successfully loaded packages stringr");
				}
				
				// set the rengine
				if(this.insight != null) {
					this.insight.getVarStore().put(IRJavaTranslator.R_ENGINE, new NounMetadata(retEngine, PixelDataType.R_ENGINE));
				}
				
				// initialize the r environment
				this.engine = retEngine;
			} catch(NullPointerException e) {
				e.printStackTrace();
				System.out.println("Could not connect to R JRI.  Please make sure paths are accurate");
				throw new IllegalArgumentException("Could not connect to R JRI.  Please make sure paths are accurate");
			} catch(ClassNotFoundException e) {
				System.out.println("ERROR ::: " + e.getMessage() + "\nMake sure you have all the following libraries installed:\n"
						+ "1)splitstackshape\n"
						+ "2)data.table\n"
						+ "3)reshape2\n"
						+ "4)stringr\n\n");
				e.printStackTrace();
				throw new IllegalArgumentException("ERROR ::: " + e.getMessage() + "\nMake sure you have all the following libraries installed:\n"
						+ "1)splitstackshape\n"
						+ "2)data.table\n"
						+ "3)reshape2\n"
						+ "4)stringr\n\n");
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
		this.engine = retEngine;
		initREnv();
	}
	
	@Override
	public Object executeR(String rScript) {
		try {
			REXP rexp = engine.eval(encapsulateForEnv(rScript));
			if(rexp == null) {
				logger.info("Hmmm... REXP returned null for script = " + rScript);
			}
			return rexp;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	@Override
	public void executeEmptyR(String rScript) {
		engine.eval(encapsulateForEnv(rScript), false);
	}

	@Override
	public String getString(String script) {
		REXP val = engine.eval(encapsulateForEnv(script));
		if(val != null) {
			return val.asString();
		}
		return null;
	}

	@Override
	public String[] getStringArray(String script) {
		REXP val = engine.eval(encapsulateForEnv(script));
		if(val != null) {
			return val.asStringArray();
		}
		return null;
	}

	@Override 
	public int getInt(String script) {
		REXP val = engine.eval(encapsulateForEnv(script));
		if(val != null) {
			return val.asInt();
		}
		return 0;
	}

	@Override
	public int[] getIntArray(String script) {
		REXP val = engine.eval(encapsulateForEnv(script));
		if(val != null) {
			return val.asIntArray();
		}
		return null;
	}

	@Override
	public double getDouble(String script) {
		REXP val = engine.eval(encapsulateForEnv(script));
		if(val != null) {
			return val.asDouble();
		}
		return 0;
	}

	@Override
	public double[] getDoubleArray(String script) {
		REXP val = engine.eval(encapsulateForEnv(script));
		if(val != null) {
			return val.asDoubleArray();
		}
		return null;
	}
	
	@Override
	public double[][] getDoubleMatrix(String script) {
		REXP val = engine.eval(encapsulateForEnv(script));
		if(val != null) {
			return val.asDoubleMatrix();
		}
		return null;
	}
	
	@Override
	public boolean getBoolean(String script) {
		REXP val = engine.eval(encapsulateForEnv(script));
		if(val != null) {
			return val.asBool().isTRUE();
		}
		return false;
	}

	@Override
	public Object getFactor(String script) {
		REXP val = engine.eval(encapsulateForEnv(script));
		if(val != null) {
			return val.asFactor();
		}
		return null;
	}
	
	@Override
	public Map<String, Object> getHistogramBreaksAndCounts(String script) {
		REXP histR = (REXP) engine.eval(encapsulateForEnv(script));
		if(histR != null) {
			RVector vectorR = histR.asVector();
			double[] breaks = vectorR.at("breaks").asDoubleArray();
			int[] counts = vectorR.at("counts").asIntArray();

			Map<String, Object> retMap = new HashMap<String, Object>();
			retMap.put("breaks", breaks);
			retMap.put("counts", counts);
			return retMap;
		}
		return null;
	}
	
	@Override
	public Map<String, Object> flushFrameAsTable(String framename, String[] colNames) {
		List<Object[]> dataMatrix = new ArrayList<Object[]>();
		
		int numCols = colNames.length;
		for(int i = 0; i < numCols; i++) {
			String script = framename + "$" + colNames[i];
			REXP val = (REXP) executeR(script);
			int typeInt = val.getType();
			
			if(typeInt == REXP.XT_ARRAY_DOUBLE) {
				double[] rows = val.asDoubleArray();
				int numRows = rows.length;
				if(dataMatrix.isEmpty()) {
					initEmptyMatrix(dataMatrix, numRows, numCols);
				}
				for(int j = 0; j < numRows; j++) {
					dataMatrix.get(j)[i] = rows[j];
				}
			} else if(typeInt == REXP.XT_ARRAY_INT) {
				int[] rows = val.asIntArray();
				int numRows = rows.length;
				if(dataMatrix.isEmpty()) {
					initEmptyMatrix(dataMatrix, numRows, numCols);
				}
				for(int j = 0; j < numRows; j++) {
					dataMatrix.get(j)[i] = rows[j];
				}
			} else if (typeInt == REXP.XT_ARRAY_STR) {
				String[] rows = val.asStringArray();
				int numRows = rows.length;
				if(dataMatrix.isEmpty()) {
					initEmptyMatrix(dataMatrix, numRows, numCols);
				}
				for(int j = 0; j < numRows; j++) {
					dataMatrix.get(j)[i] = rows[j];
				}
			} else if(typeInt == REXP.XT_INT) {
				if(dataMatrix.isEmpty()) {
					initEmptyMatrix(dataMatrix, 1, numCols);
				}
				dataMatrix.get(0)[i] = val.asInt();
			} else if(typeInt == REXP.XT_DOUBLE) {
				if(dataMatrix.isEmpty()) {
					initEmptyMatrix(dataMatrix, 1, numCols);
				}
				dataMatrix.get(0)[i] = val.asDouble();
			} else if(typeInt == REXP.XT_STR) {
				if(dataMatrix.isEmpty()) {
					initEmptyMatrix(dataMatrix, 1, numCols);
				}
				dataMatrix.get(0)[i] = val.asString();
			}
		}
		
		Map<String, Object> retMap = new HashMap<String, Object>();
		retMap.put("headers", colNames);
		retMap.put("data", dataMatrix);
		
		return retMap; 
	}
	
	@Override
	public Object[] getDataRow(String rScript, String[] headerOrdering) {
		// we do not need the header ordering
		// it is only needed for the RServe version of R
		// this will return based on the same order as the rScript
		
		REXP rs = (REXP) executeR(rScript);
		RVector rVec = rs.asVector();
		Vector names = rVec.getNames();
		int numColumns = headerOrdering.length;
		int[] headerIndex = new int[numColumns];
		for(int i = 0; i < numColumns; i++) {
			headerIndex[i] = names.indexOf(headerOrdering[i]);
		}
		
		Object[] retArr = new Object[numColumns];
		for(int colNum = 0; colNum < numColumns; colNum++) {
			int idx = headerIndex[colNum];
			REXP val = rVec.at(idx);
			int typeInt = val.getType();
			if(typeInt == REXP.XT_DOUBLE) {
				retArr[colNum] = val.asDouble();
			} else if(typeInt == REXP.XT_ARRAY_DOUBLE) {
				retArr[colNum] = val.asDoubleArray()[0];
			} else if(typeInt == REXP.XT_INT) {
				retArr[colNum] = val.asInt();
			} else if(typeInt == REXP.XT_ARRAY_INT) {
				retArr[colNum] = val.asIntArray()[0];
			} else if(typeInt == REXP.XT_STR) {
				retArr[colNum] = val.asString();
			} else if(typeInt == REXP.XT_ARRAY_STR) {
				retArr[colNum] = val.asStringArray()[0];
			} else if(typeInt == REXP.XT_BOOL) {
				retArr[colNum] = val.asBool();
			} else if(typeInt == REXP.XT_FACTOR) {
				retArr[colNum] = val.asFactor().at(0);
			} else {
				retArr[colNum] = val;
			}
		}
		
		return retArr;
	}


	@Override
	public List<Object[]> getBulkDataRow(String rScript, String[] headerOrdering) {
		REXP rs = (REXP) executeR(rScript);
		RVector rVec = rs.asVector();
		Vector names = rVec.getNames();

		int numColumns = headerOrdering.length;
		int[] headerIndex = new int[numColumns];
		for(int i = 0; i < numColumns; i++) {
			headerIndex[i] = names.indexOf(headerOrdering[i]);
		}
		List<Object[]> retArr = new Vector<Object[]>(500);
		
		for(int colNum = 0; colNum < numColumns; colNum++) {
			int idx = headerIndex[colNum];
			REXP val = rVec.at(idx);
			int typeInt = val.getType();
			if(typeInt == REXP.XT_ARRAY_DOUBLE) {
				double[] data = val.asDoubleArray();
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
			} else if(typeInt == REXP.XT_ARRAY_INT) {
				int[] data = val.asIntArray();
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
			} else if(typeInt == REXP.XT_ARRAY_STR) {
				String[] data = val.asStringArray();
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
			} else if(typeInt == REXP.XT_FACTOR) {
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
			} else if (typeInt == REXP.XT_STR) {
				String[] data = val.asStringArray();
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
			}
			
			else {
				logger.info("ERROR ::: Could not identify the return type for this iterator!!!");
			}
		}
		
		return retArr;
	}
	

	@Override
	public void setConnection(RConnection connection) {
		logger.info("JRI does not use RConnection object");
	}

	@Override
	public void setPort(String port) {
		logger.info("JRI does not require a port");
	}
	
	@Override
	public void endR() {
		// java.lang.System.setSecurityManager(curManager);
		if (engine != null) {
			engine.end();
		}
		// clean up other things
		System.out.println("R Shutdown!!");
		// java.lang.System.setSecurityManager(reactorManager);
	}

	@Override
	public void initREnv() {
		if(engine != null) {
			engine.eval("if(!exists(\"" + this.env + "\")) {" + this.env  + " <- new.env();}");
		}
	}

}
