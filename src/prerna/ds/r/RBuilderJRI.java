package prerna.ds.r;

import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.rosuda.JRI.REXP;
import org.rosuda.JRI.RFactor;
import org.rosuda.JRI.RVector;
import org.rosuda.JRI.Rengine;
import org.rosuda.REngine.Rserve.RConnection;

public class RBuilderJRI extends AbstractRBuilder {

	// holds the connection for RDataFrame to the instance of R running
	private Rengine retCon;

	public RBuilderJRI() {
		this.retCon = Rengine.getMainEngine();
		
		String OS = java.lang.System.getProperty("os.name").toLowerCase();
		if(this.retCon == null) {
			// start the R Engine
			if(OS.contains("mac")) {
				this.retCon = new Rengine(new String[]{"--vanilla"}, true, null);
			} else {
				this.retCon = new Rengine(null, true, null);
			}
			this.logger.info("Successfully created engine.. ");
			// only need to load the libraries once
			loadDefaultLibraries();
		}
	}

	public RBuilderJRI(String dataTableName) {
		this();
		if(dataTableName != null && !dataTableName.trim().isEmpty()) {
			this.dataTableName = dataTableName;
		}
	}

	private void loadDefaultLibraries() {
		String OS = System.getProperty("os.name").toLowerCase();
		// load splitstackshape
		this.logger.info("TRYING TO LOAD PACAKGE: splitstackshape");
		REXP retObj = this.retCon.eval("library(splitstackshape);");
		if(retObj == null) {
			this.logger.info(">>> FAILURE!");
		} else {
			this.logger.info("SUCCESS!");
		}		
		// data table
		this.logger.info("TRYING TO LOAD PACAKGE: data.table");
		retObj = this.retCon.eval("library(data.table);");
		if(retObj == null) {
			this.logger.info(">>> FAILURE!");
		} else {
			this.logger.info("SUCCESS!");
		}
		
		if(!OS.contains("mac")) {
			// xlsx
			this.logger.info("TRYING TO LOAD PACAKGE: xlsx");
			retObj = this.retCon.eval("library(xlsx);");
			if(retObj == null) {
				this.logger.info(">>> FAILURE!");
			} else {
				this.logger.info("SUCCESS!");
			}	
					
			// rjdbc
			this.logger.info("TRYING TO LOAD PACAKGE: RJDBC");
			retObj = this.retCon.eval("library(RJDBC);");
			if(retObj == null) {
				this.logger.info(">>> FAILURE!");
			} else {
				this.logger.info("SUCCESS!");
			}
		}
		
		// reshape2
		this.logger.info("TRYING TO LOAD PACAKGE: reshape2");
		retObj = this.retCon.eval("library(reshape2);");
		if(retObj == null) {
			this.logger.info(">>> FAILURE!");
		} else {
			this.logger.info("SUCCESS!");
		}
		
		// stringr
		this.logger.info("TRYING TO LOAD PACAKGE: stringr");
		retObj = this.retCon.eval("library(stringr);");
		if(retObj == null) {
			this.logger.info(">>> FAILURE!");
		} else {
			this.logger.info("SUCCESS!");
		}	
	}

	protected String getTableName() {
		return this.dataTableName;
	}

	@Override
	protected void evalR(String r) {
		executeR(r);
	}

	@Override
	protected RConnection getConnection() {
		this.logger.debug("JRI implementation does not have a rcon...");
		return null;
	}
	
	@Override
	protected String getPort() {
		this.logger.debug("JRI implementation does not have a port...");
		return null;
	}
	
	protected Double executeStat(String colName, String statRoutine) {
		REXP result = retCon.eval( addTryEvalToScript( statRoutine + "(" + this.dataTableName + "[,c(\"" + colName + "\")])") );
		Double val = result.asDouble();
		return val;
	}

	protected String getROutput(String rScript) {
		String newScript = "try(eval( paste(capture.output(print(" + rScript + ")),collapse='\n') ), silent=FALSE)";
		REXP result = retCon.eval(newScript);
		return result.asString();
	}

	protected REXP executeR(String r) {
		return retCon.eval(r);
	}

	/**
	 * Wrap the R script in a try-eval in order to get the same error message that a user would see if using
	 * the R console
	 * @param rscript			The R script to execute
	 * @return					The R script wrapped in a try-eval statement
	 */
	protected String addTryEvalToScript(String rscript) {
		return "try(eval(" + rscript + "), silent=FALSE)";
	}

	/**
	 * Calculate the number of rows in the data table
	 * @return
	 */
	protected int getNumRows() {
		return getNumRows(this.dataTableName);
	}
	
	/**
	 * Calculate the number of rows in the data table
	 * @return
	 */
	protected int getNumRows(String varName) {
		REXP result = executeR( addTryEvalToScript( "nrow(" + varName + ")"  ) );
		return result.asInt();
	}

	protected Iterator<Object[]> iterator(String[] headerNames, int limit, int offset) {
		return new RIterator(this, headerNames, limit, offset);
	}

	/**
	 * Determine if the datatable is empty
	 * @return
	 */
	public boolean isEmpty() {
		REXP result = executeR( addTryEvalToScript( "exists(" + this.dataTableName + ")" ) );
		// we get the boolean expression as an integer
		// 1 = TRUE, 0 = FALSE
		int intBooleanVal = result.asInt();
		if(intBooleanVal == 1) {
			// so it exists, but we should probably make sure it has at least one row
			if(getNumRows() > 0) {
				return false;
			}
		}

		return true;
	}

	@Override
	protected Object[] getDataRow(String rScript, String[] headerOrdering) {
		// we do not need the header ordering
		// it is only needed for the RServe version of R
		// this will return based on the same order as the rScript
		
		REXP rs = executeR(rScript);
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
	protected List<Object[]> getBulkDataRow(String rScript, String[] headerOrdering) {
		REXP rs = executeR(rScript);
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

	protected Object[] getBulkSingleColumn(String rScript) {
		REXP rs = executeR(rScript);
		int typeInt = rs.getType();
		if(typeInt == REXP.XT_ARRAY_DOUBLE) {
			double[] data = rs.asDoubleArray();
			Object[] retObj = new Object[data.length];
			for(int i = 0; i < data.length; i++) {
				retObj[i] = data[i];
			}
			return retObj;
		} else if(typeInt == REXP.XT_ARRAY_INT) {
			int[] data = rs.asIntArray();
			Object[] retObj = new Object[data.length];
			for(int i = 0; i < data.length; i++) {
				retObj[i] = data[i];
			}
			return retObj;
		} else if(typeInt == REXP.XT_ARRAY_STR) {
			return rs.asStringArray();
		} else if(typeInt == REXP.XT_FACTOR) {
			RFactor data = rs.asFactor();
			int size = data.size();
			Object[] retObj = new Object[size];
			for(int i = 0; i < size; i++) {
				retObj[i] = data.at(i);
			}
			return retObj;
		} else {
			logger.info("ERROR ::: Could not identify the return type for this iterator!!!");
		}

		return null;
	}

	@Override
	public Object getScalarReturn(String rScript) {
		REXP rs = executeR(rScript);
		int typeInt = rs.getType();
		if(typeInt == REXP.XT_DOUBLE) {
			return rs.asDouble();
		} else if(typeInt == REXP.XT_ARRAY_DOUBLE) {
			return rs.asDoubleArray()[0];
		} else if(typeInt == REXP.XT_INT) {
			return rs.asInt();
		} else if(typeInt == REXP.XT_ARRAY_INT) {
			return rs.asIntArray()[0];
		} else if(typeInt == REXP.XT_STR) {
			return rs.asString();
		} else if(typeInt == REXP.XT_ARRAY_STR) {
			return rs.asStringArray()[0];
		} else if(typeInt == REXP.XT_BOOL) {
			return rs.asBool();
		} else if(typeInt == REXP.XT_VECTOR){
			RVector rVec = rs.asVector();
			REXP val = rVec.at(0);
			typeInt = val.getType();
			if(typeInt == REXP.XT_DOUBLE) {
				return val.asDouble();
			} else if(typeInt == REXP.XT_ARRAY_DOUBLE) {
				return val.asDoubleArray()[0];
			} else if(typeInt == REXP.XT_INT) {
				return val.asInt();
			} else if(typeInt == REXP.XT_ARRAY_INT) {
				return val.asIntArray()[0];
			} else if(typeInt == REXP.XT_STR) {
				return val.asString();
			} else if(typeInt == REXP.XT_ARRAY_STR) {
				return val.asStringArray()[0];
			} else if(typeInt == REXP.XT_BOOL) {
				return val.asBool();
			} else {
				return val;
			}
		} else {
			return rs;
		}
	}

	@Override
	public String[] getColumnNames() {
		return getColumnNames(this.dataTableName);
	}

	@Override
	public String[] getColumnTypes() {
		return getColumnTypes(this.dataTableName);
	}
	
	@Override
	public String[] getColumnNames(String varName) {
		REXP namesR = executeR("names(" + varName + ")");
		return namesR.asStringArray();
	}

	@Override
	public String[] getColumnTypes(String varName) {
		REXP typesR = executeR("sapply(" + varName + " , class)");
		return typesR.asStringArray();
	}
	
/*	@Override
	public String[] getColumnType(String varName) {
		
		REXP typesR = executeR("sapply(" + this.dataTableName + "$" + varName + "[1]" + " , class)");
		System.out.println(executeR("sapply(" + this.dataTableName + "$" + varName + "[1]" + " , class)"));
		return typesR.asStringArray();
	}
*/	
	@Override
	public int getIntFromScript(String rScript){
		REXP result = executeR(rScript);
		int number = result.asInt();
		return number;
	}
}
