package prerna.ds.r;

import java.util.Iterator;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.rosuda.JRI.REXP;
import org.rosuda.JRI.RVector;
import org.rosuda.JRI.Rengine;
import org.rosuda.REngine.Rserve.RConnection;

public class RBuilderJRI extends AbstractRBuilder {

	private static final Logger LOGGER = LogManager.getLogger(RBuilderJRI.class.getName());

	// holds the connection for RDataFrame to the instance of R running
	private Rengine retCon;

	public RBuilderJRI() {
		this.retCon = Rengine.getMainEngine();
		if(this.retCon == null) {
			// start the R Engine
			this.retCon = new Rengine(null, true, null);
			LOGGER.info("Successfully created engine.. ");
		}
		// load in the data.table package
		this.retCon.eval("library(data.table)");
		// load in the sqldf package to run sql queries
		this.retCon.eval("library(sqldf)");
		// load all the libraries
		this.retCon.eval("library(splitstackshape);");
		// data table
		this.retCon.eval("library(data.table);");
		// reshape2
		this.retCon.eval("library(reshape2);");
		// rjdbc
		this.retCon.eval("library(RJDBC);");
	}

	public RBuilderJRI(String dataTableName) {
		this();
		if(this.dataTableName != null && !this.dataTableName.trim().isEmpty()) {
			this.dataTableName = dataTableName;
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
		LOGGER.info("JRI implementation does not have a rcon...");
		return null;
	}
	
	@Override
	protected String getPort() {
		LOGGER.info("JRI implementation does not have a port...");
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
		int numReturn = names.size();
		
		Object[] retArr = new Object[numReturn];
		for(int idx = 0; idx < numReturn; idx++) {
			REXP val = rVec.at(idx);
			int typeInt = val.getType();
			if(typeInt == REXP.XT_DOUBLE) {
				retArr[idx] = val.asDouble();
			} else if(typeInt == REXP.XT_ARRAY_DOUBLE) {
				retArr[idx] = val.asDoubleArray()[0];
			} else if(typeInt == REXP.XT_INT) {
				retArr[idx] = val.asInt();
			} else if(typeInt == REXP.XT_ARRAY_INT) {
				retArr[idx] = val.asIntArray()[0];
			} else if(typeInt == REXP.XT_STR) {
				retArr[idx] = val.asString();
			} else if(typeInt == REXP.XT_ARRAY_STR) {
				retArr[idx] = val.asStringArray()[0];
			} else if(typeInt == REXP.XT_BOOL) {
				retArr[idx] = val.asBool();
			} else if(typeInt == REXP.XT_FACTOR) {
				retArr[idx] = val.asFactor().at(0);
			} else {
				retArr[idx] = val;
			}
		}
		
		return retArr;
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
	
}
