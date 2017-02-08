package prerna.ds.r;

import java.io.File;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RserveException;

import prerna.algorithm.api.IMetaData;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.impl.r.RFileWrapper;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public abstract class AbstractRBuilder {

	protected static final Logger LOGGER = LogManager.getLogger(AbstractRBuilder.class.getName());

	// holds the connection for RDataFrame to the instance of R running
	private String dataTableName = "datatable";

	public AbstractRBuilder() {
	
	}

	public AbstractRBuilder(String dataTableName) throws RserveException {
		this.dataTableName = dataTableName;
	}

	protected String getTableName() {
		return this.dataTableName;
	}
	
	////////////////////////////////////////////////////////////////////
	///////////////////// Abstract Methods /////////////////////////////
	
	/**
	 * Method to run a r script and not need to process output
	 * @param r
	 */
	protected abstract void evalR(String r);
	
	protected abstract Iterator<Object[]> iterator(String[] headerNames, int i, int j);
	
	protected abstract RConnection getConnection();

	protected abstract String getPort();
	
	protected abstract Double executeStat(String columnHeader, String string);
	
	protected abstract Object executeR(String rScript);
	
	protected abstract boolean isEmpty();
	
	protected abstract int getNumRows();
	
	protected abstract int getNumRows(String varName);
	
	protected abstract Map<String, Object> getMapReturn(String rScript);

	protected abstract Object getScalarReturn(String rScript);

	protected abstract String[] getColumnNames();

	protected abstract String[] getColumnNames(String varName);

	protected abstract String[] getColumnTypes();
	
	protected abstract String[] getColumnTypes(String varName);


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
	 * Creates a new data table from an iterator
	 * @param it					The iterator to flush into a r data table
	 * @param typesMap				The data type of each column
	 */
	protected void createTableViaIterator(Iterator<IHeadersDataRow> it, Map<String, IMetaData.DATA_TYPES> typesMap) {
		// we will flush the iterator results into a file
		// and then we will read that file in

		String newFileLoc = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + "/" + Utility.getRandomString(6) + ".csv";
		File newFile = Utility.writeResultToFile(newFileLoc, it, typesMap);

		String loadFileRScript = RSyntaxHelper.getFReadSyntax(this.dataTableName, newFile.getAbsolutePath());
		evalR(loadFileRScript);

		// modify columns such that they are numeric where needed
		alterColumnsToNumeric(typesMap);

		newFile.delete();
	}

	/**
	 * Loads a file as the data table
	 * @param fileWrapper			RFileWrapper used to contain the required information for the load
	 */
	protected void createTableViaCsvFile(RFileWrapper fileWrapper) {
		String loadFileRScript = RSyntaxHelper.getFReadSyntax(this.dataTableName, fileWrapper.getFilePath());
		evalR(loadFileRScript);
		// this will modify the csv to contain the specified columns and rows based on selectors and filters
		String filterScript = fileWrapper.getRScript();
		if(!filterScript.isEmpty()) {
			String modifyTableScript = this.dataTableName + "<- " + filterScript;
			evalR(modifyTableScript);
		}
		// now modify column types to ensure they are all good
		alterColumnsToNumeric(fileWrapper.getDataTypes());
	}

	/**
	 * Modify columns to make sure they are numeric for math operations
	 * @param typesMap
	 */
	private void alterColumnsToNumeric(Map<String, IMetaData.DATA_TYPES> typesMap) {
		for(String header : typesMap.keySet()) {
			IMetaData.DATA_TYPES type = typesMap.get(header);
			if(type == IMetaData.DATA_TYPES.NUMBER) {
				evalR( addTryEvalToScript( RSyntaxHelper.alterColumnTypeToNumeric(this.dataTableName, header) ) );
			}
		}
	}
}
