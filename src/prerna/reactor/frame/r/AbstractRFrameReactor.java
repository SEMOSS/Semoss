package prerna.reactor.frame.r;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import prerna.algorithm.api.ICodeExecution;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.om.Variable.LANGUAGE;
import prerna.poi.main.HeadersException;
import prerna.reactor.frame.AbstractFrameReactor;
import prerna.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.reactor.frame.r.util.IRJavaTranslator;
import prerna.reactor.frame.r.util.RJavaJriTranslator;
import prerna.reactor.imports.ImportUtility;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;

public abstract class AbstractRFrameReactor extends AbstractFrameReactor implements ICodeExecution {

	// the code that was executed
	private List<String> codeExecuted = new ArrayList<>();
	protected AbstractRJavaTranslator rJavaTranslator;

	/**
	 * This method must be called to initialize the rJavaTranslator
	 */
	protected void init() {
		this.rJavaTranslator = this.insight.getRJavaTranslator(this.getLogger(this.getClass().getName()));
		this.rJavaTranslator.startR(); 
	}

	/**
	 * This method is used to recreate the frame metadata
	 * when we execute a script that modifies the data structure
	 * @param frameName
	 */
	protected RDataTable createNewFrameFromVariable(String frameName) {
		// create new frame
		RDataTable newTable = new RDataTable(this.insight.getRJavaTranslator(getLogger(this.getClass().getName())), frameName);
		OwlTemporalEngineMeta meta = genNewMetaFromVariable(frameName);
		newTable.setMetaData(meta);
		return newTable;
	}
	
	/**
	 * Recreate a new engine metadata
	 * @param frameName
	 * @return
	 */
	protected OwlTemporalEngineMeta genNewMetaFromVariable(String frameName) {
		// recreate a new frame and set the frame name
		String[] colNames = getColumns(frameName);
		String[] colTypes = getColumnTypes(frameName);
		//clean headers
		HeadersException headerChecker = HeadersException.getInstance();
		colNames = headerChecker.getCleanHeaders(colNames);
		// update frame header names in R
		String rColNames = "";
		for (int i = 0; i < colNames.length; i++) {
			rColNames += "\"" + colNames[i] + "\"";
			if (i < colNames.length - 1) {
				rColNames += ", ";
			}
		}
		String script = "colnames(" + frameName + ") <- c(" + rColNames + ")";
		this.rJavaTranslator.executeEmptyR(script);
		
		OwlTemporalEngineMeta meta = new OwlTemporalEngineMeta();
		ImportUtility.parseTableColumnsAndTypesToFlatTable(meta, colNames, colTypes, frameName);
		return meta;
	}
	
	/**
	 * Renames columns based on a string[] of old names and a string[] of new
	 * names Used by synchronize methods
	 * 
	 * @param frameName
	 * @param oldNames
	 * @param newNames
	 *            boolean print
	 */
	protected void renameColumn(String frameName, String[] oldNames, String[] newNames, boolean print) {
		int size = oldNames.length;
		if (size != newNames.length) {
			throw new IllegalArgumentException("Names arrays do not match in length");
		}
		StringBuilder oldC = new StringBuilder("c(");
		int i = 0;
		oldC.append("'").append(oldNames[i]).append("'");
		i++;
		for (; i < size; i++) {
			oldC.append(", '").append(oldNames[i]).append("'");
		}
		oldC.append(")");

		StringBuilder newC = new StringBuilder("c(");
		i = 0;
		newC.append("'").append(newNames[i]).append("'");
		i++;
		for (; i < size; i++) {
			newC.append(", '").append(newNames[i]).append("'");
		}
		newC.append(")");

		String script = "setnames(" + frameName + ", old = " + oldC + ", new = " + newC + ")";
		this.rJavaTranslator.executeEmptyR(script);

		if (print) {
			System.out.println("Running script : " + script);
			System.out.println("Successfully modified old names = " + Arrays.toString(oldNames) + " to new names " + Arrays.toString(newNames));
		}

		// FE passes the column name
		// but meta will still be table __ column
		for (i = 0; i < size; i++) {
			this.getFrame().getMetaData().modifyPropertyName(frameName + "__" + oldNames[i], frameName,
					frameName + "__" + newNames[i]);
		}
		this.getFrame().syncHeaders();
	}


	/**
	 * This method is used to fix the frame headers to be valid
	 * 
	 * @param frameName
	 * @param newColName
	 */
	protected String getCleanNewHeader(String frameName, String newColName) {
		// make the new column name valid
		HeadersException headerChecker = HeadersException.getInstance();
		String[] currentColumnNames = getColumns(frameName);
		String validNewHeader = headerChecker.recursivelyFixHeaders(newColName, currentColumnNames);
		return validNewHeader;
	}

	/**
	 * This method is used to get the column names of a frame
	 * @param frameName
	 */
	public String[] getColumns(String frameName) {
		return this.rJavaTranslator.getColumns(frameName);
	}

	/**
	 * This method is used to get the column types of a frame
	 * @param frameName
	 */
	public String[] getColumnTypes(String frameName) {
		return this.rJavaTranslator.getColumnTypes(frameName);
	}

	/**
	 * This method is used to get the column type for a single column of a frame
	 * @param frameName
	 * @param column
	 */
	public String getColumnType(String frameName, String column) {
		return this.rJavaTranslator.getColumnType(frameName, column);
	}
	
	/**
	 * Change the frame column type
	 * @param frame
	 * @param frameName
	 * @param colName
	 * @param newType
	 * @param dateFormat
	 */
	public void changeColumnType(RDataTable frame, String frameName, String colName, String newType, String dateFormat) {
		this.rJavaTranslator.changeColumnType(frame, frameName, colName, newType, dateFormat);
	}
	
	protected void storeVariable(String varName, NounMetadata noun) {
		this.insight.getVarStore().put(varName, noun);
	}

	protected Object retrieveVariable(String varName) {
		NounMetadata noun = this.insight.getVarStore().get(varName);
		if (noun == null) {
			return null;
		}
		return noun.getValue();
	}

	protected void removeVariable(String varName) {
		this.insight.getVarStore().remove(varName);
	}

	protected void endR() {
		// java.lang.System.setSecurityManager(curManager);
		// clean up other things
		this.rJavaTranslator.endR();
		if(rJavaTranslator instanceof RJavaJriTranslator) {
			removeVariable(IRJavaTranslator.R_ENGINE);
			removeVariable(IRJavaTranslator.R_PORT);
		} else {
			removeVariable(IRJavaTranslator.R_CONN);
			removeVariable(IRJavaTranslator.R_PORT);
		}
		System.out.println("R Shutdown!!");
//		java.lang.System.setSecurityManager(reactorManager);
	}
	
	/**
     * Get the base folder
     * @return
     */
     protected String getBaseFolder() {
          String baseFolder = null;
          try {
              baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
          } catch (Exception ignored) {
              //logger.info("No BaseFolder detected... most likely running as test...");
          }
          return baseFolder;
     }
     
 	public boolean smartSync(AbstractRJavaTranslator rJavaTranslator)
 	{
 		// at this point try to see if something has changed and if so
 		// trigger smart sync
 		boolean frameChanged = false;
 		ITableDataFrame frame = this.insight.getCurFrame();
 		if(frame != null && frame instanceof RDataTable)
 		{
 			StringBuffer script = new StringBuffer();
 			String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
 			script.append("source(\"" + baseFolder.replace("\\", "/") + "/R/util/smssutil.R\");\n");
 			script.append("if (!exists(\"allframe\")) allframe <- list();");
 			script.append("allframe <- getCurMeta(" + this.insight.getCurFrame().getName() + ", allframe); \n");
 			script.append("allframe <- hasFrameChanged('" + this.insight.getCurFrame().getName() + "', allframe); \n");
 			
 			// source the script
 			//script.append("source(\"" + baseFolder.replace("\\", "/") + "/R/util/smssutil.R\");").append("hasFrameChanged(" + this.insight.getCurFrame().getName() + ");");
 			//String output = rJavaTranslator.getString(script.toString());
 			
 			String sync = rJavaTranslator.runRAndReturnOutput(script.toString());
 			System.err.println("Output >> " + sync);
 			if(sync.contains("true"))
 			{
 				frameChanged = true;
 				System.err.println("sync > " + sync);
 				OwlTemporalEngineMeta meta =  genNewMetaFromVariable(frame.getName());
 				// replace the meta in the R Data table
 				((RDataTable)frame).setMetaData(meta);
 			}
 		}		
 		return frameChanged;
 	}
 	
 	/////////////////////////////////////////////////////
 	
 	/*
 	 * ICodeExecution methods
 	 */

 	public void addExecutedCode(String code) {
 		if(this.codeExecuted.isEmpty()) {
 			this.codeExecuted.add("###### Code executed from " + getClass().getSimpleName() + " #######");
 		}
 		this.codeExecuted.add(code);
 	}
	
	@Override
	public String getExecutedCode() {
		StringBuffer finalScript = new StringBuffer();
		for(String c : this.codeExecuted) {
			finalScript.append(c).append("\n");
		}
		return finalScript.toString();
	}

	@Override
	public LANGUAGE getLanguage() {
		return LANGUAGE.R;
	}
	
	@Override
	public boolean isUserScript() {
		return false;
	}

}
