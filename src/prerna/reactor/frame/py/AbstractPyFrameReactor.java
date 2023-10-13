package prerna.reactor.frame.py;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import prerna.algorithm.api.ICodeExecution;
import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.ds.py.PandasFrame;
import prerna.ds.py.PandasSyntaxHelper;
import prerna.ds.py.PyTranslator;
import prerna.om.Variable.LANGUAGE;
import prerna.reactor.frame.AbstractFrameReactor;
import prerna.reactor.imports.ImportUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;

public abstract class AbstractPyFrameReactor extends AbstractFrameReactor implements ICodeExecution {

	// the code that was executed
	private List<String> codeExecuted = new ArrayList<>();
	
	protected ITableDataFrame recreateMetadata(PandasFrame frame, boolean override) {
		// grab the existing metadata from the frame
		Map<String, String> additionalDataTypes = frame.getMetaData().getHeaderToAdtlTypeMap();
		Map<String, List<String>> sources = frame.getMetaData().getHeaderToSources();
		Map<String, String[]> complexSelectors = frame.getMetaData().getComplexSelectorsMap();

		String frameName = frame.getName();
		PandasFrame newFrame = frame; 
		// I am just going to try to recreate the frame here
		if(override) {
			newFrame = new PandasFrame(frameName);
			newFrame.setJep(frame.getJep());
			newFrame.setTranslator(insight.getPyTranslator());
			String makeWrapper = PandasSyntaxHelper.makeWrapper(newFrame.getWrapperName(), frameName);
			//newFrame.runScript(makeWrapper);
			insight.getPyTranslator().runEmptyPy(makeWrapper);
		}
		String[] colNames = getColumns(newFrame);
		// I bet this is being done for pixel.. I will keep the same
		//newFrame.runScript(PandasSyntaxHelper.cleanFrameHeaders(frameName, colNames));
		insight.getPyTranslator().runEmptyPy(PandasSyntaxHelper.cleanFrameHeaders(frameName, colNames));
		colNames = getColumns(newFrame);
		
		String[] colTypes = getColumnTypes(newFrame);
		if(colNames == null || colTypes == null) {
			throw new IllegalArgumentException("Please make sure the variable " + frameName + " exists and can be a valid data.table object");
		}
		
		// create the pandas frame
		// and set up everything else
		ImportUtility.parseTableColumnsAndTypesToFlatTable(newFrame.getMetaData(), colNames, colTypes, frameName, additionalDataTypes, sources, complexSelectors);
		if (override) {
			this.insight.setDataMaker(newFrame);
		}
		// update varStore
		NounMetadata noun = new NounMetadata(newFrame, PixelDataType.FRAME);
		this.insight.getVarStore().put(frame.getName(), noun);
		
		return newFrame;
	}
	
	protected ITableDataFrame recreateMetadata(PandasFrame frame) {
		return recreateMetadata(frame, true);
	}
	
	public String[] getColumns(PandasFrame frame) {		
		String wrapperName = frame.getWrapperName();
		// get jep thread and get the names
		List<String> val = (List<String>) insight.getPyTranslator().runScript("list(" + wrapperName + ".cache['data'])");
		return val.toArray(new String[val.size()]);
	}
	
	public String[] getColumnTypes(PandasFrame frame) {
		String wrapperName = frame.getWrapperName();
		// get jep thread and get the names
		List<String> val = (List<String>) insight.getPyTranslator().runScript(PandasSyntaxHelper.getTypes(wrapperName + ".cache['data']"));
		return val.toArray(new String[val.size()]);
	}
	
	/**
	 * Get the data type of a column by querying the python frame
	 * @param frame
	 * @param column
	 * @return
	 */
	public String getColumnType(PandasFrame frame, String column) {
		String wrapperName = frame.getWrapperName();
		String columnType = PandasSyntaxHelper.getColumnType(wrapperName + ".cache['data']", column);
		String pythonDt = (String) insight.getPyTranslator().runScript(columnType);
		SemossDataType smssDT = this.insight.getPyTranslator().convertDataType(pythonDt);
		return smssDT.toString();
	}	
	
	public boolean smartSync(PyTranslator pyt)
	{
		// at this point try to see if something has changed and if so
		// trigger smart sync
		boolean frameChanged = false;
		if(this.insight.getCurFrame() != null && this.insight.getCurFrame() instanceof PandasFrame)
		{
			StringBuffer script = new StringBuffer();
			String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
			// source the script
			script.append(this.insight.getCurFrame().getName() + "w.hasFrameChanged()");
			String sync = pyt.runScript(script.toString()) + "";
			frameChanged = sync.equalsIgnoreCase("true");
			//changing this to always on
			//frameChanged = true;
			if(frameChanged)
			{
				System.err.println("sync > " + sync);
				recreateMetadata((PandasFrame)insight.getCurFrame(), true);	
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
		return LANGUAGE.PYTHON;
	}
	
	@Override
	public boolean isUserScript() {
		return false;
	}
	
}
