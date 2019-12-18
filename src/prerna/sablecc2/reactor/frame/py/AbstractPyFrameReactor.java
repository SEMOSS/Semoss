package prerna.sablecc2.reactor.frame.py;

import java.util.List;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.ds.py.PandasFrame;
import prerna.ds.py.PandasSyntaxHelper;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.AbstractFrameReactor;
import prerna.sablecc2.reactor.imports.ImportUtility;

public abstract class AbstractPyFrameReactor extends AbstractFrameReactor {

	protected ITableDataFrame recreateMetadata(PandasFrame frame, boolean override) {
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
		ImportUtility.parseTableColumnsAndTypesToFlatTable(newFrame.getMetaData(), colNames, colTypes, frameName);
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
		List<String> val = (List<String>) frame.runScript("list(" + wrapperName + ".cache['data'])");
		return val.toArray(new String[val.size()]);
	}
	
	public String[] getColumnTypes(PandasFrame frame) {
		String wrapperName = frame.getWrapperName();
		// get jep thread and get the names
		List<String> val = (List<String>) frame.runScript(PandasSyntaxHelper.getTypes(wrapperName + ".cache['data']"));
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
		String pythonDt = (String) frame.runScript(columnType);
		SemossDataType smssDT = this.insight.getPyTranslator().convertDataType(pythonDt);
		return smssDT.toString();
	}
}
