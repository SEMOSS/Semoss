package prerna.sablecc2.reactor.frame.py;

import java.util.ArrayList;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.py.PandasFrame;
import prerna.ds.py.PandasSyntaxHelper;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.AbstractFrameReactor;
import prerna.sablecc2.reactor.imports.ImportUtility;

public abstract class AbstractFramePyReactor extends AbstractFrameReactor {

	@Override
	public abstract NounMetadata execute(); 
	
	
	
	protected ITableDataFrame recreateMetadata(PandasFrame frame)
	{
		
		String frameName = frame.getName();
		
		// I am just going to try to recreate the frame here
		PandasFrame newFrame = new PandasFrame(frameName);
		newFrame.setJep(frame.getJep());

		String[] colNames = getColumns(newFrame);
		
		// I bet this is being done for pixel.. I will keep the same
		newFrame.runScript(PandasSyntaxHelper.cleanFrameHeaders(frameName, colNames));
		colNames = getColumns(newFrame);
		
		String[] colTypes = getColumnTypes(newFrame);

		if(colNames == null || colTypes == null) {
			throw new IllegalArgumentException("Please make sure the variable " + frameName + " exists and can be a valid data.table object");
		}
		
		// create the pandas frame
		// and set up teverything else

		ImportUtility.parsePyTableColumnsAndTypesToFlatTable(newFrame, colNames, colTypes, frameName);
		newFrame.setDataTypeMap(newFrame.getMetaData().getHeaderToTypeMap());
		
		String makeWrapper = frameName+"w = PyFrame.makefm(" + frameName +")";
		newFrame.runScript(makeWrapper);

		
		this.insight.setDataMaker(newFrame);
		newFrame.setPrevFrame(frame);

		return newFrame;

	}
	
	public String [] getColumns(PandasFrame frame)
	{
		
		String frameName = frame.getName();
		// get jep thread and get the names
		ArrayList <String> val = (ArrayList<String>)frame.runScript("list(" + frameName + ")");
		String [] retString = new String[val.size()];
		
		val.toArray(retString);
		
		return retString;
		
		
	}
	
	public String [] getColumnTypes(PandasFrame frame)
	{
		String frameName = frame.getName();
		
		// get jep thread and get the names
		ArrayList <String> val = (ArrayList<String>)frame.runScript(PandasSyntaxHelper.getTypes(frameName));
		String [] retString = new String[val.size()];
		
		val.toArray(retString);
		
		return retString;
	}



}
