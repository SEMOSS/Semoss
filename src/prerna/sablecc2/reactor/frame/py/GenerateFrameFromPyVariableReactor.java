package prerna.sablecc2.reactor.frame.py;

import java.util.ArrayList;

import org.apache.log4j.Logger;

import prerna.ds.py.PandasFrame;
import prerna.ds.py.PandasSyntaxHelper;
import prerna.ds.py.PyExecutorThread;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.AbstractFrameReactor;
import prerna.sablecc2.reactor.imports.ImportUtility;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class GenerateFrameFromPyVariableReactor extends AbstractFrameReactor {
	
	private static final String CLASS_NAME = GenerateFrameFromPyVariableReactor.class.getName();
	
	public GenerateFrameFromPyVariableReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.VARIABLE.getKey(), ReactorKeysEnum.OVERRIDE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		//init();
		organizeKeys();
		String varName = getVarName();
		//this.rJavaTranslator.executeEmptyR(RSyntaxHelper.asDataTable(varName, varName));
		// recreate a new frame and set the frame name
		String[] colNames = getColumns(varName);
		
		// I bet this is being done for pixel.. I will keep the same
		runScript(PandasSyntaxHelper.cleanFrameHeaders(varName, colNames));
		colNames = getColumns(varName);
		
		String[] colTypes = getColumnTypes(varName);

		if(colNames == null || colTypes == null) {
			throw new IllegalArgumentException("Please make sure the variable " + varName + " exists and can be a valid data.table object");
		}
		
		// create the pandas frame
		// and set up teverything else
		PandasFrame pf = new PandasFrame(varName);
		pf.setJep(this.insight.getPy());
		
		String makeWrapper = varName+"w = PyFrame.makefm(" + varName +")";
		runScript(makeWrapper);

		ImportUtility.parsePyTableColumnsAndTypesToFlatTable(pf, colNames, colTypes, varName);
		pf.setDataTypeMap(pf.getMetaData().getHeaderToTypeMap());

		NounMetadata noun = new NounMetadata(pf, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE, PixelOperationType.FRAME_HEADERS_CHANGE);		
		if(overrideFrame()) {
			this.insight.setDataMaker(pf);
		}
		// add the alias as a noun by default
		if(varName != null && !varName.isEmpty()) {
			this.insight.getVarStore().put(varName, noun);
		}
		
		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				null, 
				"GenerateFrameFromPyVariable", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		
		return noun;
	}
	
	
	public Object runScript(String... script)
	{
			// so I am nto sure we need to write it to a file etc. for now.. I will run it
			
			PyExecutorThread py = this.insight.getPy();
			py.command = script;
			Object monitor = py.getMonitor();
			Object response = null;
			synchronized(monitor)
			{
				try
				{
					monitor.notify();
					monitor.wait();
				}catch (Exception ignored)
				{
					
				}
				if(script.length == 1)
					response = py.response.get(script[0]);
				else
					response = py.response;
			}
			
	/*		// write the script to a file
			File f = new File(this.scripFolder + "/" + Utility.getRandomString(6) + ".py");
			try {
				FileUtils.writeStringToFile(f, script);
			} catch (IOException e1) {
				System.out.println("Error in writing python script for execution!");
				e1.printStackTrace();
			}
			
			// execute the file
			jep.runScript(f.getAbsolutePath());
			
			// delete the file
			f.delete();
			
	*/	
			return response;
	}
	

	private boolean overrideFrame() {
		GenRowStruct overrideGrs = this.store.getNoun(ReactorKeysEnum.OVERRIDE.getKey());
		if(overrideGrs != null && !overrideGrs.isEmpty()) {
			return (boolean) overrideGrs.get(0);
		}
		// default is to override
		return true;
	}
	
	/**
	 * Get the input being the r variable name
	 * @return
	 */
	private String getVarName() {
		// key based
		GenRowStruct overrideGrs = this.store.getNoun(ReactorKeysEnum.VARIABLE.getKey());
		if(overrideGrs != null && !overrideGrs.isEmpty()) {
			return  (String) overrideGrs.get(0);
		}
		// first input
		return this.curRow.get(0).toString();
	}
	
	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.VARIABLE.getKey())) {
			return "Name of the r variable";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
	
	public String [] getColumns(String tableName)
	{
		
		// get jep thread and get the names
		ArrayList <String> val = (ArrayList<String>)runScript("list(" + tableName + ")");
		String [] retString = new String[val.size()];
		
		val.toArray(retString);
		
		return retString;
		
		
	}
	
	public String [] getColumnTypes(String tableName)
	{
		
		// get jep thread and get the names
		ArrayList <String> val = (ArrayList<String>)runScript(PandasSyntaxHelper.getTypes(tableName));
		String [] retString = new String[val.size()];
		
		val.toArray(retString);
		
		return retString;
	}


}
