package prerna.reactor.codeexec;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ICodeExecution;
import prerna.ds.py.PyTranslator;
import prerna.ds.py.PyUtils;
import prerna.om.Variable.LANGUAGE;
import prerna.reactor.frame.py.AbstractPyFrameReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.PythonUtils;
import prerna.util.Settings;
import prerna.util.Utility;
import prerna.util.PythonUtils;

public class PyReactor extends AbstractPyFrameReactor implements ICodeExecution {
	
//	private static transient SecurityManager defaultManager = System.getSecurityManager();
	private static final String CLASS_NAME = PyReactor.class.getName();
	// the code that was executed
	private String code = null;

	@Override
	public NounMetadata execute() {
        try {
            PythonUtils.verifyPyCapabilities();
        } catch (IllegalArgumentException e) {
            String errorMsg = "Python capabilities verification failed: " + e.getMessage();
            return new NounMetadata(errorMsg, PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
        }
        
		Logger logger = getLogger(CLASS_NAME);

		this.code = Utility.decodeURIComponent(this.curRow.get(0).toString());
		int tokens = code.split("\\n").length;

		this.code = fillVars(this.code);
		
		PyTranslator pyTranslator = this.insight.getPyTranslator();
		pyTranslator.setLogger(logger);
		//String output = pyTranslator.runPyAndReturnOutput(code);
		String output = null;
		
		if(code.startsWith("sns.")) {
			return new NounMetadata("Please use PyPlot to plot your chart", PixelDataType.CONST_STRING);
		}
		
		NounMetadata execNoun = null;
//		try
//		{
			//if(tokens > 1) 
			{
//				if(nativePyServer)
//				{
//					Map appMap = insight.getUser().getVarMap();
//					
//					if (appMap != null && appMap.containsKey("PY_VAR_STRING"))
//					{
//						String varFolderAssignment = appMap.get("PY_VAR_STRING").toString();
//						varFolderAssignment = varFolderAssignment.replace("\n", ";");
//						pyTranslator.runScript(varFolderAssignment);
//					}
//					output = pyTranslator.runScript(code, this.insight) + "";
//				}
//				else
					output = pyTranslator.runSingle(insight.getUser().getVarMap(), code, this.insight) + "";
					execNoun = new NounMetadata(output, PixelDataType.CONST_STRING);
			} 
			/*else {
				//output = pyTranslator.runScript(code) + "";
				output = pyTranslator.runScript(insight.getUser().getVarMap(), code) + "";
			}*/
//		}catch(SemossPixelException ex)
//		{
//			output = ex.getMessage();
//			execNoun = new NounMetadata(output, PixelDataType.ERROR, PixelOperationType.ERROR);
//		}
		List<NounMetadata> outputs = new ArrayList<>(2);
		outputs.add(execNoun);
		
		boolean smartSync = (insight.getProperty("SMART_SYNC") != null) && insight.getProperty("SMART_SYNC").equalsIgnoreCase("true");
		//forcing smart sync to true
		smartSync = true;

		if(smartSync) {
			// if this returns true
			if(smartSync(pyTranslator)) {
				outputs.add(new NounMetadata(this.insight.getCurFrame(), PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE));
			}
		}
		// call it here.. and if it return true
		// regenerate the metadata. 
		return new NounMetadata(outputs, PixelDataType.CODE, PixelOperationType.CODE_EXECUTION);
	}
	
	
	@Override
	public String getExecutedCode() {
		return this.code;
	}

	@Override
	public LANGUAGE getLanguage() {
		return LANGUAGE.PYTHON;
	}

	@Override
	public boolean isUserScript() {
		return true;
	}
	
}
