package prerna.sablecc2.reactor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.algorithm.api.DataFrameTypeEnum;
import prerna.algorithm.api.ITableDataFrame;
import prerna.om.Variable;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class CalcVarReactor extends AbstractReactor {

	public String[] keysToGet = new String[]{ReactorKeysEnum.VARIABLE.getKey()};
	// which of these are optional : 1 means required, 0 means optional
	public int[] keyRequired = new int[] {0}; // if nothing is given calculate everything

	// sample - 			String [] formulas = new String[]{"x=1", "age_sum = frame_d['age'].astype(int).sum()", "msg = 'Total Age now is {}'.format(age_sum)"};	
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		// in case variable or variableName is passed
		List dynamicVarNames = null;
		if(this.getNounStore().getNoun(keysToGet[0]) != null) {
			dynamicVarNames = this.getNounStore().getNoun(this.keysToGet[0]).getAllValues();
		} else if(!this.curRow.isEmpty()) {
			dynamicVarNames = this.curRow.getAllValues();
		} else {
			dynamicVarNames = insight.getAllVars();
		}
		
		StringBuffer pyDeleter = new StringBuffer("del(");
		StringBuffer rDeleter = new StringBuffer("rm(");
		
		Map <String, String> oldNew = new HashMap<String, String>();

		Map<String, Object> varValue = new HashMap<String, Object>();
		
		// get all the frames processed first
		for(int varIndex =0; varIndex < dynamicVarNames.size();varIndex++) {
			Object val = dynamicVarNames.get(varIndex);
			String name = null;
			Variable var = null;
			if(val instanceof String) {
				name = (String) val;
				var = insight.getVariable(name);
			} else if(val instanceof Variable) {
				var = (Variable) val;
				name = ((Variable) val).getName();
			} else {
				throw new IllegalArgumentException("Input " + val + " is not a valid variable");
			}
			// get the variable
			// get the frames
			// get the language
			// compare to see if the frame and language are the same 
			// TODO else we need to move it like how the pivot is
			// get the frame from insight for frame name
			// do a query all and create a query struct
			// make the call to the frame to create a secondary variable
			// string replace old frame with new
			// run the calculation
			// give the response
			//List <String> frames = var.getFrames();
			
			// get the variable
			// only for testing
			/*
			Variable var = new Variable();
			var.setExpression("'Sum of ages is now set to.. {}'.format(frame_d['age'].astype(int).sum())");
			var.setName("age_sum");
			var.addFrame("frame_d");
			var.setLanguage(Variable.LANGUAGE.PYTHON);
			*/
			
			//this.insight.getVariable(name);
						
			// get the frames
			List <String> frameNames = var.getFrames();
			// get the language
			// compare to see if the frame and language are the same 
			// TODO else we need to move it like how the pivot is
			// get the frame from insight for frame name
			
			for(int frameIndex = 0;frameIndex < frameNames.size();frameIndex++)
			{
				String thisFrameName = frameNames.get(frameIndex);
				ITableDataFrame frame = insight.getFrame(thisFrameName);
				
				if(!oldNew.containsKey(thisFrameName)) // if not already processed. Generate one
				{
					// query the frame
					// make the call to the frame to create a secondary variable
					try {
						// forcing it to be pandas frame for now
						String newName = frame.createVarFrame();
						oldNew.put(thisFrameName, newName);	
						if(frame.getFrameType() == DataFrameTypeEnum.PYTHON) {
							pyDeleter.append(newName).append(", ");
						} else if(frame.getFrameType() == DataFrameTypeEnum.R) {
							rDeleter.append(newName).append(", ");
						}
					} catch (Exception e)  {
						e.printStackTrace();
					}
				}
			}
		}
		
		// replace the frames and execute
		for(int varIndex = 0; varIndex < dynamicVarNames.size(); varIndex++) {
			Object val = dynamicVarNames.get(varIndex);
			String name = null;
			Variable var = null;
			if(val instanceof String) {
				name = (String) val;
				var = insight.getVariable(name);
			} else if(val instanceof Variable) {
				var = (Variable) val;
				name = ((Variable) val).getName();
			}
			
			List <String> frameNames = var.getFrames();
			String expression = var.getExpression();
			// now that the frames are created
			// replace and run
			// string replace old frame with new
			for(int frameIndex = 0;frameIndex < frameNames.size();frameIndex++) {
				String thisFrameName = frameNames.get(frameIndex);
				String newName = oldNew.get(thisFrameName);				
				expression = expression.replace(thisFrameName, newName);
			}
			
			// calc the var			
			// run the calculation
			// give the response
			if(var.getLanguage() == Variable.LANGUAGE.PYTHON) {
				Object value = insight.getPyTranslator().runScript(expression);
				varValue.put(var.getName(), value);
			}
			if(var.getLanguage() == Variable.LANGUAGE.R) {
				Object value = insight.getRJavaTranslator(this.getClass().getName()).runRAndReturnOutput(expression);
				varValue.put(var.getName(), value);
			}
		}	
		
		// need something to delete all the interim frame variables
		if(pyDeleter.indexOf(",") > 0) // atleast one is filled up
			insight.getPyTranslator().runEmptyPy(pyDeleter.substring(0, pyDeleter.length() - 2) + ")");
		
		if(rDeleter.indexOf(",") > 0) // atleast one is filled up
			insight.getRJavaTranslator(this.getClass().getName()).executeEmptyR(rDeleter.substring(0, pyDeleter.length() - 2) + ")");
		
		return new NounMetadata(varValue, PixelDataType.MAP);
	}

}
