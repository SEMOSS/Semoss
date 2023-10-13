package prerna.reactor;

import java.util.List;

import prerna.om.Variable;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AddVarReactor extends AbstractReactor {
	
	public AddVarReactor( ) {
		this.keysToGet = new String[]{ReactorKeysEnum.VARIABLE.getKey(), 
				 ReactorKeysEnum.FRAME.getKey(), 
				 ReactorKeysEnum.EXPRESSION.getKey(), 
				 ReactorKeysEnum.LANGUAGE.getKey(),
				 ReactorKeysEnum.FORMAT.getKey()};
		// which of these are optional : 1 means required, 0 means optional
		this.keyRequired = new int[] {1,1,1,0,0}; // if nothing is given calculate everything
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String name = (String)this.getNounStore().getNoun(this.keysToGet[0]).get(0);
		// this should be a list of strings
		List frames = (List)this.getNounStore().getNoun(this.keysToGet[1]).getAllValues();
		String expression = (String)this.getNounStore().getNoun(this.keysToGet[2]).get(0);
		String language = (String)this.getNounStore().getNoun(this.keysToGet[3]).get(0);
		
		Variable var = new Variable();
		var.setName(name);
		var.setExpression(expression);
		var.setFrames(frames);
		
		if(this.getNounStore().getNoun(this.keysToGet[4]) != null) {
			String format = (String)this.getNounStore().getNoun(this.keysToGet[4]).get(0);
			if(format != null) {
				var.setFormat(format);		
			}	
		}
		
		if(language != null) {
			if(language.equalsIgnoreCase("r")) {
				
				var.setLanguage(Variable.LANGUAGE.R);
				
				// try to execute in R and see if the expression works
				try
				{
					String newExpression = "tryCatch(" + expression + ", error=function(e) { 'error'})";
					String obj = this.insight.getRJavaTranslator(this.getClass().getCanonicalName()).runRAndReturnOutput(newExpression) + "";
					if(obj != null && (obj.toString().contains("error") || obj.contains("java.lang.IllegalArgumentException")))
					{
						System.err.println("Came in with exception");
						return NounMetadata.getErrorNounMessage("Expression has error, please correct " + expression);
					}
				}catch(Exception ex)
				{
					System.err.println("Exception occurred" + ex);
				}
			} else if(language.equalsIgnoreCase("python")) {
				var.setLanguage(Variable.LANGUAGE.PYTHON);
			}
		}
		
		// add the variable
		boolean success = insight.addVariable(var);
		NounMetadata retNoun = null;
		if(success) {
			retNoun = new NounMetadata(name, PixelDataType.CONST_STRING, PixelOperationType.ADD_VARIABLE);
			retNoun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Variable Set : " + name));
		} else {
			retNoun = NounMetadata.getErrorNounMessage("One or more of the frames this variable uses is not available");
		}
		return retNoun;
	}

}
