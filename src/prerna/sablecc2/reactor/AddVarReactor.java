package prerna.sablecc2.reactor;

import java.util.List;

import prerna.om.Variable;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AddVarReactor extends AbstractReactor 
{
	public String[] keysToGet = new String[]{ReactorKeysEnum.VARIABLE.getKey(), 
											 ReactorKeysEnum.FRAME.getKey(), 
											 ReactorKeysEnum.EXPRESSION.getKey(), 
											 ReactorKeysEnum.LANGUAGE.getKey()};
	// which of these are optional : 1 means required, 0 means optional
	public int[] keyRequired = new int[] {1,1,1,0}; // if nothing is given calculate everything
	
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
		if(language != null) {
			if(language.equalsIgnoreCase("r")) {
				var.setLanguage(Variable.LANGUAGE.R);
			} else if(language.equalsIgnoreCase("python")) {
				var.setLanguage(Variable.LANGUAGE.PYTHON);
			}
		}
		
		// add the variable
		boolean success = insight.addVariable(var);
		
		if(success)
			return NounMetadata.getSuccessNounMessage("Variable Set : " + name);
		else
			return NounMetadata.getErrorNounMessage("One or more of the frames this variable uses is not available");
	}

}
