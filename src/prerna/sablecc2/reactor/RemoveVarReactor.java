package prerna.sablecc2.reactor;

import java.util.List;

import prerna.om.Variable;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class RemoveVarReactor extends AbstractReactor 
{
	public String[] keysToGet = new String[]{ReactorKeysEnum.VARIABLE.getKey()};
	// which of these are optional : 1 means required, 0 means optional
	public int[] keyRequired = new int[] {1}; // if nothing is given calculate everything
	
	@Override
	public NounMetadata execute() {
		// TODO Auto-generated method stub
		organizeKeys();
		String name = (String)this.getNounStore().getNoun(keysToGet[0]).get(0);
		
		// add the variable
		insight.removeVariable(name);
				
		return NounMetadata.getSuccessNounMessage("Variable Removed : " + name);
	}

}
