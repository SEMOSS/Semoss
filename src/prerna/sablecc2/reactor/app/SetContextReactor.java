package prerna.sablecc2.reactor.app;

import java.util.Map;

import prerna.engine.api.IEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class SetContextReactor extends AbstractReactor {
	
	private static final String CLASS_NAME = SetContextReactor.class.getName();
	
	// takes in a the name and engine and mounts the engine assets as that variable name in both python and R
	// I need to accomodate for when I should over ride
	// for instance a user could have saved a recipe with some mapping and then later, they would like to use a different mapping

	public SetContextReactor()
	{
		this.keysToGet = new String[] {ReactorKeysEnum.CONTEXT.getKey()};
		this.keyRequired = new int[]{1};
	}
	
	
	@Override
	public NounMetadata execute() {
		
		organizeKeys();
		
		String context = keyValue.get(keysToGet[0]);
		
		// all of this can be moved into the context reactor
		if(context == null)
			return getError("No context is set - please use Context(<mount point>) to set context");
		
		// need to replace the app with the 
		boolean success = this.insight.setContext(context);
		// attempt once to directly map it with same name
		if(!success)
			return getError("No mount point " + context + " - please use Mount(<mount point>, App Name) to set a mount point");
		
		return new NounMetadata("Successfully set context to '" + context , PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
	}

}
