package prerna.sablecc2.reactor.project;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class GetContextReactor extends AbstractReactor {
	
	private static final String CLASS_NAME = GetContextReactor.class.getName();
	
	// takes in a the name and engine and mounts the engine assets as that variable name in both python and R
	// I need to accomodate for when I should over ride
	// for instance a user could have saved a recipe with some mapping and then later, they would like to use a different mapping

	
	@Override
	public NounMetadata execute() {
		
		// need to replace the app with the 
		if(insight.getCmdUtil() != null)
		{
			String context = insight.getCmdUtil().getMountName();
			return new NounMetadata(context , PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
		}
		return new NounMetadata("Context is not set !! ", PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
	}

}
