package prerna.sablecc2.reactor.app;

import java.util.List;
import java.util.Vector;

import org.apache.log4j.Logger;

import prerna.ds.py.PyTranslator;
import prerna.ds.py.PyUtils;
import prerna.ds.py.TCPPyTranslator;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Utility;

public class MountReactor extends AbstractReactor {
	
	private static final String CLASS_NAME = MountReactor.class.getName();
	
	// takes in a the name and engine and mounts the engine assets as that variable name in both python and R
	// I need to accomodate for when I should over ride
	// for instance a user could have saved a recipe with some mapping and then later, they would like to use a different mapping

	public MountReactor()
	{
		this.keysToGet = new String[] {ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.VARIABLE.getKey()};
		this.keyRequired = new int[]{1,1};
	}
	
	
	@Override
	public NounMetadata execute() {
		
		organizeKeys();
		
		String varName = keyValue.get(keysToGet[1]);
		String appName = keyValue.get(keysToGet[0]);

		if(varName == null || appName == null)
		{
			throw new IllegalArgumentException("Var / Appa cannor be null ");			
		}
		
		

		
		boolean success = insight.getUser().addVarMap(varName, appName);
		//String varString = insight.getUser().getVarString(false);
		
		
		if(!success)
		{
			throw new IllegalArgumentException("No app with " + appName + " available ");			
		}
		return new NounMetadata("Successfully mounted '" + varName + "' to app '" + appName + "'", PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
	}

}
