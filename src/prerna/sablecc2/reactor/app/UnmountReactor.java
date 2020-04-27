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

public class UnmountReactor extends AbstractReactor {
	
	private static final String CLASS_NAME = UnmountReactor.class.getName();
	
	// takes in a the name and engine and mounts the engine assets as that variable name in both python and R

	public UnmountReactor()
	{
		this.keysToGet = new String[] {ReactorKeysEnum.VARIABLE.getKey()};
		this.keyRequired = new int[]{1};
	}
	
	
	@Override
	public NounMetadata execute() {
		
		organizeKeys();
		
		String varName = keyValue.get(keysToGet[0]);

		if(varName == null)
		{
			throw new IllegalArgumentException("Var or App Name cannot be null ");			
		}

		// remove from R and Py as well
		insight.getPyTranslator().runPyAndReturnOutput(insight.getUser().getAppMap(), "del " + varName);
		insight.getRJavaTranslator(getLogger(CLASS_NAME)).runRAndReturnOutput("rm(" + varName + ")", insight.getUser().getAppMap());
		
		insight.getUser().removeVarString(varName, null);
		return new NounMetadata("Mount point " + varName + " Unmounted " , PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
	}

}
