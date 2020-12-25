package prerna.sablecc2.reactor.app;

import java.util.Map;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.CmdExecUtil;

public class CommandReactor extends AbstractReactor {
	
	private static final String CLASS_NAME = CommandReactor.class.getName();
	
	// takes in a the name and engine and mounts the engine assets as that variable name in both python and R
	// I need to accomodate for when I should over ride
	// for instance a user could have saved a recipe with some mapping and then later, they would like to use a different mapping

	public CommandReactor()
	{
		this.keysToGet = new String[] {ReactorKeysEnum.COMMAND.getKey()};
		this.keyRequired = new int[]{1};
	}
	
	
	@Override
	public NounMetadata execute() {
		
		organizeKeys();
		
		String command = keyValue.get(keysToGet[0]);

		CmdExecUtil util = this.insight.getUser().getCmdUtil();
		// all of this can be moved into the context reactor
		if(util == null)
			return getError("No context is set - please use Context(<mount point>) to set context");
		String output = util.executeCommand(command);
		// need to replace the app with the 
		return new NounMetadata(output, PixelDataType.CONST_STRING, PixelOperationType.HELP);
	}

}
