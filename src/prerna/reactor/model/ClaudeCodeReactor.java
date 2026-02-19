package prerna.reactor.model;

import prerna.reactor.AbstractReactor;
import java.util.ArrayList;
import java.util.List;

import prerna.auth.User;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.engine.impl.model.ClaudeCodeManager;

public class ClaudeCodeReactor extends AbstractReactor {
	
	public ClaudeCodeReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.ENGINE.getKey(),
				ReactorKeysEnum.COMMAND.getKey(),
				ReactorKeysEnum.PROJECT.getKey(),
				ReactorKeysEnum.CONTEXT.getKey(),
				ReactorKeysEnum.ROOM_ID.getKey(),
				"allowedTools",
				"permissionMode"
		};
		this.keyRequired = new int[] { 1, 1, 1, 0, 0, 0, 0};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		String roomId = this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey());
		String command = this.keyValue.get(ReactorKeysEnum.COMMAND.getKey());
		String context = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());
		String projectId = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());
		String permissionMode = this.keyValue.get("permissionMode");
	    GenRowStruct grs = this.store.getGenRowStruct("allowedTools");
	    List<String> allowedTools = (grs != null && !grs.isEmpty()) 
	        ? grs.getAllStrValues() 
	        : new ArrayList<>();
		
		User user = this.insight.getUser();
		ClaudeCodeManager manager = new ClaudeCodeManager();

		String response = manager.query(this.insight, user, engineId, projectId, command, context, roomId, allowedTools, permissionMode);
		
		return new NounMetadata(response, PixelDataType.CONST_STRING,
				PixelOperationType.OPERATION);

	}

}
