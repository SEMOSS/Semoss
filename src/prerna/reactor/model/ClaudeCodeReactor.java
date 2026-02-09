package prerna.reactor.model;

import prerna.reactor.AbstractReactor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MessageUtils;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;
import prerna.engine.impl.model.ClaudeCodeManager;

public class ClaudeCodeReactor extends AbstractReactor {
	
	public ClaudeCodeReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.ENGINE.getKey(),
				ReactorKeysEnum.COMMAND.getKey(),
				ReactorKeysEnum.PROJECT.getKey(),
				ReactorKeysEnum.CONTEXT.getKey(),
				ReactorKeysEnum.ROOM_ID.getKey()
		};
		this.keyRequired = new int[] { 1, 1, 1, 0, 0};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		String roomId = this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey());
		String command = this.keyValue.get(ReactorKeysEnum.COMMAND.getKey());
		String context = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());
		String projectId = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());
		
		User user = this.insight.getUser();
		
		String response = ClaudeCodeManager.query(this.insight, user, engineId, projectId, prompt, systemPrompt, roomId)

	}

}
