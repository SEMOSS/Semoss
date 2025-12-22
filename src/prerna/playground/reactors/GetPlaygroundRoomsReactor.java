package prerna.playground.reactors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;

import prerna.auth.User;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.inferencetracking.reactors.GetUserConversationRoomsReactor;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.MessageType;
import prerna.engine.impl.model.message.MessageUtils;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.playground.PlaygroundUtils;
import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetPlaygroundRoomsReactor extends GetUserConversationRoomsReactor {
	
	@Override
	public NounMetadata execute() {
		GenRowStruct projectGRS = this.store.getGenRowStruct(ReactorKeysEnum.PROJECT.getKey());
		if(projectGRS != null) {
			projectGRS.clear();
		} else {
			projectGRS = new GenRowStruct();
		}
		projectGRS.add(new NounMetadata(PlaygroundUtils.PLAYGROUND_PROJECT_ID, PixelDataType.CONST_STRING));
		this.store.addNoun(ReactorKeysEnum.PROJECT.getKey(), projectGRS);
		return super.execute();
	}

}
