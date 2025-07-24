package prerna.engine.impl.model.inferencetracking.reactors;

import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;


public class RenameRoomReactor extends AbstractReactor {
	
	public RenameRoomReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.ROOM_ID.getKey(), ReactorKeysEnum.NAME.getKey()};
		this.keyRequired = new int [] {1, 1};
	}
	

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String roomId = this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey());
		String roomName = this.keyValue.get(ReactorKeysEnum.NAME.getKey());
		
		boolean result = ModelInferenceLogsUtils.doSetNameForRoom(insight.getUser().getPrimaryLoginToken().getId(), roomId, roomName);
		
		return new NounMetadata(result, PixelDataType.BOOLEAN);
	}

}
