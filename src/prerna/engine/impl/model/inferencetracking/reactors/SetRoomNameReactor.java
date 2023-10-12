package prerna.engine.impl.model.inferencetracking.reactors;

import prerna.auth.User;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class SetRoomNameReactor extends AbstractReactor {
	
    public SetRoomNameReactor() {
        this.keysToGet = new String[] {"roomId", "roomName"};
        this.keyRequired = new int[] {1, 1};
    }
    
	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
        if (user == null) {
            throw new IllegalArgumentException("You are not properly logged in");
        }
        String roomId = this.keyValue.get(this.keysToGet[0]);
        String roomName = this.keyValue.get(this.keysToGet[1]);
        boolean output = ModelInferenceLogsUtils.doSetNameForRoom(user.getPrimaryLoginToken().getId(), roomId, roomName);
		return new NounMetadata(output, PixelDataType.BOOLEAN);
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("roomId")) {
			return "The room or conversation ID for a given chat app";
		} else if (key.equals("roomName")) {
			return "A sequence of characters to set as the new name";
		} 
		return super.getDescriptionForKey(key);
	}
}
