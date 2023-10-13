package prerna.engine.impl.model.inferencetracking.reactors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class RemoveUserRoomReactor extends AbstractReactor {
	@SuppressWarnings("unused")
	private static final Logger logger = LogManager.getLogger(RemoveUserRoomReactor.class);

    public RemoveUserRoomReactor() {
        this.keysToGet = new String[] {"roomId"};
        this.keyRequired = new int[] {1};
    }
    
	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
        if (user == null) {
            throw new IllegalArgumentException("You are not properly logged in");
        }
        String roomId = this.keyValue.get(this.keysToGet[0]);
        boolean result = ModelInferenceLogsUtils.doSetRoomToInactive(user.getPrimaryLoginToken().getId(), roomId);
		return new NounMetadata(result, PixelDataType.BOOLEAN);
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("roomId")) {
			return "The room or conversation ID for a given chat app";
		} 
		return super.getDescriptionForKey(key);
	}
}