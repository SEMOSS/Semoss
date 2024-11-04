package prerna.engine.impl.model.inferencetracking.reactors;

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetRoomMessagesReactor extends AbstractReactor {
	@SuppressWarnings("unused")
	private static final Logger logger = LogManager.getLogger(GetRoomMessagesReactor.class);

    public GetRoomMessagesReactor() {
        this.keysToGet = new String[] {"roomId","limit","offset"};
        this.keyRequired = new int[] {1};
    }
    
	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
        if (user == null) {
            throw new IllegalArgumentException("You are not properly logged in");
        }
        Integer limit = -1, offset = -1;
        String roomId = this.keyValue.get(this.keysToGet[0]);
        if (this.keyValue.get(this.keysToGet[1])!= null && this.keyValue.get(this.keysToGet[2]) != null) {
        	limit = Integer.parseInt(this.keyValue.get(this.keysToGet[1]));
        	offset = Integer.parseInt(this.keyValue.get(this.keysToGet[2]));
        }
        List<Map<String, Object>> output = ModelInferenceLogsUtils.doRetrieveConversation(user.getPrimaryLoginToken().getId(), roomId, "DESC", limit, offset);
		return new NounMetadata(output, PixelDataType.VECTOR);
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("roomId")) {
			return "The room or conversation ID for a given chat app";
		} 
		return super.getDescriptionForKey(key);
	}
}