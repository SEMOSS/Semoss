package prerna.engine.impl.model.inferencetracking.reactors;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.engine.impl.model.RoomUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetRoomOptionsReactor extends AbstractReactor {
    @SuppressWarnings("unused")
	private static final Logger logger = LogManager.getLogger(GetRoomOptionsReactor.class);

	public GetRoomOptionsReactor() {
		this.keysToGet = new String[] { "roomId" };
		this.keyRequired = new int[] { 1 };
	}

    @Override
    public NounMetadata execute() {

        organizeKeys();
		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("You are not properly logged in");
		}

        String roomId = this.keyValue.get(this.keysToGet[0]);

		Map<String, Object> roomOptions = RoomUtils.getRoomOptions(roomId, user.getPrimaryLoginToken().getId());
		return new NounMetadata(roomOptions, PixelDataType.MAP);
    }
}
