package prerna.reactor.livekit;

import prerna.auth.utils.SecurityAdminUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.livekit.LiveKitController;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.IOException;
import livekit.LivekitModels.Room;
import java.util.List;

public class LiveKitListRoomsAdminReactor extends AbstractReactor {
	private static final Logger classLogger = LogManager.getLogger(LiveKitListRoomsAdminReactor.class);

	public LiveKitListRoomsAdminReactor() {}

	@Override
	public NounMetadata execute() {
		
		boolean isAdmin = SecurityAdminUtils.userIsAdmin(this.insight.getUser());
		if(!isAdmin) {
			throw new IllegalArgumentException("User must be an admin for this operation!");
		}
		
		LiveKitController controller = LiveKitController.getInstance();
		
		try {
			List<Room> rooms = controller.listRooms();
			
			return new NounMetadata(rooms, PixelDataType.CUSTOM_DATA_STRUCTURE);
			
		} catch (IOException e) {
			String errorMsg = "Failed to list LiveKit rooms: " + e.getMessage();
			classLogger.error(errorMsg, e);
			
			return getError(errorMsg);
		}
	}
	
	@Override
	public String getReactorDescription() {
		return "List active LiveKit Rooms. Admin Only.";
	}
}
