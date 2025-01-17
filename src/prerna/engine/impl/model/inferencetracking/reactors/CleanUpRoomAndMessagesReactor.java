package prerna.engine.impl.model.inferencetracking.reactors;

import prerna.auth.utils.SecurityAdminUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class CleanUpRoomAndMessagesReactor extends AbstractReactor {

	public CleanUpRoomAndMessagesReactor() {
		this.keysToGet = new String[] { "days" };
		this.keyRequired = new int[] { 1 };
	}

	//takes days as an argument and removes all the rooms,
	// and the messages and feedback associated with the rooms
	// which are older than the days provided
	@Override
	public NounMetadata execute() {
		organizeKeys();
		boolean isAdmin = SecurityAdminUtils.userIsAdmin(this.insight.getUser());
		if(!isAdmin) {
			throw new IllegalArgumentException("User must be an admin for this operation!");
		}
		Integer days = Integer.parseInt(this.keyValue.get(this.keysToGet[0]));
		try {
		if (SecurityAdminUtils.userIsAdmin(this.insight.getUser())) {
			ModelInferenceLogsUtils.cleanUpRoomAndMessages(days);
		}
		}
		catch (Exception e) {
			throw new SemossPixelException(e.getMessage());
		}
		return new NounMetadata("Completed", PixelDataType.BOOLEAN);
	}

	

}
