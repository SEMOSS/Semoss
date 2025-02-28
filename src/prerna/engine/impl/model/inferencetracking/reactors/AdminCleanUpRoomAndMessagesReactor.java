package prerna.engine.impl.model.inferencetracking.reactors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityAdminUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class AdminCleanUpRoomAndMessagesReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AdminCleanUpRoomAndMessagesReactor.class);

	public AdminCleanUpRoomAndMessagesReactor() {
		this.keysToGet = new String[] { "days" };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		/*
		 * takes days as an argument and removes all the rooms,
		 * and the messages and feedback associated with the rooms
		 * which are older than the days provided
		 */
		organizeKeys();
		boolean isAdmin = SecurityAdminUtils.userIsAdmin(this.insight.getUser());
		if(!isAdmin) {
			throw new IllegalArgumentException("User must be an admin to perform this function");
		}
		String daysStr = this.keyValue.get(this.keysToGet[0]);
		int days = -1;
		try {
			days = ((Number) Double.parseDouble(daysStr)).intValue();
		} catch(NumberFormatException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("The number of days must be a valid integer value");
		}
		try {
			ModelInferenceLogsUtils.cleanUpRoomAndMessages(days);
		} catch (Exception e) {
			throw new SemossPixelException(e.getMessage());
		}
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

	@Override
	public String getReactorDescription() {
		return "This reactor is used to delete old chat messages from the model logs inference database";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals("days")) {
			return "The number of days since the creation time of the message to delete";
		}
		return super.getDescriptionForKey(key);
	}

}
