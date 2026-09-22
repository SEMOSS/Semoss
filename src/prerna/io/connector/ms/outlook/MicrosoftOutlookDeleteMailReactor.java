package prerna.io.connector.ms.outlook;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Deletes a message from the signed in user's mailbox.
 *
 * <p>
 * Required delegated Microsoft Graph scope:
 * </p>
 * <ul>
 * <li>{@code Mail.ReadWrite} for {@code DELETE /me/messages/{id}}</li>
 * </ul>
 *
 * <p>
 * The message goes to Deleted Items rather than disappearing, so it can be
 * fished back out of Outlook for as long as that folder keeps it. Moving it to
 * {@code archive} with {@code MicrosoftOutlookMoveMail} is the gentler way to
 * get something out of an inbox.
 * </p>
 */
public class MicrosoftOutlookDeleteMailReactor extends AbstractMicrosoftOutlookMessageReactor {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftOutlookDeleteMailReactor.class);

	public MicrosoftOutlookDeleteMailReactor() {
		this.keysToGet = new String[] { UID };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String uid = requiredUid("delete a message");

		try {
			User user = this.insight.getUser();
			String accessToken = MicrosoftLoginUtils.getMicrosoftAccessToken(user);
			new MicrosoftOutlookMailHelper().deleteMessage(accessToken, null, uid);

			Map<String, Object> output = new LinkedHashMap<>();
			output.put(UID, uid);
			output.put("deleted", true);
			return new NounMetadata(output, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while deleting the message '{}'", uid, e);
			throw e;
		} catch (IllegalArgumentException e) {
			classLogger.error("Invalid input passed to delete a message", e);
			throw new SemossPixelException(e.getMessage());
		} catch (Exception e) {
			classLogger.error("Failed to delete the message '{}'", uid, e);
			throw new SemossPixelException("An error occurred deleting the message. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Delete a message from the signed in user's own Microsoft 365 mailbox, which sends it to Deleted Items.";
	}
}
