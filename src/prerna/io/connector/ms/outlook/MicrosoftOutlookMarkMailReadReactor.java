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
 * Marks a message in the signed in user's mailbox read or unread.
 *
 * <p>
 * Required delegated Microsoft Graph scope:
 * </p>
 * <ul>
 * <li>{@code Mail.ReadWrite} for {@code PATCH /me/messages/{id}}</li>
 * </ul>
 *
 * <p>
 * This is what closes the loop on {@code MicrosoftOutlookListMail} with
 * {@code unreadOnly}: something that reads a mailbox and acts on what it finds
 * needs a way to say it has dealt with a message, or it finds the same one
 * again on the next pass. Marking a message unread again is the same call with
 * {@code read} set to false.
 * </p>
 */
public class MicrosoftOutlookMarkMailReadReactor extends AbstractMicrosoftOutlookMessageReactor {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftOutlookMarkMailReadReactor.class);

	private static final String READ = "read";

	public MicrosoftOutlookMarkMailReadReactor() {
		this.keysToGet = new String[] { UID, READ };
		this.keyRequired = new int[] { 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String uid = requiredUid("mark a message");
		// marking something read is what a caller almost always means, so that is
		// what happens when nothing says otherwise
		String requested = trimToNull(this.keyValue.get(READ));
		boolean read = requested == null || Boolean.parseBoolean(requested);

		try {
			User user = this.insight.getUser();
			String accessToken = MicrosoftLoginUtils.getMicrosoftAccessToken(user);
			new MicrosoftOutlookMailHelper().setRead(accessToken, null, uid, read);

			Map<String, Object> output = new LinkedHashMap<>();
			output.put(UID, uid);
			output.put(READ, read);
			output.put("unread", !read);
			return new NounMetadata(output, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while marking the message '{}'", uid, e);
			throw e;
		} catch (IllegalArgumentException e) {
			classLogger.error("Invalid input passed to mark a message", e);
			throw new SemossPixelException(e.getMessage());
		} catch (Exception e) {
			classLogger.error("Failed to mark the message '{}'", uid, e);
			throw new SemossPixelException("An error occurred marking the message. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Mark a message in the signed in user's own Microsoft 365 mailbox as read, or back to unread.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(READ)) {
			return "Optional boolean for whether the message is marked read. Defaults to true; pass false to mark it unread again.";
		}
		return super.getDescriptionForKey(key);
	}
}
