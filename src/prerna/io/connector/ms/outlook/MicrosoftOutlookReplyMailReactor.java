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
 * Answers a message in the signed in user's mailbox.
 *
 * <p>
 * Required delegated Microsoft Graph scopes:
 * </p>
 * <ul>
 * <li>{@code Mail.Send} for {@code POST /me/messages/{id}/reply} and
 * {@code /replyAll}</li>
 * <li>{@code Mail.ReadWrite} instead, when {@code asDraft} asks for
 * {@code POST /me/messages/{id}/createReply}, which writes a draft rather than
 * sending anything</li>
 * </ul>
 *
 * <p>
 * This is what keeps an answer in the thread it belongs to. Composing a fresh
 * message with {@code MicrosoftOutlookSendMail} starts a new conversation
 * however carefully the subject is copied, which is what everybody on the
 * thread then has to untangle.
 * </p>
 *
 * <p>
 * Microsoft Outlook writes the recipients and quotes the original underneath,
 * so what is passed here is only what the reply adds above it. Answering
 * everybody is a choice rather than the default, since a reply to all is the
 * one that is hard to take back.
 * </p>
 */
public class MicrosoftOutlookReplyMailReactor extends AbstractMicrosoftOutlookMessageReactor {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftOutlookReplyMailReactor.class);

	private static final String REPLY_ALL = "replyAll";

	public MicrosoftOutlookReplyMailReactor() {
		this.keysToGet = new String[] { UID, COMMENT, REPLY_ALL, AS_DRAFT };
		this.keyRequired = new int[] { 1, 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String uid = requiredUid("answer a message");
		String comment = this.keyValue.get(COMMENT);
		boolean replyAll = Boolean.parseBoolean(this.keyValue.get(REPLY_ALL));
		boolean asDraft = Boolean.parseBoolean(this.keyValue.get(AS_DRAFT));

		if (comment == null || comment.trim().isEmpty()) {
			throw new SemossPixelException("A " + COMMENT + " is required to answer a message.");
		}

		try {
			User user = this.insight.getUser();
			String accessToken = MicrosoftLoginUtils.getMicrosoftAccessToken(user);
			Map<String, Object> draft = new MicrosoftOutlookMailHelper().reply(accessToken, null, uid, comment,
					replyAll, asDraft);

			Map<String, Object> output = new LinkedHashMap<>();
			output.put("repliedTo", uid);
			output.put(REPLY_ALL, replyAll);
			output.put("sent", !asDraft);
			if (draft != null) {
				// the draft's own id, which is what MicrosoftOutlookSendDraft takes
				output.put(UID, draft.get("id"));
				MicrosoftOutlookMessageMapper.putIfPresent(output, "webLink", draft.get("webLink"));
			}
			return new NounMetadata(output, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while answering the message '{}'", uid, e);
			throw e;
		} catch (IllegalArgumentException e) {
			classLogger.error("Invalid input passed to answer a message", e);
			throw new SemossPixelException(e.getMessage());
		} catch (Exception e) {
			classLogger.error("Failed to answer the message '{}'", uid, e);
			throw new SemossPixelException("An error occurred answering the message. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Reply to a message in the signed in user's own Microsoft 365 mailbox, keeping the answer in its thread.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(COMMENT)) {
			return "What the reply says. Microsoft Outlook quotes the message being answered underneath it.";
		} else if (key.equals(REPLY_ALL)) {
			return "Optional boolean to answer everybody on the message rather than only whoever sent it. Defaults to false.";
		}
		return super.getDescriptionForKey(key);
	}
}
