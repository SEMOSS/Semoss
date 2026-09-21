package prerna.io.connector.ms.outlook;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.execptions.SemossPixelException;

/**
 * What the reactors that act on a message already in the mailbox have in
 * common.
 *
 * <p>
 * The composing reactors assemble something new, and these take something that
 * is already there and answer it, move it, mark it or delete it. What they
 * share is the message they are pointed at: {@code uid} is the id a listing
 * handed back, named the way the listing names it rather than the way Graph
 * does, so what comes out of one reactor goes straight into the next.
 * </p>
 *
 * <p>
 * Every one of these works against the signed in user's own mailbox. Nothing
 * takes a mailbox, because the token is what says whose mail this is.
 * </p>
 */
public abstract class AbstractMicrosoftOutlookMessageReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AbstractMicrosoftOutlookMessageReactor.class);

	protected static final String UID = "uid";
	protected static final String COMMENT = "comment";
	protected static final String AS_DRAFT = "asDraft";
	protected static final String MAX_BODY_CHARS = "maxBodyChars";

	/** How much of a body comes back before it is cut short. */
	protected static final int DEFAULT_MAX_BODY_CHARS = 10_000;

	/**
	 * Read the message this reactor was pointed at.
	 *
	 * @param toDo what is being done to it, used in the error
	 * @return the message id
	 */
	protected String requiredUid(String toDo) {
		String uid = trimToNull(this.keyValue.get(UID));
		if (uid == null) {
			throw new SemossPixelException(
					"A " + UID + ", as returned by MicrosoftOutlookListMail, is required to " + toDo + ".");
		}
		return uid;
	}

	/**
	 * Read a key that has to be a positive whole number.
	 *
	 * @param key      the key to read
	 * @param fallback what it is when the caller left it out
	 * @param cap      the largest value allowed, which the value is held down to
	 * @return the value
	 */
	protected int positiveInt(String key, int fallback, int cap) {
		String value = trimToNull(this.keyValue.get(key));
		if (value == null) {
			return fallback;
		}
		int parsed;
		try {
			parsed = Integer.parseInt(value);
		} catch (NumberFormatException e) {
			classLogger.error("Invalid {} of '{}' passed to a Microsoft Outlook reactor", key, value, e);
			throw new SemossPixelException(key + " must be a whole number.");
		}
		if (parsed <= 0) {
			throw new SemossPixelException(key + " must be greater than 0.");
		}
		if (parsed > cap) {
			classLogger.warn("A {} of {} was asked for, using {} instead", key, parsed, cap);
			return cap;
		}
		return parsed;
	}

	/**
	 * Read one set of values, which a caller may pass as several nouns, as one
	 * comma separated noun, or as any mixture of the two.
	 *
	 * @param key the key to read
	 * @return the values, or null when none were passed
	 */
	protected String[] values(String key) {
		GenRowStruct grs = this.store.getGenRowStruct(key);
		if (grs == null || grs.isEmpty()) {
			return null;
		}
		List<String> values = new ArrayList<>();
		for (int i = 0; i < grs.size(); i++) {
			Object value = grs.getNoun(i).getValue();
			if (value == null) {
				continue;
			}
			// a single value can still be a comma separated list, since that is how
			// somebody writing the pixel by hand tends to pass more than one
			for (String entry : value.toString().split(",")) {
				if (!entry.trim().isEmpty()) {
					values.add(entry.trim());
				}
			}
		}
		if (values.isEmpty()) {
			return null;
		}
		return values.toArray(new String[0]);
	}

	/**
	 * @param value the value to trim
	 * @return the value without surrounding space, or null when there is nothing
	 *         left of it
	 */
	protected static String trimToNull(String value) {
		if (value == null || value.trim().isEmpty()) {
			return null;
		}
		return value.trim();
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(UID)) {
			return "Id of the message, as returned by MicrosoftOutlookListMail.";
		} else if (key.equals(COMMENT)) {
			return "What to say above the message being answered or forwarded. Microsoft Outlook quotes the original underneath it.";
		} else if (key.equals(AS_DRAFT)) {
			return "Optional boolean to leave the message in Drafts instead of sending it, so somebody can read it first and send it with MicrosoftOutlookSendDraft. Defaults to false.";
		} else if (key.equals(MAX_BODY_CHARS)) {
			return "Optional longest body to return before it is truncated. Defaults to " + DEFAULT_MAX_BODY_CHARS
					+ ".";
		}
		return super.getDescriptionForKey(key);
	}

}
