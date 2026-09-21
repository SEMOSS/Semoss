package prerna.io.connector.ms.outlook;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Lists the mail folders of the signed in user's mailbox.
 *
 * <p>
 * Required delegated Microsoft Graph scope:
 * </p>
 * <ul>
 * <li>{@code Mail.Read} for {@code GET /me/mailFolders}. {@code Mail.ReadWrite}
 * also satisfies it.</li>
 * </ul>
 *
 * <p>
 * This is how a caller finds out what a mailbox is actually organised into,
 * rather than guessing at folder names. The id of a folder here is what
 * {@code MicrosoftOutlookListMail} takes as its {@code folder} and what
 * {@code MicrosoftOutlookMoveMail} moves into.
 * </p>
 *
 * <p>
 * Only the top level of the mailbox comes back. A folder somebody nested inside
 * another is reachable by its id but is not listed here.
 * </p>
 */
public class MicrosoftOutlookListMailFoldersReactor extends AbstractMicrosoftOutlookMessageReactor {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftOutlookListMailFoldersReactor.class);

	public MicrosoftOutlookListMailFoldersReactor() {
		this.keysToGet = new String[] {};
		this.keyRequired = new int[] {};
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();

		try {
			User user = this.insight.getUser();
			String accessToken = MicrosoftLoginUtils.getMicrosoftAccessToken(user);
			List<Map<String, Object>> found = new MicrosoftOutlookMailHelper().listFolders(accessToken, null);

			List<Map<String, Object>> folders = new ArrayList<>();
			for (Map<String, Object> folder : found) {
				Map<String, Object> described = new LinkedHashMap<>();
				described.put("id", folder.get("id"));
				MicrosoftOutlookMessageMapper.putIfPresent(described, "name", folder.get("displayName"));
				MicrosoftOutlookMessageMapper.putIfPresent(described, "totalItemCount", folder.get("totalItemCount"));
				folders.add(described);
			}

			Map<String, Object> output = new LinkedHashMap<>();
			output.put("count", folders.size());
			output.put("folders", folders);
			return new NounMetadata(output, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while listing the signed in user's mail folders", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to list the signed in user's mail folders", e);
			throw new SemossPixelException(
					"An error occurred retrieving the list of folders. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "List the mail folders of the signed in user's own Microsoft 365 mailbox.";
	}
}
