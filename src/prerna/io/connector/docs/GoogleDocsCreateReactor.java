package prerna.io.connector.docs;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GoogleDocsCreateReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(GoogleDocsCreateReactor.class);

	public GoogleDocsCreateReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROMPT_TITLE.getKey(), ReactorKeysEnum.CONTENT.getKey() };
		this.keyRequired = new int[] { 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String title = this.keyValue.get(this.keysToGet[0]);
		String content = null;

		if (this.keyValue.get(this.keysToGet[1]) != null && !this.keyValue.get(this.keysToGet[1]).isEmpty()) {
			content = this.keyValue.get(this.keysToGet[1]);
		}

		try {
			User user = this.insight.getUser();
			String accessToken = GoogleDocsUtils.getGoogleAccessToken(user);
			return GoogleDocsHelper.createDoc(accessToken, title, content);
		} catch (Exception e) {
			classLogger.error("Unauthorized access or Please provide valid input");
			throw new SemossPixelException("Please provide valid input: " + e.getMessage(), e);
		}

	}

	@Override
	public String getReactorDescription() {
		return "This reactor is used to create the Google document.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.PROMPT_TITLE.getKey())) {
			return "Title of the Document " + ReactorKeysEnum.PROMPT_TITLE.getKey();
		} else if (key.equals(ReactorKeysEnum.CONTENT.getKey())) {
			return "Content to be added to the document " + ReactorKeysEnum.CONTENT.getKey();
		}
		return super.getDescriptionForKey(key);
	}

}
