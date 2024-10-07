package prerna.reactor.prompt;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityPromptUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class CheckPromptTitleReactor extends AbstractReactor {

	public CheckPromptTitleReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.PROMPT_TITLE.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		// TODO Auto-generated method stub
		User user = this.insight.getUser();
		String userId = this.insight.getUserId();
		if (user == null) {
			NounMetadata noun = new NounMetadata(
					"User must be signed into an account in order to create prompt", PixelDataType.CONST_STRING,
					PixelOperationType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR);
			SemossPixelException err = new SemossPixelException(noun);
			err.setContinueThreadOfExecution(false);
			throw err;
		}

		if (AbstractSecurityUtils.anonymousUsersEnabled()) {
			if (this.insight.getUser().isAnonymous()) {
				throwAnonymousUserError();
			}
		}
		
		organizeKeys();
		String promptTitle = this.keyValue.get(this.keysToGet[0]);
		Boolean promptTitleUsed = SecurityPromptUtils.checkPromptTitle(promptTitle);
		NounMetadata nm = new NounMetadata(promptTitleUsed, PixelDataType.BOOLEAN);
		return nm;
	}

}
