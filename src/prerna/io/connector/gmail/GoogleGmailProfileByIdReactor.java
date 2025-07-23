package prerna.io.connector.gmail;

import com.google.api.services.gmail.Gmail;
import com.google.api.services.gmail.model.Profile;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GoogleGmailProfileByIdReactor extends AbstractReactor{
	
	public GoogleGmailProfileByIdReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.USERID.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String emailId = this.keyValue.get(this.keysToGet[0]);
		try {
			User user = this.insight.getUser();
			String accessToken = GoogleGmailUtils.getGoogleAccessToken(user);
			Gmail GmailService = GoogleGmailUtils.getGmailServiceUsingToken(accessToken);
			Profile result = GoogleGmailHelper.getGmailProfileById(GmailService, emailId);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
			throw new SemossPixelException("Please provide valid input: " + e.getMessage(), e);
		}
		
	}
	
	@Override
	public String getReactorDescription() {
		return "This reactor is used to get the GmailProfile details";
	}

}
