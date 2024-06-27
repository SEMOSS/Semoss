package prerna.forms;

import java.io.IOException;
import java.util.Map;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.api.IDatabaseEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

import prerna.util.Constants;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class UpdateFormReactor extends AbstractReactor {

	private static final String FORM_DATA = "form_input";

	private static final Logger classLogger = LogManager.getLogger(UpdateFormReactor.class);

	public UpdateFormReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey(), FORM_DATA};
	}

	@Override
	public NounMetadata execute() {
		String userId = null;
		User user = this.insight.getUser();
		if(user.getAccessToken(AuthProvider.CAC) != null) {
			userId = user.getAccessToken(AuthProvider.CAC).getId();
		} else if(user.getAccessToken(AuthProvider.SAML) != null) {
			// if not CAC - we are using SMAL
			userId = user.getAccessToken(AuthProvider.SAML).getId();
		}
		if(userId == null) {
			throw new IllegalArgumentException("Could not identify user");
		}
		
		String databaseName = this.store.getNoun(this.keysToGet[0]).get(0) + "";
		Map<String, Object> engineHash = (Map<String, Object>) this.store.getNoun(FORM_DATA).get(0);

		IDatabaseEngine engine = Utility.getDatabase(databaseName);
		AbstractFormBuilder formbuilder = FormFactory.getFormBuilder(engine);
		try {
			formbuilder.commitFormData(engineHash, userId);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			return new NounMetadata(false, PixelDataType.BOOLEAN);
		}
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}
	public String getName()
	{
		return "UpdateForms";
	}

}
