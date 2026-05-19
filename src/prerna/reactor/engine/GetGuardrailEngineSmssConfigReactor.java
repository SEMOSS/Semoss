package prerna.reactor.engine;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.IGuardrailReactorFunctionEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class GetGuardrailEngineSmssConfigReactor extends AbstractReactor {

	public GetGuardrailEngineSmssConfigReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		 organizeKeys();

		 User user = this.insight.getUser();
		 if (user == null) {
			 throw new SemossPixelException("User must be signed into an account in order to use this reactor");
		 }
		 if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			 throwAnonymousUserError();
		 }

		 String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		 if (engineId == null || (engineId = engineId.trim()).isEmpty()) {
			 throw new SemossPixelException("Must input an engine id");
		 }

		 engineId = SecurityQueryUtils.testUserEngineIdForAlias(user, engineId);

		 if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
			 throw new SemossPixelException(
					 "Engine '" + engineId + "' does not exist or the user does not have access.");
		 }

		 IEngine.CATALOG_TYPE engineType = SecurityEngineUtils.getEngineType(engineId);
		 if (engineType != IEngine.CATALOG_TYPE.GUARDRAIL) {
			 throw new SemossPixelException("Engine '" + engineId + "' is not a guardrail engine.");
		 }

		 IGuardrailReactorFunctionEngine engine = Utility.getGuardrailEngine(engineId);
		 if (engine == null) {
			 throw new SemossPixelException("Could not load guardrail engine '" + engineId + "'.");
		 }

		 Map<String, String> result = new LinkedHashMap<>();
		 result.putAll(engine.getKeysAndValuesToGet());
		 return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}

	@Override
	public String getReactorDescription() {
		return "Returns the available labels, default threshold and future keys as well for a guardrail engine.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The guardrail engine id";
		}
		return super.getDescriptionForKey(key);
	}
}
