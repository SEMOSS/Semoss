package prerna.reactor.model;

import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.impl.model.ModelUsageRestrictionUtility;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetUserModelUsageReactor extends AbstractReactor {

    public GetUserModelUsageReactor() {
        this.keysToGet = new String[] {ReactorKeysEnum.ENGINE.getKey()};
        this.keyRequired = new int[] {1};
    }

    @Override
    public NounMetadata execute() {
    	organizeKeys();
        User user = insight.getUser();
        String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
        
        if (user == null) {
			throw new IllegalArgumentException("You are not properly logged in");
		}
        
		if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
			throw new IllegalArgumentException(
					"Model " + engineId + " does not exist or user does not have access to this model");
		}
        Map<String, Object> userRestrictionMap = ModelUsageRestrictionUtility.getModelUsageRestriction(user, engineId);

        return new NounMetadata(userRestrictionMap, PixelDataType.MAP);
    }
    
    @Override
    public String getReactorDescription() {
        return "Returns model usage restrictions for a user.";
    }
    
    @Override
    protected String getDescriptionForKey(String key) {
        if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
        	return "Engine id for which to check model usage restrictions";
        }
        return super.getDescriptionForKey(key);
    }
}