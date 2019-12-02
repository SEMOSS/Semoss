package prerna.sablecc2.reactor.app;

import prerna.auth.utils.SecurityAppUtils;
import prerna.om.AppAvailabilityStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class IsAppEnabledReactor extends AbstractReactor {

	public IsAppEnabledReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.APP.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String appId = this.keyValue.get(this.keysToGet[0]);

		AppAvailabilityStore availableAppStore = AppAvailabilityStore.getInstance();
		// if no security, all apps are always enabled
		if(availableAppStore == null) {
			return new NounMetadata(true, PixelDataType.BOOLEAN);
		}

		// check if user has access to app
		if(!SecurityAppUtils.userCanViewEngine(this.insight.getUser(), appId)) {
			throw new IllegalArgumentException("App does not exist or user does not have access");
		}
		boolean disabled = availableAppStore.isAppDisabledByOwner(appId);
		
		// return the noun
		// opposite of disabled
		return new NounMetadata(!disabled, PixelDataType.BOOLEAN);
	}

}
