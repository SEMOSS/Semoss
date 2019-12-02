package prerna.sablecc2.reactor.app;

import prerna.om.AppAvailabilityStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class StartAppReactor extends AbstractReactor {

	public StartAppReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.APP.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String appId = this.keyValue.get(this.keysToGet[0]);
		
		AppAvailabilityStore availableAppStore = AppAvailabilityStore.getInstance();
		if(availableAppStore == null) {
			throw new IllegalArgumentException("Security must be enabled in the application to stop/start apps");
		}

		boolean enabled = availableAppStore.enableApp(this.insight.getUser(), appId);
		if(!enabled) {
			throw new IllegalArgumentException("User does not have access to enable the app. Must be the owner/author of the app to disable.");
		}
		
		// return the noun
		return new NounMetadata(enabled, PixelDataType.BOOLEAN);
	}
	
}
