package prerna.sablecc2.reactor.storage;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IStorage;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Utility;

public class StorageReactor extends AbstractReactor {

	public StorageReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.STORAGE.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		// get the selectors
		this.organizeKeys();
		String storageId = this.keyValue.get(this.keysToGet[0]);
		// we may have the alias
		if(AbstractSecurityUtils.securityEnabled()) {
			storageId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), storageId);
			if(!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), storageId)) {
				throw new IllegalArgumentException("Storage " + storageId + " does not exist or user does not have access to storage");
			}
		} else {
			storageId = MasterDatabaseUtility.testDatabaseIdIfAlias(storageId);
			if(!MasterDatabaseUtility.getAllDatabaseIds().contains(storageId)) {
				throw new IllegalArgumentException("Storage " + storageId + " does not exist");
			}
		}
		
		IStorage storage = Utility.getStorage(storageId);
		return new NounMetadata(storage, PixelDataType.STORAGE);
	}
	
}
