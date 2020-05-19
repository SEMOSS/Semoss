package prerna.sablecc2.reactor.utils;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.EngineSyncUtility;

public class UnlockAppsReactor extends AbstractReactor {

	public UnlockAppsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.APP.getKey() };
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String appId = this.keyValue.get(this.keysToGet[0]);
		
		ConcurrentMap<String, ReentrantLock> locks = EngineSyncUtility.getAllLocks(this.insight.getUser());
		if(appId == null) {
			locks.clear();
		} else {
			locks.remove(appId);
		}
		// always return true as long as you are admin
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}
	
}
