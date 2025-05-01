package prerna.reactor.utils;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EngineSyncUtility;

public class UnlockEngineReactor extends AbstractReactor {

	public UnlockEngineReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey() };
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		
		boolean retBool = false;
		// this method checks if you are an admin
		ConcurrentMap<String, ReentrantLock> locks = EngineSyncUtility.getAllLocks(this.insight.getUser());
		if(engineId == null) {
			// unlock any current locks in use
			for(String key : locks.keySet()) {
				ReentrantLock thisLock = locks.get(key);
				if(thisLock != null) {
					thisLock.unlock();
				}
			}
			locks.clear();
			retBool = true;
		} else {
			ReentrantLock lock = locks.remove(engineId);
			if(lock != null) {
				lock.unlock();
				retBool = true;
			}
			retBool = false;
		}
		
		return new NounMetadata(retBool, PixelDataType.BOOLEAN);
	}
	
}
