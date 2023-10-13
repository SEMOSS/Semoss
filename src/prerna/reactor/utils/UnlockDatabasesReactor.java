package prerna.reactor.utils;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EngineSyncUtility;

public class UnlockDatabasesReactor extends AbstractReactor {

	public UnlockDatabasesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DATABASE.getKey() };
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		
		boolean retBool = false;
		// this method checks if you are an admin
		ConcurrentMap<String, ReentrantLock> locks = EngineSyncUtility.getAllLocks(this.insight.getUser());
		if(databaseId == null) {
			// unlock any current locks in use
			for(String key : locks.keySet()) {
				locks.get(key).unlock();
			}
			locks.clear();
			retBool = true;
		} else {
			ReentrantLock lock = locks.remove(databaseId);
			lock.unlock();
			retBool = true;
		}
		
		return new NounMetadata(retBool, PixelDataType.BOOLEAN);
	}
	
}
