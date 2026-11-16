/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.auth.utils.reactors.admin;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EngineSyncUtility;

public class AdminUnlockEngineReactor extends AbstractReactor {

	public AdminUnlockEngineReactor() {
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
		}
		
		return new NounMetadata(retBool, PixelDataType.BOOLEAN);
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition()
				.description("Admin Only: Releases the lock this server holds on an engine's files, or every "
						+ "engine lock on this server when no engine is given")
				.key(ReactorKeysEnum.ENGINE, "The engine to unlock. Without it every engine lock is released")
				.output(Boolean.class, "Whether a lock was released; always true without an engine");
	}
}
