/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.reactor.utils;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.ProjectSyncUtility;

public class UnlockProjectsReactor extends AbstractReactor {

	public UnlockProjectsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey()};
	}

	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(user);
		if (adminUtils == null) {
			throw new IllegalArgumentException("User must be an admin to perform this function");
		}

		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);

		boolean retBool = false;
		ConcurrentMap<String, ReentrantLock> locks = ProjectSyncUtility.getAllLocks(this.insight.getUser());
		if (projectId == null) {
			// unlock any current locks in use
			for (String key : locks.keySet()) {
				locks.get(key).unlock();
			}
			locks.clear();
			retBool = true;
		} else {
			ReentrantLock lock = locks.remove(projectId);
			lock.unlock();
			retBool = true;
		}

		return new NounMetadata(retBool, PixelDataType.BOOLEAN);
	}
}
