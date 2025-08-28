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
package prerna.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;

public class ProjectSyncUtility {

	private static ConcurrentMap<String, ReentrantLock> prorjectLocks = new ConcurrentHashMap<>();

	public static ReentrantLock getProjectLock(String projectId) {
		prorjectLocks.putIfAbsent(projectId, new ReentrantLock());
		return prorjectLocks.get(projectId);
	}

	public static ConcurrentMap<String, ReentrantLock> getAllLocks(User user) {
		if (!SecurityAdminUtils.userIsAdmin(user)) {
			throw new IllegalArgumentException("User must be an admin to perform this method");
		}
		return prorjectLocks;
	}
}
