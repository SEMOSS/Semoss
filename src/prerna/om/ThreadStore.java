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
package prerna.om;

import java.util.HashMap;
import java.util.Map;
import prerna.auth.User;

public class ThreadStore {

	private static final ThreadLocal<Map<String, Object>> CURRENT = new ThreadLocal<Map<String, Object>>();

	private static Map<String, Object> getThreadMap() {
		Map<String, Object> map = CURRENT.get();
		if (map == null) {
			map = new HashMap<String, Object>();
			CURRENT.set(map);
		}
		return map;
	}

	public static Map<String, Object> getTheadMapObject() {
		return CURRENT.get();
	}

	public static void setThreadMapObject(Map<String, Object> mapValues) {
		Map<String, Object> map = CURRENT.get();
		map.putAll(mapValues);
	}

	public static void setInsightId(String insightId) {
		Map<String, Object> map = getThreadMap();
		map.put("insightId", insightId);
	}

	public static String getInsightId() {
		return (String) getThreadMap().get("insightId");
	}

	public static void setSessionId(String sessionId) {
		Map<String, Object> map = getThreadMap();
		map.put("sessionId", sessionId);
	}

	public static String getSessionId() {
		return (String) getThreadMap().get("sessionId");
	}

	public static void setRouteId(String routeId) {
		Map<String, Object> map = getThreadMap();
		map.put("routeId", routeId);
	}

	public static String getRouteId() {
		return (String) getThreadMap().get("routeId");
	}

	public static void setJobId(String jobId) {
		Map<String, Object> map = getThreadMap();
		map.put("jobId", jobId);
	}

	public static Boolean isSchedulerMode() {
		return (Boolean) getThreadMap().get("scheduler");
	}

	public static void setSchedulerMode(Boolean scheduler) {
		Map<String, Object> map = getThreadMap();
		map.put("scheduler", scheduler);
	}

	public static String getJobId() {
		return (String) getThreadMap().get("jobId");
	}

	public static void setUser(User user) {
		Map<String, Object> map = getThreadMap();
		map.put("user", user);
	}

	public static User getUser() {
		return (User) getThreadMap().get("user");
	}

	public static void remove() {
		CURRENT.remove();
	}
}
