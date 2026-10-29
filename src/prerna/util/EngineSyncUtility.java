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
package prerna.util;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.engine.api.IEngine;

public class EngineSyncUtility {

	private static final Logger classLogger = LogManager.getLogger(EngineSyncUtility.class);

	private static ConcurrentMap<String, ReentrantLock> engineLocks = new ConcurrentHashMap<>();

	// store some database metadata as well
	public static ConcurrentMap<String, List<Object[]>> databaseStructureCache = new ConcurrentHashMap<>();
	public static ConcurrentMap<String, Map<String, Object>> metamodelCache = new ConcurrentHashMap<>();
	public static ConcurrentMap<String, Map<String, List<String>>> metamodelLogicalNamesCache = new ConcurrentHashMap<>();
	public static ConcurrentMap<String, Map<String, String>> metamodelDescriptionsCache = new ConcurrentHashMap<>();
	public static ConcurrentMap<String, Map<String, Object>> metamodelPositionsCache = new ConcurrentHashMap<>();

	public static ReentrantLock getEngineLock(String engineId) {
		engineLocks.putIfAbsent(engineId, new ReentrantLock());
		return engineLocks.get(engineId);
	}

	public static ConcurrentMap<String, ReentrantLock> getAllLocks(User user) {
		if (!SecurityAdminUtils.userIsAdmin(user)) {
			throw new IllegalArgumentException("User must be an admin to perform this method");
		}
		return engineLocks;
	}

	/**
	 * Checks if an engine is open on this node, meaning an instance of it is held
	 * in DIHelper. An engine that is only cataloged here, with its smss known but
	 * never opened, is not open.
	 *
	 * @param engineId the engine to check
	 * @return true if the engine is open on this node
	 */
	public static boolean isEngineOpen(String engineId) {
		return DIHelper.getInstance().getEngineProperty(engineId) instanceof IEngine;
	}

	/**
	 * Closes the engine instance opened on this node and removes it from DIHelper,
	 * along with its cached metadata, so the next {@link Utility#getEngine(String)}
	 * opens it again from its smss file and files. Closing shuts down the python
	 * process an engine owns. The close runs under the engine lock that loading
	 * uses, so it cannot interleave with a concurrent open. An engine that is not
	 * open on this node is left alone.
	 *
	 * @param engineId the engine to close
	 * @return true if the engine was open on this node and is now closed
	 */
	public static boolean closeLoadedEngine(String engineId) {
		ReentrantLock lock = getEngineLock(engineId);
		lock.lock();
		try {
			Object loaded = DIHelper.getInstance().getEngineProperty(engineId);
			if (!(loaded instanceof IEngine openEngine)) {
				classLogger.info("Engine {} is not open on this node, nothing to close",
						Utility.cleanLogString(engineId));
				return false;
			}
			classLogger.info("Closing engine {}", Utility.cleanLogString(engineId));
			try {
				openEngine.close();
			} catch (IOException e) {
				classLogger.error("Failed to close engine {}, removing it from DIHelper anyway",
						Utility.cleanLogString(engineId), e);
			}
			UploadUtilities.removeEngineExcludingSMSSFromDIHelper(engineId);
			clearEngineCache(engineId);
			classLogger.info("Closed engine {}", Utility.cleanLogString(engineId));
			return true;
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Get the database structure cache
	 * 
	 * @param engineId
	 * @return
	 */
	public static List<Object[]> getDatabaseStructureCache(String engineId) {
		return databaseStructureCache.get(engineId);
	}

	/**
	 * Set the database structure cache
	 * 
	 * @param engineId
	 * @param cacheObj
	 */
	public static void setDatabaseStructureCache(String engineId, List<Object[]> cacheObj) {
		databaseStructureCache.put(engineId, cacheObj);
	}

	/**
	 * Get the engine metamodel
	 * 
	 * @param engineId
	 * @return
	 */
	public static Map<String, Object> getMetamodel(String engineId) {
		return metamodelCache.get(engineId);
	}

	/**
	 * Set the engine metamodel
	 * 
	 * @param engineId
	 * @param cacheObj
	 */
	public static void setMetamodel(String engineId, Map<String, Object> cacheObj) {
		metamodelCache.put(engineId, cacheObj);
	}

	/**
	 * Get the engine logical names
	 * 
	 * @param engineId
	 * @return
	 */
	public static Map<String, List<String>> getMetamodelLogicalNamesCache(String engineId) {
		return metamodelLogicalNamesCache.get(engineId);
	}

	/**
	 * Set the engine logical names
	 * 
	 * @param engineId
	 * @param cacheObj
	 */
	public static void setMetamodelLogicalNames(String engineId, Map<String, List<String>> cacheObj) {
		metamodelLogicalNamesCache.put(engineId, cacheObj);
	}

	/**
	 * Get the engine descriptions
	 * 
	 * @param engineId
	 * @return
	 */
	public static Map<String, String> getMetamodelDescriptionsCache(String engineId) {
		return metamodelDescriptionsCache.get(engineId);
	}

	/**
	 * Set the engine descriptions
	 * 
	 * @param engineId
	 * @param cacheObj
	 */
	public static void setMetamodelDescriptions(String engineId, Map<String, String> cacheObj) {
		metamodelDescriptionsCache.put(engineId, cacheObj);
	}

	/**
	 * 
	 * @param engineId
	 * @return
	 */
	public static Map<String, Object> getMetamodelPositions(String engineId) {
		return metamodelPositionsCache.get(engineId);
	}

	/**
	 * 
	 * @param engineId
	 * @param cacheObj
	 */
	public static void setMetamodelPositions(String engineId, Map<String, Object> cacheObj) {
		metamodelPositionsCache.put(engineId, cacheObj);
	}

	/**
	 * Clear all the cached data for an engine
	 * 
	 * @param engineId
	 */
	public static void clearEngineCache(String engineId) {
		databaseStructureCache.remove(engineId);
		metamodelCache.remove(engineId);
		metamodelLogicalNamesCache.remove(engineId);
		metamodelDescriptionsCache.remove(engineId);
		metamodelPositionsCache.remove(engineId);
	}

}
