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
package prerna.om;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import prerna.auth.User;

/**
 * In-memory singleton store for local user authentication/session data.
 * <p>
 * The store tracks raw user store details in {@code localStore} and resolved
 * {@link User} objects in {@code userCache}, both keyed by the same identifier.
 * </p>
 */
public class LocalUserStore {

	private final Map<String, Object[]> localStore = new ConcurrentHashMap<String, Object[]>();
	private final Map<String, User> userCache = new ConcurrentHashMap<String, User>();

	private volatile static LocalUserStore userStore;

	private LocalUserStore() {
		// do nothing
	}

	/**
	 * Returns the singleton {@link LocalUserStore} instance.
	 *
	 * @return the shared local user store
	 */
	public static LocalUserStore getInstance() {
		if (userStore != null) {
			return userStore;
		}

		synchronized (LocalUserStore.class) {
			if (userStore == null) {
				userStore = new LocalUserStore();
			}
		}
		return userStore;
	}

	/**
	 * Stores user-related details under the supplied key.
	 *
	 * @param key   unique identifier used to retrieve the stored values
	 * @param value details array expected by this store's retrieval/validation
	 *              logic
	 */
	public void store(String key, Object[] value) {
		localStore.put(key, value);
	}

	/**
	 * Validates a key by comparing the first stored value against the provided
	 * value.
	 *
	 * @param key   identifier to validate
	 * @param value value to compare to the first slot in the stored array
	 * @return {@code true} when the key exists and the first stored value matches;
	 *         otherwise {@code false}
	 */
	public boolean validate(String key, String value) {
		if (localStore.containsKey(key)) {
			if (localStore.get(key)[0].equals(value)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Returns the user store details currently held at positions 1 and 2 for the
	 * given key.
	 *
	 * @param key identifier used to look up stored details
	 * @return two-element array containing values from index 1 and 2 of the stored
	 *         entry
	 */
	public Object[] getUserStoreDetails(String key) {
		Object[] userStore = localStore.get(key);
		return new Object[] { userStore[1], userStore[2] };
	}

	/**
	 * Retrieves a cached {@link User} by key.
	 *
	 * @param key identifier used for user cache lookup
	 * @return cached user if present; otherwise {@code null}
	 */
	public User getCachedUser(String key) {
		return userCache.get(key);
	}

	/**
	 * Caches a {@link User} for a key when both inputs are non-null.
	 *
	 * @param key  identifier associated with the user
	 * @param user user object to cache
	 */
	public void cacheUser(String key, User user) {
		if (key != null && user != null) {
			userCache.put(key, user);
		}
	}

	/**
	 * Removes both local store and cached user entries for the provided key when
	 * the key is non-null.
	 *
	 * @param key identifier to remove from both maps
	 */
	public User remove(String key) {
		if (key != null) {
			localStore.remove(key);
			return userCache.remove(key);
		}
		return null;
	}

}
