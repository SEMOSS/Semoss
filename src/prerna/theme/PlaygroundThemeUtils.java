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
package prerna.theme;

import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.util.Constants;
import prerna.util.Utility;

/**
 * Theme accessors intended to be safe for all users (read-only).
 *
 * Uses Caffeine cache with automatic time-based expiration to ensure
 * theme changes propagate across distributed instances.
 */
public class PlaygroundThemeUtils extends AbstractThemeUtils {

	private static final Logger classLogger = LogManager.getLogger(PlaygroundThemeUtils.class);

	// Cache keys
	private static final String CACHE_KEY_SYSTEM_PROMPT = "globalSystemPrompt";
	private static final String CACHE_KEY_PROMPT_VARS = "systemPromptVars";

	// Default cache duration: 10 minutes
	private static final long DEFAULT_CACHE_DURATION_MINUTES = 10L;

	/**
	 * Caffeine cache with time-based expiration.
	 * Automatically refreshes from database after expiration.
	 * Duration is configured via THEME_CACHE_DURATION_MINUTES property.
	 */
	private static final Cache<String, Object> THEME_CACHE = initializeCache();

	private PlaygroundThemeUtils() {
	}

	/**
	 * Initialize the Caffeine cache with configured duration.
	 */
	private static Cache<String, Object> initializeCache() {
		long cacheDurationMinutes = DEFAULT_CACHE_DURATION_MINUTES;

		// Read from properties file (e.g., RDF_Map.prop)
		String configuredDuration = Utility.getDIHelperProperty(Constants.THEME_CACHE_DURATION_MINUTES);
		if (configuredDuration != null && !configuredDuration.trim().isEmpty()) {
			try {
				cacheDurationMinutes = Long.parseLong(configuredDuration.trim());
				classLogger.info("Theme cache duration set to {} minutes from configuration", cacheDurationMinutes);
			} catch (NumberFormatException e) {
				classLogger.warn("Invalid THEME_CACHE_DURATION_MINUTES value '{}', using default {} minutes",
						configuredDuration, DEFAULT_CACHE_DURATION_MINUTES);
			}
		} else {
			classLogger.info("THEME_CACHE_DURATION_MINUTES not configured, using default {} minutes",
					DEFAULT_CACHE_DURATION_MINUTES);
		}

		return Caffeine.newBuilder()
				.expireAfterWrite(Duration.ofMinutes(cacheDurationMinutes))
				.maximumSize(10) // Small cache, just a few theme properties
				.recordStats() // Enable statistics for monitoring
				.build();
	}

	/**
	 * Returns {@code playground.globalSystemPrompt} from the currently active theme,
	 * or {@code null} if not defined.
	 *
	 * Value is cached and automatically refreshed based on configured duration.
	 */
	public static String getPlaygroundGlobalSystemPrompt() {
		String result = (String) THEME_CACHE.get(CACHE_KEY_SYSTEM_PROMPT, key -> {
			// This function is called only when cache misses or expires
			loadThemeIntoCache();
			return THEME_CACHE.getIfPresent(CACHE_KEY_SYSTEM_PROMPT);
		});
		return result;
	}

	/**
	 * Returns {@code playground.systemPromptVars} from the currently active theme,
	 * or an empty map if not defined.
	 *
	 * Value is cached and automatically refreshed based on configured duration.
	 *
	 * <p>Expected JSON shape:
	 * <pre>
	 * {
	 *   "playground": {
	 *     "systemPromptVars": {
	 *       "DATE": "Date();",
	 *       "USER_INFO": "GetUserInfo();"
	 *     }
	 *   }
	 * }
	 * </pre>
	 */
	public static Map<String, String> getPlaygroundSystemPromptVars() {
		@SuppressWarnings("unchecked")
		Map<String, String> cached = (Map<String, String>) THEME_CACHE.get(CACHE_KEY_PROMPT_VARS, key -> {
			// This function is called only when cache misses or expires
			loadThemeIntoCache();
			return THEME_CACHE.getIfPresent(CACHE_KEY_PROMPT_VARS);
		});

		// Return defensive copy to prevent external modification
		return cached == null ? new LinkedHashMap<>() : new LinkedHashMap<>(cached);
	}

	/**
	 * Manually refreshes the cache from the active theme.
	 * Useful for immediate updates when an admin changes the theme.
	 */
	public static void refreshCacheFromActiveTheme() {
		classLogger.debug("Manually refreshing theme cache");
		invalidateCache();
		loadThemeIntoCache();
	}

	/**
	 * Invalidates the cache, forcing a reload on next access.
	 * Use this when you know the theme has changed but don't want to load immediately.
	 */
	public static void invalidateCache() {
		classLogger.debug("Invalidating theme cache");
		THEME_CACHE.invalidateAll();
	}

	/**
	 * Get cache statistics for monitoring.
	 * Useful for understanding cache hit/miss rates.
	 *
	 * @return Cache statistics string including hit rate, miss rate, etc.
	 */
	public static String getCacheStats() {
		return THEME_CACHE.stats().toString();
	}

	/**
	 * Logs cache statistics at INFO level.
	 * Call this periodically to monitor cache effectiveness.
	 */
	public static void logCacheStats() {
		classLogger.info("Theme cache statistics: {}", getCacheStats());
	}

	/**
	 * Loads theme data from the database into the cache.
	 * This is called automatically when cache expires or is invalidated.
	 */
	private static void loadThemeIntoCache() {
		classLogger.debug("Loading theme data into cache");
		String themeMapJson = extractActiveThemeMapJson();
		parseAndCacheThemeMap(themeMapJson);
	}

	/**
	 * Parses the theme JSON and populates the cache.
	 */
	private static void parseAndCacheThemeMap(String themeMapJson) {
		String globalSystemPrompt = null;
		Map<String, String> systemPromptVars = new LinkedHashMap<>();

		if (themeMapJson != null) {
			try {
				JsonObject themeMap = JsonParser.parseString(themeMapJson).getAsJsonObject();
				JsonElement playgroundElem = themeMap.get("playground");

				if (playgroundElem != null && playgroundElem.isJsonObject()) {
					JsonObject playground = playgroundElem.getAsJsonObject();

					// Extract globalSystemPrompt
					JsonElement globalSystemPromptElem = playground.get("globalSystemPrompt");
					if (globalSystemPromptElem != null && globalSystemPromptElem.isJsonPrimitive()) {
						globalSystemPrompt = StringUtils.trimToNull(globalSystemPromptElem.getAsString());
					}

					// Extract systemPromptVars
					JsonElement varsElem = playground.get("systemPromptVars");
					if (varsElem != null && varsElem.isJsonObject()) {
						JsonObject varsObj = varsElem.getAsJsonObject();
						for (String key : varsObj.keySet()) {
							JsonElement valElem = varsObj.get(key);
							if (valElem != null && valElem.isJsonPrimitive()) {
								String val = StringUtils.trimToNull(valElem.getAsString());
								if (val != null) {
									systemPromptVars.put(key, val);
								}
							}
						}
					}
				}
			} catch (Exception e) {
				classLogger.warn("Error parsing theme JSON, using empty values", e);
			}
		}

		// Populate cache
		THEME_CACHE.put(CACHE_KEY_SYSTEM_PROMPT, globalSystemPrompt);
		THEME_CACHE.put(CACHE_KEY_PROMPT_VARS, systemPromptVars);

		classLogger.debug("Theme cache loaded: globalSystemPrompt={}, systemPromptVars.size={}",
				globalSystemPrompt != null, systemPromptVars.size());
	}

	/**
	 * Extracts the active theme's JSON from the database.
	 */
	private static String extractActiveThemeMapJson() {
		Map<String, Object> theme = getActiveTheme();
		if (theme.isEmpty()) {
			return null;
		}

		Object themeMapObj = theme.get("ADMIN_THEME__THEME_MAP");
		if (!(themeMapObj instanceof String)) {
			themeMapObj = theme.get("THEME_MAP");
		}
		if (!(themeMapObj instanceof String)) {
			return null;
		}

		return StringUtils.trimToNull((String) themeMapObj);
	}

	/**
	 * Retrieves the active theme from the database.
	 */
	private static Map<String, Object> getActiveTheme() {
		if (themeDb == null) {
			return new HashMap<>();
		}

		Object themeObj = AdminThemeUtils.getActiveAdminTheme();
		if (themeObj instanceof Map<?, ?>) {
			@SuppressWarnings("unchecked")
			Map<String, Object> theme = (Map<String, Object>) themeObj;
			return theme;
		}
		return new HashMap<>();
	}
}
