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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.engine.api.IRDBMSEngine;
import prerna.util.Constants;
import prerna.util.SystemEngineRegistry;

/**
 * Theme accessors intended to be safe for all users (read-only).
 */
public class PlaygroundThemeUtils extends AbstractThemeUtils {

	private static final Logger classLogger = LogManager.getLogger(PlaygroundThemeUtils.class);
	private static final Object CACHE_LOCK = new Object();
	private static volatile String cachedGlobalSystemPrompt = null;
	private static volatile Map<String, String> cachedSystemPromptVars = null;
	private static volatile List<Map<String, Object>> cachedDefaultTools = null;
	private static volatile boolean cacheInitialized = false;

	private PlaygroundThemeUtils() {
	}

	/**
	 * Returns {@code playground.globalSystemPrompt} from the currently active
	 * theme, or {@code null} if not defined.
	 */
	public static String getPlaygroundGlobalSystemPrompt() {
		ensureCacheLoaded();
		return cachedGlobalSystemPrompt;
	}

	/**
	 * Returns {@code playground.systemPromptVars} from the currently active theme,
	 * or an empty map if not defined.
	 * <p>
	 * Expected JSON shape:
	 * 
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
		ensureCacheLoaded();
		return cachedSystemPromptVars == null ? new LinkedHashMap<>() : new LinkedHashMap<>(cachedSystemPromptVars);
	}

	/**
	 * Returns {@code playground.defaultTools} from the currently active theme,
	 * or an empty list if not defined.
	 */
	public static List<Map<String, Object>> getPlaygroundDefaultTools() {
		ensureCacheLoaded();
		return cachedDefaultTools == null ? Collections.emptyList() : new ArrayList<>(cachedDefaultTools);
	}

	/**
	 * Refreshes the in-memory cache from the active theme.
	 */
	public static void refreshCacheFromActiveTheme() {
		synchronized (CACHE_LOCK) {
			parseThemeMap(extractActiveThemeMapJson());
			cacheInitialized = true;
		}
	}

	private static void refreshCache() {
		synchronized (CACHE_LOCK) {
			cachedGlobalSystemPrompt = null;
			cachedSystemPromptVars = null;
			cachedDefaultTools = null;
			cacheInitialized = false;
		}
	}

	private static void ensureCacheLoaded() {
		if (cacheInitialized) {
			return;
		}
		refreshCacheFromActiveTheme();
	}

	private static void parseThemeMap(String themeMapJson) {
		cachedGlobalSystemPrompt = null;
		cachedSystemPromptVars = new LinkedHashMap<>();
		cachedDefaultTools = new ArrayList<>();
		if (themeMapJson == null) {
			return;
		}
		try {
			JsonObject themeMap = JsonParser.parseString(themeMapJson).getAsJsonObject();
			JsonElement playgroundElem = themeMap.get("playground");
			if (playgroundElem == null || !playgroundElem.isJsonObject()) {
				return;
			}
			JsonObject playground = playgroundElem.getAsJsonObject();

			JsonElement globalSystemPromptElem = playground.get("globalSystemPrompt");
			if (globalSystemPromptElem != null && globalSystemPromptElem.isJsonPrimitive()) {
				cachedGlobalSystemPrompt = StringUtils.trimToNull(globalSystemPromptElem.getAsString());
			}

			JsonElement varsElem = playground.get("systemPromptVars");
			if (varsElem != null && varsElem.isJsonObject()) {
				JsonObject varsObj = varsElem.getAsJsonObject();
				for (String key : varsObj.keySet()) {
					JsonElement valElem = varsObj.get(key);
					if (valElem == null || !valElem.isJsonPrimitive()) {
						continue;
					}
					String val = StringUtils.trimToNull(valElem.getAsString());
					if (val == null) {
						continue;
					}
					cachedSystemPromptVars.put(key, val);
				}
			}

			JsonElement defaultToolsElem = playground.get("defaultTools");
			if (defaultToolsElem != null && defaultToolsElem.isJsonArray()) {
				for (JsonElement toolElem : defaultToolsElem.getAsJsonArray()) {
					if (!toolElem.isJsonObject()) {
						continue;
					}
					Map<String, Object> tool = new LinkedHashMap<>();
					for (Map.Entry<String, JsonElement> entry : toolElem.getAsJsonObject().entrySet()) {
						if (entry.getValue().isJsonPrimitive()) {
							tool.put(entry.getKey(), entry.getValue().getAsString());
						}
					}
					tool.put("isDefault", true);
					cachedDefaultTools.add(tool);
				}
			}
		} catch (Exception e) {
			classLogger.debug(Constants.STACKTRACE, e);
		}
	}

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

	private static Map<String, Object> getActiveTheme() {
		IRDBMSEngine themeDb = SystemEngineRegistry.getThemesDb();
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
