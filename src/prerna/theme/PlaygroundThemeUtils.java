package prerna.theme;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.util.Constants;

/**
 * Theme accessors intended to be safe for all users (read-only).
 */
public class PlaygroundThemeUtils extends AbstractThemeUtils {

	private static final Logger classLogger = LogManager.getLogger(PlaygroundThemeUtils.class);
	private static final Object CACHE_LOCK = new Object();
	private static volatile String cachedGlobalSystemPrompt = null;
	private static volatile Map<String, String> cachedSystemPromptVars = null;
	private static volatile boolean cacheInitialized = false;

	private PlaygroundThemeUtils() {
	}

	/**
	 * Returns {@code playground.globalSystemPrompt} from the currently active theme,
	 * or {@code null} if not defined.
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
			if (varsElem == null || !varsElem.isJsonObject()) {
				return;
			}
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
