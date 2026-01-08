package prerna.theme;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.util.Constants;

/**
 * Theme accessors intended to be safe for all users (read-only).
 */
public class PlaygroundThemeUtils extends AbstractThemeUtils {

	private static final Logger classLogger = LogManager.getLogger(PlaygroundThemeUtils.class);

	private PlaygroundThemeUtils() {
	}

	/**
	 * Returns {@code playground.globalSystemPrompt} from the currently active theme,
	 * or {@code null} if not defined.
	 */
	public static String getPlaygroundGlobalSystemPrompt() {
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

		String themeMapJson = StringUtils.trimToNull((String) themeMapObj);
		if (themeMapJson == null) {
			return null;
		}

		try {
			JsonObject themeMap = JsonParser.parseString(themeMapJson).getAsJsonObject();
			JsonElement playgroundElem = themeMap.get("playground");
			if (playgroundElem == null || !playgroundElem.isJsonObject()) {
				return null;
			}
			JsonObject playground = playgroundElem.getAsJsonObject();
			JsonElement globalSystemPromptElem = playground.get("globalSystemPrompt");
			if (globalSystemPromptElem != null && globalSystemPromptElem.isJsonPrimitive()) {
				return StringUtils.trimToNull(globalSystemPromptElem.getAsString());
			}
		} catch (Exception e) {
			classLogger.debug(Constants.STACKTRACE, e);
		}

		return null;
	}

	private static Map<String, Object> getActiveTheme() {
		if (themeDb == null) {
			return new HashMap<>();
		}

		final String THEME_PREFIX = "ADMIN_THEME__";
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(THEME_PREFIX + "ID"));
		qs.addSelector(new QueryColumnSelector(THEME_PREFIX + "THEME_NAME"));
		qs.addSelector(new QueryColumnSelector(THEME_PREFIX + "THEME_MAP"));
		qs.addSelector(new QueryColumnSelector(THEME_PREFIX + "IS_ACTIVE"));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter(THEME_PREFIX + "IS_ACTIVE", "==", true, PixelDataType.BOOLEAN));

		List<Map<String, Object>> retVal = null;
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(themeDb, qs);
			retVal = flushRsToMap(wrapper);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		if (retVal == null || retVal.isEmpty()) {
			return new HashMap<>();
		}
		return retVal.get(0);
	}
}

