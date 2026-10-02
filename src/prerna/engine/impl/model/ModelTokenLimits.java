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
package prerna.engine.impl.model;

import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.impl.CaseInsensitiveProperties;
import prerna.util.Constants;

/** Token limits shared by local and remote model engines. */
public record ModelTokenLimits(int contextWindow, Long maxTokens) {

	private static final Logger classLogger = LogManager.getLogger(ModelTokenLimits.class);

	/**
	 * A metadata row owns both limits, including explicit NULL values. Only
	 * engines without a row use legacy SMSS limits; the input properties are
	 * never modified.
	 */
	public static ModelTokenLimits resolve(Map<String, Object> metadata, Properties smssProperties) {
		Properties legacy = new CaseInsensitiveProperties(smssProperties);
		Long context = positiveLong(metadata == null ? legacy.getProperty(Constants.CONTEXT_WINDOW)
				: metadata.get("contextWindow"), Constants.CONTEXT_WINDOW);
		Long maxTokens = positiveLong(metadata == null ? legacy.getProperty(Constants.MAX_TOKENS)
				: metadata.get("maxOutputTokens"), Constants.MAX_TOKENS);
		if (context != null && context > Integer.MAX_VALUE) {
			classLogger.warn("Context window {} exceeds the supported integer range - treating it as unknown", context);
			context = null;
		}
		return new ModelTokenLimits(context == null ? 0 : context.intValue(), maxTokens);
	}

	/** Make legacy property consumers use the same effective limits as the engine. */
	public void applyTo(CaseInsensitiveProperties properties) {
		setOrRemove(properties, Constants.CONTEXT_WINDOW, contextWindow > 0 ? contextWindow : null);
		setOrRemove(properties, Constants.MAX_TOKENS, maxTokens);
	}

	private static void setOrRemove(Properties properties, String key, Number value) {
		if (value == null) {
			properties.remove(key);
		} else {
			properties.setProperty(key, value.toString());
		}
	}

	private static Long positiveLong(Object value, String key) {
		if (value == null || value.toString().isBlank()) {
			return null;
		}
		try {
			long parsed = Long.parseLong(value.toString().trim());
			if (parsed > 0) {
				return parsed;
			}
		} catch (NumberFormatException e) {
			// Treat malformed legacy values as unknown, just like missing limits.
		}
		classLogger.warn("Invalid model setting {}={} - treating it as unset", key, value);
		return null;
	}
}
