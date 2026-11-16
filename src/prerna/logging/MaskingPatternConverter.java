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
package prerna.logging;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.pattern.ConverterKeys;
import org.apache.logging.log4j.core.pattern.LogEventPatternConverter;
import org.apache.logging.log4j.core.pattern.PatternConverter;

import prerna.security.SensitiveKeys;
import prerna.util.Constants;

/**
 * The {@code %maskMsg} log pattern: the log message with the values of secret
 * keys masked. A value is masked in a URL query ({@code ?key=value}), a JSON
 * string property ({@code "key":"value"}) or an assignment ({@code key=value})
 * when {@link SensitiveKeys#isSensitiveKey(String)} says its key is secret.
 */
@Plugin(name = "MaskMsg", category = PatternConverter.CATEGORY)
@ConverterKeys({ "maskMsg" })
public class MaskingPatternConverter extends LogEventPatternConverter {

	private static final Pattern QUERY_PARAMETER = Pattern.compile("([?&])([A-Za-z0-9_.-]+)=[^&\\s\"']+");
	private static final Pattern JSON_PROPERTY = Pattern.compile("(\"\\s*([A-Za-z0-9_.-]+)\\s*\"\\s*:\\s*\")[^\"]*\"");
	private static final Pattern ASSIGNMENT = Pattern
			.compile("(?<![A-Za-z0-9_.-])([A-Za-z0-9_.-]+)(\\s*=\\s*)[^,&\\s]+");

	protected MaskingPatternConverter() {
		super("maskMsg", "maskMsg");
	}

	@Override
	public void format(LogEvent event, StringBuilder toAppendTo) {
		toAppendTo.append(mask(event.getMessage().getFormattedMessage()));
	}

	/**
	 * Masks the values of secret keys in a log message.
	 *
	 * @param message the formatted log message
	 * @return the message with secret values replaced by
	 *         {@link Constants#SENSITIVE_INFO_MASK}
	 */
	static String mask(String message) {
		if (message == null || message.isEmpty()) {
			return message;
		}
		String masked = QUERY_PARAMETER.matcher(message)
				.replaceAll(m -> Matcher.quoteReplacement(SensitiveKeys.isSensitiveKey(m.group(2))
						? m.group(1) + m.group(2) + "=" + Constants.SENSITIVE_INFO_MASK
						: m.group()));
		masked = JSON_PROPERTY.matcher(masked)
				.replaceAll(m -> Matcher.quoteReplacement(
						SensitiveKeys.isSensitiveKey(m.group(2)) ? m.group(1) + Constants.SENSITIVE_INFO_MASK + "\""
								: m.group()));
		return ASSIGNMENT.matcher(masked)
				.replaceAll(m -> Matcher.quoteReplacement(SensitiveKeys.isSensitiveKey(m.group(1))
						? m.group(1) + m.group(2) + Constants.SENSITIVE_INFO_MASK
						: m.group()));
	}

	@PluginFactory
	public static MaskingPatternConverter newInstance() {
		return new MaskingPatternConverter();
	}
}
