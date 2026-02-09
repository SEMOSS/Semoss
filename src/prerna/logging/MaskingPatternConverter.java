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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.pattern.ConverterKeys;
import org.apache.logging.log4j.core.pattern.LogEventPatternConverter;
import org.apache.logging.log4j.core.pattern.PatternConverter;

import prerna.engine.impl.SmssUtilities;
import prerna.util.Constants;

@Plugin(name = "MaskMsg", category = PatternConverter.CATEGORY)
@ConverterKeys({ "maskMsg" })
public class MaskingPatternConverter extends LogEventPatternConverter {

	private final List<MaskRule> rules;

	protected MaskingPatternConverter(List<MaskRule> rules) {
		super("maskMsg", "maskMsg");
		this.rules = rules;
	}

	@Override
	public void format(LogEvent event, StringBuilder toAppendTo) {
		String message = event.getMessage().getFormattedMessage();
		for (MaskRule rule : rules) {
			Matcher m = rule.regex.matcher(message);
			message = m.replaceAll(rule.replacement);
		}
		toAppendTo.append(message);
	}

	@PluginFactory
	public static MaskingPatternConverter newInstance() {
		List<MaskRule> rules = new ArrayList<MaskRule>();

		List<String> valuesToMask = new ArrayList<>();
		valuesToMask.addAll(SmssUtilities.SENSITIVE_KEYWORDS);
		valuesToMask.add("private_key");

		valuesToMask.forEach(key -> {
			String escapedKey = Pattern.quote(key);

			// Pattern 1: key=value format in pixel
			String pattern1 = "(?i)(" + escapedKey + "\\s*=\\s*)[^, ]+";
			Pattern regex1 = Pattern.compile(pattern1);
			rules.add(new MaskRule(regex1, "$1" + Constants.SENSITIVE_INFO_MASK));

			// Pattern 2: JSON-style "key":"value" format
			String pattern2 = "(?i)(\"\\s*" + escapedKey + "\\s*\"\\s*:\\s*\")[^\"]*(\")";
			Pattern regex2 = Pattern.compile(pattern2);
			rules.add(new MaskRule(regex2, "$1" + Constants.SENSITIVE_INFO_MASK + "$2"));

			// Pattern 3: JDBC URL format - example for password parameter in connection
			String pattern3 = "(?i)([?&]" + escapedKey + "=)[^&\\s]+";
			Pattern regex3 = Pattern.compile(pattern3);
			rules.add(new MaskRule(regex3, "$1" + Constants.SENSITIVE_INFO_MASK));
		});

		return new MaskingPatternConverter(rules);
	}

	/*
	 * Simple utility class for regex replacements
	 */
	private static class MaskRule {
		final Pattern regex;
		final String replacement;

		MaskRule(Pattern regex, String replacement) {
			this.regex = regex;
			this.replacement = replacement;
		}
	}

}
