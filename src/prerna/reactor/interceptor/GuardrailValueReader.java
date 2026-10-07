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
package prerna.reactor.interceptor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MessagePart;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.engine.impl.model.message.TextMessagePart;
import prerna.engine.impl.model.responses.AbstractModelEngineResponse;

/**
 * Reduces an intercepted value to the content a guardrail can screen.
 *
 * An engine call carries its text inside whatever objects that engine's API
 * uses - a message, a response wrapper, a map of request fields, or a
 * collection of any of those. A guardrail reads parameters as text, so a value
 * handed over as its own object reaches the guardrail as the object's default
 * string form. Reducing the value first is what makes a guardrail able to
 * screen it.
 */
public final class GuardrailValueReader {

	private static final Logger classLogger = LogManager.getLogger(GuardrailValueReader.class);

	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

	private GuardrailValueReader() {
	}

	/**
	 * Reduces a value to the content a guardrail screens. Messages become their
	 * text, response wrappers become their payload, and a collection is reduced
	 * item by item so a list of messages arrives as a list of text. Anything else
	 * is returned as it is.
	 *
	 * @param value value read from the intercepted call
	 * @return the content to hand the guardrail
	 */
	public static Object screenableValue(Object value) {
		if (value instanceof AbstractMessage) {
			return messageText((AbstractMessage) value);
		}
		if (value instanceof AbstractModelEngineResponse) {
			return screenableValue(((AbstractModelEngineResponse<?>) value).getResponse());
		}
		if (value instanceof Map) {
			return toJson(value);
		}
		if (value instanceof Collection) {
			return screenableCollection((Collection<?>) value);
		}
		return value;
	}

	/**
	 * Reduces every item of a collection. Items that all reduce to text are joined
	 * into one value, because a guardrail reads a text parameter as a single value
	 * and would otherwise screen only the first item. A collection whose items are
	 * not all text keeps its shape, so a guardrail parameter that genuinely wants a
	 * list still receives one.
	 *
	 * @param values collection read from the intercepted call
	 * @return the joined text, or the reduced collection
	 */
	private static Object screenableCollection(Collection<?> values) {
		List<Object> screenable = new ArrayList<>();
		boolean allText = true;
		for (Object item : values) {
			Object reduced = screenableValue(item);
			allText = allText && reduced instanceof String;
			screenable.add(reduced);
		}
		if (!allText || screenable.isEmpty()) {
			return screenable;
		}
		StringBuilder joined = new StringBuilder();
		for (Object item : screenable) {
			if (joined.length() > 0) {
				joined.append('\n');
			}
			joined.append((String) item);
		}
		return joined.toString();
	}

	/**
	 * Serializes a value so a guardrail reads its fields rather than the shape Java
	 * prints by default. A value that cannot be serialized falls back to that
	 * default rather than failing the intercepted call, since a guardrail refusing
	 * to run would block traffic over a formatting problem.
	 *
	 * @param value value to serialize
	 * @return the value as JSON, or its default string form
	 */
	private static String toJson(Object value) {
		try {
			return GSON.toJson(value);
		} catch (RuntimeException e) {
			classLogger.warn("Unable to serialize a guardrail input of type {}; using its default string form",
					value.getClass().getName(), e);
			return String.valueOf(value);
		}
	}

	/**
	 * The text of a message. Each message type already knows which of its parts
	 * carries the text a model would see, so those accessors are used rather than
	 * re-deriving it; a message type with neither falls back to collecting its text
	 * parts.
	 *
	 * @param message message to read
	 * @return the message text, or null when it carries none
	 */
	public static String messageText(AbstractMessage message) {
		if (message == null) {
			return null;
		}
		if (message instanceof InputMessage) {
			return ((InputMessage) message).getFullInputPrompt();
		}
		if (message instanceof ResponseMessage) {
			return ((ResponseMessage) message).getContent();
		}

		List<MessagePart> parts = message.getParts();
		if (parts == null) {
			return null;
		}
		StringBuilder text = new StringBuilder();
		for (MessagePart part : parts) {
			if (!(part instanceof TextMessagePart)) {
				continue;
			}
			String partText = ((TextMessagePart) part).getText();
			if (partText == null || partText.isEmpty()) {
				continue;
			}
			if (text.length() > 0) {
				text.append('\n');
			}
			text.append(partText);
		}
		return text.length() > 0 ? text.toString() : null;
	}
}
