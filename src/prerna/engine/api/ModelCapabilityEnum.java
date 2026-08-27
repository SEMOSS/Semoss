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
package prerna.engine.api;

import java.util.Locale;

/**
 * Capabilities supported by model metadata.
 */
public enum ModelCapabilityEnum {
	TEXT_GENERATION, IMAGE_GENERATION, VIDEO_GENERATION, EMBEDDING, TRANSCRIPTION, SPEECH_SYNTHESIS, RERANKING,
	MODERATION;

	/**
	 * Parse a capability name case-insensitively, accepting the common aliases
	 * (CHAT/LLM, EMBEDDINGS, TTS/TEXT_TO_SPEECH, STT/SPEECH_TO_TEXT) so every
	 * entry point resolves the same input to the same capability.
	 *
	 * @param value capability name or alias
	 * @return the matching capability
	 * @throws IllegalArgumentException when the value is null, blank, or unknown
	 */
	public static ModelCapabilityEnum fromName(String value) {
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException("Unsupported model capability " + value);
		}
		String normalizedValue = value.trim().toUpperCase(Locale.ROOT).replace('-', '_').replace(' ', '_');
		normalizedValue = switch (normalizedValue) {
		case "CHAT", "LLM" -> TEXT_GENERATION.name();
		case "EMBEDDINGS" -> EMBEDDING.name();
		case "TTS", "TEXT_TO_SPEECH" -> SPEECH_SYNTHESIS.name();
		case "STT", "SPEECH_TO_TEXT" -> TRANSCRIPTION.name();
		default -> normalizedValue;
		};
		try {
			return valueOf(normalizedValue);
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException("Unsupported model capability " + normalizedValue, e);
		}
	}
}
