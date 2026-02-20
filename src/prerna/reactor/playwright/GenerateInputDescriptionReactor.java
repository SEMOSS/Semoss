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
package prerna.reactor.playwright;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.microsoft.playwright.Locator;
import com.microsoft.playwright.Page;

import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class GenerateInputDescriptionReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GenerateInputDescriptionReactor.class);

	// JavaScript code to extract DOM context for an input field
	private static final String JS_EXTRACT_DOM_CONTEXT = """
			(el) => {
			  const attrMap = {};
			  for (const a of el.attributes) attrMap[a.name] = a.value;

			  const aria = {};
			  for (const [k, v] of Object.entries(attrMap)) {
			    if (k.startsWith('aria-')) aria[k] = v;
			  }

			  // Label by for= association
			  let labelText = null;
			  if (el.id) {
			    const l = document.querySelector('label[for="' + el.id + '"]');
			    if (l) labelText = l.innerText.trim();
			  }
			  // Proximity fallback
			  if (!labelText) {
			    let p = el.parentElement;
			    while (p && p !== document.body) {
			      const prox = p.querySelector('label');
			      if (prox) { labelText = prox.innerText.trim(); break; }
			      p = p.parentElement;
			    }
			  }

			  // Ancestry (limit depth to 5)
			  const ancestry = [];
			  let cur = el.parentElement;
			  let depth = 0;
			  while (cur && depth < 5) {
			    ancestry.push({
			      tag: cur.tagName.toLowerCase(),
			      id: cur.id || null,
			      className: cur.className || null,
			      role: cur.getAttribute('role'),
			      outerHTML: cur.outerHTML.slice(0, 1500)
			    });
			    cur = cur.parentElement;
			    depth++;
			  }

			  const form = el.closest('form');
			  const fieldset = el.closest('fieldset');
			  const heading = el.closest('section,div,form,fieldset')?.querySelector('h1,h2,h3,h4,h5,h6');

			  // Nearby text (simple scan)
			  const nearbyTexts = [];
			  const container = el.closest('div,form,section,fieldset') || el.parentElement || el;
			  const walker = document.createTreeWalker(container, NodeFilter.SHOW_TEXT);
			  let node;
			  while ((node = walker.nextNode()) && nearbyTexts.length < 8) {
			    const t = node.textContent.trim();
			    if (t && t.length > 0 && t.length < 200) nearbyTexts.push(t);
			  }

			  return {
			    labelText,
			    input: {
			      outerHTML: el.outerHTML.slice(0, 2000),
			      tag: el.tagName.toLowerCase(),
			      attributes: {
			        id: el.id || null,
			        name: el.name || null,
			        type: el.type || null,
			        placeholder: el.placeholder || null,
			        value: el.value || null,
			        autocomplete: el.getAttribute('autocomplete'),
			        pattern: el.getAttribute('pattern'),
			        required: el.required || false,
			        maxlength: el.maxLength > 0 ? el.maxLength : null,
			        role: el.getAttribute('role'),
			        ...aria
			      }
			    },
			    ancestry,
			    form: form ? {
			      name: form.getAttribute('name'),
			      method: form.getAttribute('method'),
			      action: form.getAttribute('action'),
			      outerHTML: form.outerHTML.slice(0, 1500)
			    } : null,
			    fieldsetLegend: fieldset?.querySelector('legend')?.innerText?.trim() || null,
			    headingText: heading?.innerText?.trim() || null,
			    nearbyText: nearbyTexts.slice(0, 5)
			  };
			}
			""";

	/**
	 * Constructor for the GenerateInputDescriptionReactor. Initializes the required
	 * keys the reactor expects: engine, sessionId, selector, and tabId.
	 */
	public GenerateInputDescriptionReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), "sessionId", "selector", "tabId" };
		this.keyRequired = new int[] { 1, 1, 1, 1 };
	}

	/**
	 * Executes the reactor logic to generate a description for an input field.
	 *
	 * @return NounMetadata containing the generated description for the input
	 *         field.
	 * @throws IllegalArgumentException if engineId is null or empty
	 * @throws IllegalArgumentException if sessionId is invalid or session not found
	 * @throws IllegalArgumentException if the element is not found or context
	 *                                  extraction fails
	 */
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		if (engineId == null || engineId.isEmpty()) {
			throw new IllegalArgumentException("Model engineId is required");
		}

		String sessionId = this.keyValue.get(this.keysToGet[1]);
		String selector = this.keyValue.get(this.keysToGet[2]);
		String tabId = this.keyValue.get(this.keysToGet[3]);

		PlaywrightSession session = this.insight.getUser().getPlaywrightSession(sessionId);
		if (session == null) {
			throw new IllegalArgumentException("Invalid session ID: " + sessionId);
		}

		Map<String, Object> domContext = extractDomContext(session, selector, tabId);
		String description = generateInputDescription(engineId, domContext, session, tabId);
		return new NounMetadata(description, PixelDataType.CONST_STRING);
	}

	/**
	 * Extracts the DOM context for a given element using the Playwright session.
	 *
	 * @param session  The Playwright session.
	 * @param selector The CSS selector for the target element.
	 * @param tabId    The ID of the tab containing the target element.
	 * @return A map containing the extracted DOM context.
	 * @throws IllegalArgumentException if tabId is invalid or session not found
	 */
	private Map<String, Object> extractDomContext(PlaywrightSession session, String selector, String tabId) {
		Page page = session.tabPages.get(tabId);
		if (page == null) {
			throw new IllegalArgumentException("Invalid tab ID or page not found");
		}

		Locator locator = page.locator(selector);

		// Execute JavaScript to extract DOM context
		Object result = locator.evaluate(JS_EXTRACT_DOM_CONTEXT);

		if (result == null) {
			throw new IllegalArgumentException(
					"Element not found or context extraction failed for selector: " + selector);
		}

		return (Map<String, Object>) result;
	}

	/**
	 * Generates a user-friendly description for an input field based on its DOM
	 * context.
	 *
	 * @param engineId   The ID of the model engine to use.
	 * @param domContext The extracted DOM context of the input field.
	 * @param session    The Playwright session.
	 * @param tabId      The ID of the tab containing the input field.
	 * @return The generated description as a string.
	 */
	private String generateInputDescription(String engineId, Map<String, Object> domContext, PlaywrightSession session,
			String tabId) {
		// Get page title
		Page page = tabId != null ? session.tabPages.get(tabId) : session.getPage();
		String pageTitle = page != null ? page.title() : "";

		String prompt = generatePrompt(domContext, pageTitle);
		classLogger.info(prompt);

		IModelEngine modelEngine = Utility.getModel(engineId);
		Room room = RoomUtils.createRoomIfNotExists(UUID.randomUUID().toString(), insight, modelEngine, null);
		InputMessage inputMessage = InputMessage.builder(room).withText(prompt).build();
		ResponseMessage response = room.ask(inputMessage, modelEngine);
		return response.getContent();
	}

	/**
	 * Generates a prompt string for the model engine based on the DOM context and
	 * page title.
	 *
	 * @param domContext The extracted DOM context of the input field.
	 * @param pageTitle  The title of the page containing the input field.
	 * @return The generated prompt string.
	 */
	private String generatePrompt(Map<String, Object> domContext, String pageTitle) {
		StringBuilder prompt = new StringBuilder();
		prompt.append(
				"Generate a user-friendly description for an input field based on the provided context. Describe the input field and the context of the page it's in but not in a command form and not with too technical details. ");
		prompt.append("Return only the concise description, no additional text.\n\n");

		// Extract basic info
		String labelText = (String) domContext.get("labelText");
		Map<String, Object> input = (Map<String, Object>) domContext.get("input");
		Map<String, Object> attributes = input != null ? (Map<String, Object>) input.get("attributes") : null;

		if (pageTitle != null && !pageTitle.isEmpty()) {
			append(prompt, "Page Title", pageTitle);
		}

		if (labelText != null && !labelText.isEmpty()) {
			append(prompt, "Label", labelText);
		}

		if (attributes != null) {
			append(prompt, "Placeholder", (String) attributes.get("placeholder"));
			append(prompt, "Type", (String) attributes.get("type"));
			append(prompt, "Name", (String) attributes.get("name"));
			append(prompt, "Autocomplete", (String) attributes.get("autocomplete"));
			append(prompt, "Pattern", (String) attributes.get("pattern"));

			Boolean required = (Boolean) attributes.get("required");
			if (required != null && required) {
				prompt.append("Required: true. ");
			}
		}

		// Add contextual information
		append(prompt, "Section Heading", (String) domContext.get("headingText"));
		append(prompt, "Group Legend", (String) domContext.get("fieldsetLegend"));

		// Add nearby text
		List<String> nearbyText = (List<String>) domContext.get("nearbyText");
		if (nearbyText != null && !nearbyText.isEmpty()) {
			append(prompt, "Nearby Text", String.join(" | ", nearbyText));
		}

		// Add form context if available
		Map<String, Object> form = (Map<String, Object>) domContext.get("form");
		if (form != null) {
			String formName = (String) form.get("name");
			if (formName != null && !formName.isEmpty()) {
				append(prompt, "Form Name", formName);
			}
		}

		// Add HTML snippets for rich context
		if (input != null) {
			String outerHTML = (String) input.get("outerHTML");
			if (outerHTML != null && !outerHTML.isEmpty()) {
				append(prompt, "Input HTML", truncate(outerHTML, 1000));
			}
		}

		// Add parent context (up to 3 levels)
		List<Map<String, Object>> ancestry = (List<Map<String, Object>>) domContext.get("ancestry");
		if (ancestry != null && !ancestry.isEmpty()) {
			int parentsToInclude = Math.min(3, ancestry.size());
			for (int i = 0; i < parentsToInclude; i++) {
				Map<String, Object> parent = ancestry.get(i);
				String parentHTML = (String) parent.get("outerHTML");
				if (parentHTML != null && !parentHTML.isEmpty()) {
					String label = "Parent " + (i + 1) + " Context";
					append(prompt, label, truncate(parentHTML, 800));
				}
			}
		}

		return prompt.toString().trim();
	}

	/**
	 * Appends a label and value to the prompt string if the value is not null or
	 * empty.
	 *
	 * @param sb    The StringBuilder to append to.
	 * @param label The label for the value.
	 * @param value The value to append.
	 */
	private void append(StringBuilder sb, String label, String value) {
		if (value != null && !value.isEmpty()) {
			sb.append(label).append(": ").append(value).append(". ");
		}
	}

	/**
	 * Truncates a string to the specified maximum length.
	 *
	 * @param s   The string to truncate.
	 * @param max The maximum length of the string.
	 * @return The truncated string.
	 */
	private String truncate(String s, int max) {
		return (s == null || s.length() <= max) ? s : s.substring(0, max) + "...";
	}

	@Override
	public String getReactorDescription() {
		return "Generates a user-friendly description for an input field based on its DOM context using playwright";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The ID of the model engine to use for generating the description";
		} else if (key.equals("sessionId")) {
			return "The Playwright session ID containing the target input field";
		} else if (key.equals("selector")) {
			return "The CSS selector for the target input field element";
		} else if (key.equals("tabId")) {
			return "The ID of the tab containing the target input field";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}
