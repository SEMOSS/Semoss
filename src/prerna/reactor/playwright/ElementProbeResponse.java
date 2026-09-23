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

import java.util.Map;

/**
 * Represents a comprehensive response containing detailed information about a
 * probed HTML element. This record is designed to provide a snapshot of an
 * element's properties for analysis or interaction.
 * 
 * @param tag              The HTML tag name of the element (e.g., "input",
 *                         "textarea", "button", "a").
 * @param type             The type attribute of the element (e.g., "text",
 *                         "password", "submit" for input elements).
 * @param inputCategory    A categorization of the input type (e.g., "text",
 *                         "option", "boolean", "action", "link").
 * @param role             The ARIA role of the element, if present.
 * @param selector         A simple CSS selector synthesized for the element.
 * @param placeholder      The placeholder text of the element.
 * @param labelText        The associated label text, derived from
 *                         {@code <label for=>}, {@code aria-label}, or nearest
 *                         text.
 * @param value            The current value of the element (for
 *                         inputs/textareas).
 * @param href             The href attribute value, if the element is a link.
 * @param contentEditable  True if the element is content editable, false
 *                         otherwise.
 * @param rect             The viewport-relative bounding rectangle of the
 *                         element in CSS pixels.
 * @param metrics          Layout metrics of the element that ignore transforms
 *                         (integers).
 * @param styles           Resolved computed styles for visual cloning.
 * @param placeholderStyle Computed style of the {@code ::placeholder}
 *                         pseudo-element, if supported.
 * @param attrs            HTML attributes relevant to behavior and
 *                         accessibility.
 * @param isTextControl    Convenience flag indicating if the element is a text
 *                         input control.
 */
public record ElementProbeResponse(String tag, String type, String inputCategory, String role, String selector,
		String placeholder, String labelText, String value, String href, boolean contentEditable, ElementRect rect,
		ElementMetrics metrics, Map<String, String> styles, Map<String, String> placeholderStyle,
		Map<String, String> attrs, boolean isTextControl) {
}
