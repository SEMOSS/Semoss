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

import java.util.HashMap;
import java.util.Map;

import com.microsoft.playwright.Page;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Returns elements data in the selected area to be used by LLM for generating
 * playwright steps
 */
public class ExtractElementsDataForLLMReactor extends AbstractReactor {

	private static final String JS_EXTRACT_HTML = """
			([startX, startY, endX, endY]) => {
			    function getCssPath(el) {
			        if (!el) return "";
			        const tag = el.tagName.toLowerCase();

			        // Only use ID if it doesn't contain special characters that would need escaping
			        if (el.id && /^[a-zA-Z0-9_-]+$/.test(el.id)) {
			            return tag + "#" + el.id;
			        }

			        let p = el.parentElement;

			        // Cross shadow DOM boundary: parentElement is null for direct children of a shadow root
			        if (!p) {
			            const root = el.getRootNode();
			            if (root && root.host) {
			                const sib = Array.from(root.children).filter(c => c.tagName === el.tagName);
			                const idx = sib.indexOf(el) + 1;
			                return getCssPath(root.host) + ">" + tag + ":nth-of-type(" + idx + ")";
			            }
			            return tag;
			        }

			        const sib = Array.from(p.children).filter(c => c.tagName === e.tagName);
                    const idx = sib.indexOf(e) + 1;
			        return getCssPath(p) + ">" + tag + ":nth-of-type(" + idx + ")";
			    }

			    function isInteractive(el) {
			        const tag = el.tagName.toLowerCase();

			        // Primary interactive elements
			        if (['input', 'button', 'select', 'textarea', 'a'].includes(tag)) {
			            return true;
			        }

			        // Check role
			        const role = el.getAttribute('role');
			        if (role && ['button', 'link', 'textbox', 'checkbox', 'radio'].includes(role)) {
			            return true;
			        }

			        // Check for click handlers
			        if (el.onclick || el.getAttribute('onclick')) return true;

			        // Check class names for buttons
			        const className = (el.className.baseVal || el.className || '').toString().toLowerCase();
			        if (className.includes('btn') || className.includes('button')) {
			            return true;
			        }

			        // Check cursor style
			        const cs = getComputedStyle(el);
			        if (cs.cursor === 'pointer') return true;

			        return false;
			    }

			    function getElementPurpose(el, attrs) {
			        const tag = el.tagName.toLowerCase();
			        const type = attrs.type || '';
			        const name = (attrs.name || '').toLowerCase();
			        const placeholder = (attrs.placeholder || '').toLowerCase();
			        const ariaLabel = (attrs['aria-label'] || '').toLowerCase();

			        // Detect input types
			        if (tag === 'input') {
			            if (type === 'password') return 'password-field';
			            if (type === 'email' || name.includes('email') || placeholder.includes('email')) {
			                return 'email-field';
			            }
			            if (name.includes('user') || placeholder.includes('user') ||
			                ariaLabel.includes('user')) {
			                return 'username-field';
			            }
			            if (type === 'submit') return 'submit-button';

			            return 'input-' + ariaLabel + '-field';
			            return 'input-field';
			        }

			        if (tag === 'button' || attrs.role === 'button') {
			            const text = el.innerText.toLowerCase();
			            if (text.includes('sign in') || text.includes('login') ||
			                text.includes('submit') || type === 'submit') {
			                return 'submit-button';
			            }
			            return 'button';
			        }

			        if (tag === 'a') return 'link';

			        return 'interactive';
			    }

			    function isElementVisible(el, rect) {
			        if (rect.width === 0 || rect.height === 0) return false;

			        const cs = getComputedStyle(el);
			        if (cs.display === 'none' || cs.visibility === 'hidden' ||
			            cs.opacity === '0') return false;

			        // Check if element is off-screen (like hidden password fields)
			        if (rect.x < -1000 || rect.y < -1000) return false;

			        return true;
			    }

			    function isInBounds(rect) {
			        const centerX = rect.left + rect.width / 2;
			        const centerY = rect.top + rect.height / 2;
			        return centerX >= startX && centerX <= endX &&
			               centerY >= startY && centerY <= endY;
			    }

			    function getNearbyLabels(el, rect) {
			        const labels = [];
			        const radius = 100; // pixels

			        // Check all text nodes near the element
			        const walker = document.createTreeWalker(
			            document.body,
			            NodeFilter.SHOW_TEXT,
			            null
			        );

			        let node;
			        while (node = walker.nextNode()) {
			            const parent = node.parentElement;
			            if (!parent || !node.nodeValue.trim()) continue;

			            const parentRect = parent.getBoundingClientRect();
			            const distance = Math.sqrt(
			                Math.pow(parentRect.x - rect.x, 2) +
			                Math.pow(parentRect.y - rect.y, 2)
			            );

			            if (distance < radius && node.nodeValue.trim().length > 2) {
			                labels.push({
			                    text: node.nodeValue.trim(),
			                    distance: Math.round(distance),
			                    position: parentRect.y < rect.y ? 'above' :
			                             parentRect.y > rect.y + rect.height ? 'below' :
			                             parentRect.x < rect.x ? 'left' : 'right'
			                });
			            }
			        }

			        return labels.slice(0, 3); // Top 3 closest
			    }

			    function getSectionHeader(el) {
			        let current = el;

			        // Walk up and back in DOM to find nearest heading
			        while (current) {
			            const prev = current.previousElementSibling;
			            if (prev) {
			                const heading = prev.querySelector('h1, h2, h3, h4, h5, h6');
			                if (heading) return heading.innerText.trim();

			                if (/^h[1-6]$/i.test(prev.tagName)) {
			                    return prev.innerText.trim();
			                }
			            }
			            current = current.parentElement;
			        }

			        return null;
			    }

			    function getTableContext(el) {
			        const cell = el.closest('td, th');
			        if (!cell) return null;

			        const row = cell.closest('tr');
			        const table = cell.closest('table');

			        if (!table) return null;

			        // Get column header
			        const cellIndex = Array.from(row.children).indexOf(cell);
			        const thead = table.querySelector('thead');
			        let columnHeader = null;

			        if (thead) {
			            const headerRow = thead.querySelector('tr');
			            if (headerRow && headerRow.children[cellIndex]) {
			                columnHeader = headerRow.children[cellIndex].innerText.trim();
			            }
			        }

			        return {
			            columnHeader: columnHeader,
			            rowIndex: Array.from(table.querySelectorAll('tr')).indexOf(row),
			            columnIndex: cellIndex
			        };
			    }

			    // Recursively collect all elements from a root (document, shadow root, or iframe document)
			    function collectAllElements(root, framesSoFar = []) {
			        const elements = [];

			        try {
			            const allEls = root.querySelectorAll('*');
			            for (const el of allEls) {
			                elements.push({ element: el, frames: framesSoFar });

			                // Drill into shadow DOM
			                if (el.shadowRoot) {
			                    const shadowElements = collectAllElements(el.shadowRoot, framesSoFar);
			                    elements.push(...shadowElements);
			                }

			                // Drill into iframes
			                if (el.tagName === 'IFRAME') {
			                    try {
			                        const childWin = el.contentWindow;
			                        if (childWin && childWin.document) {
			                            const childElements = collectAllElements(childWin.document, framesSoFar.concat(el));
			                            elements.push(...childElements);
			                        }
			                    } catch (e) {
			                        // Cross-origin iframe, skip
			                    }
			                }
			            }
			        } catch (e) {
			            // Error accessing root
			        }

			        return elements;
			    }

			    // Find only interactive elements in bounds
			    const allElementsWithFrames = collectAllElements(document);
			    let interactive = [];

			    for (const item of allElementsWithFrames) {
			        const actualElement = item.element;
			        if (!actualElement) continue;

			        let frames = item.frames || [];
			                 let insideFrame = frames.length > 0;
			                   // Selector of the innermost iframe that directly contains the element (if any)
			                   let frameSelector = "";
			                   if (insideFrame) {
			                     let lastFrame = frames[frames.length - 1];
			                     frameSelector = getCssPath(lastFrame);
			                   }

			                 // Get the element's rect
			                 let rect = actualElement.getBoundingClientRect();

			                 // If element is inside iframe, adjust rect to viewport coordinates
			                 if (insideFrame) {
			                     for (const frame of frames) {
			                         const frameRect = frame.getBoundingClientRect();
			                         rect = {
			                             left: rect.left + frameRect.left,
			                             top: rect.top + frameRect.top,
			                             right: rect.right + frameRect.left,
			                             bottom: rect.bottom + frameRect.top,
			                             width: rect.width,
			                             height: rect.height,
			                             x: rect.x + frameRect.x,
			                             y: rect.y + frameRect.y
			                         };
			                     }
			                 }

			        if (!isInBounds(rect)) continue;

			        if (!isInteractive(actualElement)) continue;
			        if (!isElementVisible(actualElement, rect)) continue;

			        const tag = actualElement.tagName.toLowerCase();

			        // Get key attributes
			        const attrs = {};
			        ['id', 'name', 'class', 'type', 'placeholder', 'value',
			         'aria-label', 'role', 'href'].forEach(attr => {
			            const val = actualElement.getAttribute(attr);
			            if (val) attrs[attr] = val;
			        });

			        const nearbyLabels = getNearbyLabels(actualElement, rect);
			        const sectionHeader = getSectionHeader(actualElement);
			        const tableContext = getTableContext(actualElement);

			        const purpose = getElementPurpose(actualElement, attrs);
			        const text = (actualElement.innerText || actualElement.textContent || '').trim().slice(0, 100);

			        interactive.push({
			            tag: tag,
			            purpose: purpose,
			            text: text,
			            selector: getCssPath(actualElement),
			            frameSelector: frameSelector,
			            coords: {
			                x: Math.round(rect.x + rect.width / 2),
			                y: Math.round(rect.y + rect.height / 2)
			            },
			            rect: {
			                x: Math.round(rect.x),
			                y: Math.round(rect.y),
			                width: Math.round(rect.width),
			                height: Math.round(rect.height)
			            },
			            attributes: attrs,
			            nearbyLabels: nearbyLabels,
			            sectionHeader: sectionHeader,
			            tableContext: tableContext,
			            interactive: true,
			            visible: true
			        });
			    }

			    // Sort by position (top to bottom, left to right)
			    interactive.sort((a, b) => {
			        const dy = a.rect.y - b.rect.y;
			        if (Math.abs(dy) > 20) return dy;
			        return a.rect.x - b.rect.x;
			    });

			    //limit to 30 elements
			    if (interactive.length > 30) {
			        interactive.splice(30);
			    }

			    // Detect form structure
			    const hasEmail = interactive.some(e => e.purpose === 'email-field');
			    const hasUsername = interactive.some(e => e.purpose === 'username-field');
			    const hasPassword = interactive.some(e => e.purpose === 'password-field');
			    const hasSubmit = interactive.some(e => e.purpose === 'submit-button');
			    const isLoginForm = (hasEmail || hasUsername) && (hasPassword || hasSubmit);

			    return {
			        elements: interactive,
			        elementCount: interactive.length,
			        interactiveCount: interactive.length,
			        bounds: {
			            startX: startX,
			            startY: startY,
			            endX: endX,
			            endY: endY,
			            width: endX - startX,
			            height: endY - startY
			        },
			        summary: {
			            totalElements: interactive.length,
			            interactiveElements: interactive.length,
			            isLoginForm: isLoginForm,
			            hasEmail: hasEmail,
			            hasUsername: hasUsername,
			            hasPassword: hasPassword,
			            hasSubmit: hasSubmit
			        }
			    };
			}
			""";

	public ExtractElementsDataForLLMReactor() {
		this.keysToGet = new String[] { "sessionId", "tabId", ReactorKeysEnum.PARAM_VALUES_MAP.getKey() };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String sessionId = this.keyValue.get(this.keysToGet[0]);
		String tabId = this.keyValue.get(this.keysToGet[1]);
		Map<String, Object> paramValues = getMap(this.keysToGet[2]);

		PlaywrightSession playwrightSession = this.insight.getUser().getPlaywrightSession(sessionId);
		Page page = playwrightSession.tabPages.get(tabId);

		// Get coordinates from params
		int startX = ((Number) paramValues.get("startX")).intValue();
		int startY = ((Number) paramValues.get("startY")).intValue();
		int endX = ((Number) paramValues.get("endX")).intValue();
		int endY = ((Number) paramValues.get("endY")).intValue();

		// Execute JavaScript to extract HTML from the selected area
		@SuppressWarnings("unchecked")
		Map<String, Object> result = (Map<String, Object>) page.evaluate(JS_EXTRACT_HTML,
				new Object[] { startX, startY, endX, endY });

		if (result == null) {
			// Return empty result if nothing found
			Map<String, Object> emptyResponse = new HashMap<>();
			emptyResponse.put("html", "");
			emptyResponse.put("elements", new java.util.ArrayList<>());
			emptyResponse.put("elementCount", 0);
			emptyResponse.put("interactiveCount", 0);

			Map<String, Object> bounds = new HashMap<>();
			bounds.put("startX", startX);
			bounds.put("startY", startY);
			bounds.put("endX", endX);
			bounds.put("endY", endY);
			emptyResponse.put("bounds", bounds);

			return new NounMetadata(emptyResponse, PixelDataType.MAP);
		}

		return new NounMetadata(result, PixelDataType.MAP);
	}

	@Override
	public String getReactorDescription() {
		return "Extracts interactive HTML elements data from a specified area of the webpage for LLM processing.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("sessionId")) {
			return "The id of the session of the current playwright";
		} else if (key.equals("tabId")) {
			return "The id of the current tab in the session";
		}
		return super.getDescriptionForKey(key);
	}
}