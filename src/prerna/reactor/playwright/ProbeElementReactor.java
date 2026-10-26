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

import com.google.gson.reflect.TypeToken;
import com.microsoft.playwright.Page;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ProbeElementReactor extends AbstractReactor {

	private static final String JS_PROBE = """
			 ([x,y]) => {
			     // Helper: shallow copy of selected style properties
			     const pick = (src, names) => {
			       const out = {};
			       for (const n of names) out[n] = src[n];
			       return out;
			     };

			     function escCss(str) {
			       if (window.CSS && CSS.escape) {
			         return CSS.escape(str);
			       }
			       // Fallback escape: prefix any special character with a backslash
			       return str.replace(/([ !"#$%&'()*+,.\\/:;<=>?@\\[\\\\\\]^`{|}~])/g, '\\\\$1');
			     }

			     // Helper: build a CSS path from element to root
			       function cssPath(e){
			         if (!e) return "";
			         const tag = e.tagName.toLowerCase();

			         // Only use ID if it doesn't contain special characters that would need escaping
			         // Escaped selectors with backslashes cause parsing issues when passed through Java
			         if (e.id && /^[a-zA-Z0-9_-]+$/.test(e.id)) {
			           return tag + "#" + e.id;
			         }

			         // Otherwise, build a path with :nth-of-type
			         let p = e.parentElement;

			         // Cross shadow DOM boundary: parentElement is null for direct children of a shadow root
			         if (!p) {
			           const root = e.getRootNode();
			           if (root && root.host) {
			             const sib = Array.from(root.children).filter(c => c.tagName === e.tagName);
			             const idx = sib.indexOf(e) + 1;
			             return cssPath(root.host) + ">" + tag + ":nth-of-type(" + idx + ")";
			           }
			           return tag;
			         }

			         const sib = Array.from(p.children).filter(c => c.tagName === e.tagName);
			                  const idx = sib.indexOf(e) + 1;
			         return cssPath(p) + ">" + tag + ":nth-of-type(" + idx + ")";
			       }

			     /**
			      * Recursively find the deepest element at (x,y),
			      * drilling into same-origin iframes when possible.
			      * Returns { element, frames }
			      *  - element: the final HTMLElement
			      *  - frames: array of iframe elements we passed through (outer?inner)
			      */
			     function deepestElementFromPoint(win, x, y, framesSoFar = []) {
			       const doc = win.document;
			       let el = doc.elementFromPoint(x, y);
			       if (!el) {
			         return { element: null, frames: framesSoFar };
			       }

			       // Drill through shadow DOM boundaries (coordinates stay viewport-relative)
			       while (el.shadowRoot) {
			         const deeper = el.shadowRoot.elementFromPoint(x, y);
			         if (!deeper || deeper === el) break;
			         el = deeper;
			       }

			       if (el.tagName === "IFRAME") {
			         try {
			           const rect = el.getBoundingClientRect();
			           const innerX = x - rect.left;
			           const innerY = y - rect.top;
			           const childWin = el.contentWindow;
			           if (childWin && childWin.document) {
			             // Recurse inside the iframe
			             const result = deepestElementFromPoint(childWin, innerX, innerY, framesSoFar.concat(el));
			             if (result.element) {
			               return result;
			             }
			           }
			         } catch (e) {
			           // Cross-origin iframe or access denied - fall back to iframe element itself
			         }
			       }

			       return { element: el, frames: framesSoFar };
			     }

			     const info = deepestElementFromPoint(window, x, y, []);
			     const el = info.element;
			     if (!el) return null;

			     const frames = info.frames || [];
			     const insideFrame = frames.length > 0;

			     // Selector of the innermost iframe that directly contains the element (if any)
			     let frameSelector = "";
			     if (insideFrame) {
			       const lastFrame = frames[frames.length - 1];
			       frameSelector = cssPath(lastFrame);
			     }

			     // Get element's bounding rect (iframe-relative if inside iframe)
			     const r  = el.getBoundingClientRect();

			     // Calculate viewport-relative coordinates for UI positioning
			     let viewportX = r.x;
			     let viewportY = r.y;
			     if (insideFrame) {
			       // Walk through the iframe chain and accumulate offsets
			       for (const iframe of frames) {
			         const iframeRect = iframe.getBoundingClientRect();
			         viewportX += iframeRect.x;
			         viewportY += iframeRect.y;
			       }
			     }

			     const cs = getComputedStyle(el);

			const styleProps = [
			  "boxSizing","display","visibility","opacity",
			  "width","height","minWidth","minHeight","maxWidth","maxHeight",
			  "marginTop","marginRight","marginBottom","marginLeft",
			  "paddingTop","paddingRight","paddingBottom","paddingLeft",
			  "borderTopWidth","borderRightWidth","borderBottomWidth","borderLeftWidth",
			  "borderTopStyle","borderRightStyle","borderBottomStyle","borderLeftStyle",
			  "borderTopColor","borderRightColor","borderBottomColor","borderLeftColor",
			  "borderTopLeftRadius","borderTopRightRadius","borderBottomRightRadius","borderBottomLeftRadius",
			  "outlineWidth","outlineStyle","outlineColor","outlineOffset","boxShadow","textShadow",
			  "color","backgroundColor","backgroundImage","backgroundClip",
			  "fontFamily","fontSize","fontWeight","fontStyle","fontStretch","fontVariant",
			  "lineHeight","letterSpacing","textAlign","textTransform",
			  "textDecorationLine","textDecorationStyle","textDecorationColor",
			  "whiteSpace","wordBreak","direction","writingMode",
			  "caretColor","overflow","overflowX","overflowY"
			];
			const styles = pick(cs, styleProps);

			let placeholderStyle = null;
			try {
			  const ph = getComputedStyle(el, "::placeholder");
			  if (ph) {
			    placeholderStyle = pick(ph, [
			      "color","opacity","fontStyle","fontWeight","fontSize","fontFamily","letterSpacing"
			    ]);
			  }
			} catch (e) {}

			const metrics = {
			  offsetWidth:  el.offsetWidth,
			  offsetHeight: el.offsetHeight,
			  clientWidth:  el.clientWidth,
			  clientHeight: el.clientHeight,
			  scrollWidth:  el.scrollWidth,
			  scrollHeight: el.scrollHeight
			};

			let labelText = "";
			if (el.labels && el.labels.length) labelText = el.labels[0].innerText.trim();
			if (!labelText) labelText = el.getAttribute("aria-label") || "";
			if (!labelText) {
			  const lab = el.closest("label");
			  if (lab) labelText = lab.innerText.trim();
			}

			const attrNames = [
			  "id","name","class","placeholder",
			  "autocomplete","inputmode","pattern","maxlength","minlength","size",
			  "dir","lang","list","step","min","max","form","wrap","cols","rows",
			  "aria-label","aria-labelledby","aria-describedby"
			];
			const attrs = {};
			for (const n of attrNames) {
			  const v = el.getAttribute(n);
			  if (v !== null) attrs[n] = v;
			}

			const tag = el.tagName.toLowerCase();
			const inputType = (el.type || "").toLowerCase();
			const className = el.className || "";
			const id = el.id || "";
			const ariaRole = el.getAttribute("role") || "";

			// Heuristic detection for date/time pickers
			const dateTimePatterns = [
			  /date.*picker/i, /picker.*date/i, /datetime/i, /datepicker/i,
			  /time.*picker/i, /picker.*time/i, /calendar/i
			];
			const hasDateTimeClass = dateTimePatterns.some(p => p.test(className) || p.test(id));
			const hasDateTimeAttr = el.hasAttribute("data-datepicker") ||
			                        el.hasAttribute("data-date") ||
			                        el.hasAttribute("data-calendar");

			// Categorize input types
			const textInputTypes = ["text", "password", "email", "search", "tel", "url"];
			const dateTimeTypes = ["date", "datetime-local", "time", "month", "week"];
			const numericTypes = ["number", "range"];
			const selectionTypes = ["checkbox", "radio", "select-one", "select-multiple"];
			const fileTypes = ["file"];
			const buttonTypes = ["button", "submit", "reset"];

			let inputCategory = "other";
			let isTextControl = false;

			if (tag === "textarea") {
			  inputCategory = "text";
			  isTextControl = true;
			} else if (tag === "select") {
			  inputCategory = "selection";
			} else if (tag === "input") {
			  // Check for date/time first (including heuristic detection)
			  if (dateTimeTypes.includes(inputType) || hasDateTimeClass || hasDateTimeAttr) {
			    inputCategory = "datetime";
			  } else if (textInputTypes.includes(inputType)) {
			    inputCategory = "text";
			    isTextControl = true;
			  } else if (numericTypes.includes(inputType)) {
			    inputCategory = "numeric";
			  } else if (selectionTypes.includes(inputType)) {
			    inputCategory = "selection";
			  } else if (fileTypes.includes(inputType)) {
			    inputCategory = "file";
			  } else if (buttonTypes.includes(inputType)) {
			    inputCategory = "button";
			  }
			}

			   // Selector for the inner element (final target)
			   const innerSelector = cssPath(el);

			   return {
			     tag,
			     type: (el.type || "") + "",
			     inputCategory,
			     role: ariaRole || "",
			     selector: innerSelector,      // inner element selector (as before)
			     frameSelector,                // selector of the iframe containing it (if any)
			     insideFrame,                  // true if inside an iframe
			     placeholder: el.getAttribute("placeholder") || "",
			     labelText,
			     value: (("value" in el) ? (el.value || "") : ""),
			     href: el.getAttribute("href") || "",
			     contentEditable: el.isContentEditable === true,
			     rect: { x: viewportX, y: viewportY, width: r.width, height: r.height },  // viewport-relative for UI
			     iframeRelativeRect: insideFrame ? { x: r.x, y: r.y, width: r.width, height: r.height } : null,  // iframe-relative for backend healing
			     metrics, styles, placeholderStyle, attrs, isTextControl
			   };
			 }
			 """;

	public ProbeElementReactor() {
		this.keysToGet = new String[] { "sessionId", "coords", "tabId" };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String sessionId = this.keyValue.get(this.keysToGet[0]);
		String coordsStr = this.keyValue.get(this.keysToGet[1]);
		String tabId = this.keyValue.get(this.keysToGet[2]);

		Coords coords = parseCoords(coordsStr);
		PlaywrightSession s = this.insight.getUser().getPlaywrightSession(sessionId);

		ElementProbeResponse response = probeElementAt(s, coords, tabId);

		// test if the return format can be processed by the backend
		Map<String, Object> asMap = GSON.fromJson(GSON.toJson(response), new TypeToken<Map<String, Object>>() {
		}.getType());
		return new NounMetadata(asMap, PixelDataType.MAP);
	}

	/**
	 * 
	 * @param coordsStr
	 * @return
	 */
	private Coords parseCoords(String coordsStr) {
		// Simple comma-separated "x,y"
		String[] parts = coordsStr.split(",");
		if (parts.length != 2) {
			throw new IllegalArgumentException("Invalid coords format. Expected 'x,y' but got: " + coordsStr);
		}
		// make sure the parts are integers
		parts[0] = parts[0].split("\\.")[0];
		parts[1] = parts[1].split("\\.")[0];
		return new Coords(Integer.parseInt(parts[0].trim()), Integer.parseInt(parts[1].trim()));
	}

	@SuppressWarnings("unchecked")
	public static ElementProbeResponse probeElementAt(PlaywrightSession s, Coords coords, String tabId) {
		Page page = s.tabPages.get(tabId);

		int x = coords.x();
		int y = coords.y();

		Map<String, Object> data = (Map<String, Object>) page.evaluate(JS_PROBE, new Object[] { x, y });

		if (data == null) {
			return new ElementProbeResponse(null, null, null, null, null, null, null, null, null, false,
					new ElementRect(0, 0, 0, 0), new ElementMetrics(0, 0, 0, 0, 0, 0), null, null, null, false);
		}

		Map<String, Object> rect = (Map<String, Object>) data.get("rect");
		ElementRect er = new ElementRect(((Number) rect.get("x")).doubleValue(), ((Number) rect.get("y")).doubleValue(),
				((Number) rect.get("width")).doubleValue(), ((Number) rect.get("height")).doubleValue());

		Map<String, Object> m = (Map<String, Object>) data.get("metrics");
		ElementMetrics metrics = new ElementMetrics(getIntValue(m, "offsetWidth"), getIntValue(m, "offsetHeight"),
				getIntValue(m, "clientWidth"), getIntValue(m, "clientHeight"), getIntValue(m, "scrollWidth"),
				getIntValue(m, "scrollHeight"));

		Map<String, String> styles = (Map<String, String>) data.get("styles");
		Map<String, String> placeholderStyle = (Map<String, String>) data.get("placeholderStyle");
		Map<String, String> attrs = (Map<String, String>) data.get("attrs");
		boolean isTextControl = (Boolean) data.getOrDefault("isTextControl", false);

		String frameSelector = (String) data.get("frameSelector");
		Boolean insideFrame = (Boolean) data.get("insideFrame");
		Map<String, Object> iframeRelativeRect = (Map<String, Object>) data.get("iframeRelativeRect");

		if (attrs != null) {
			if (frameSelector != null && !frameSelector.isEmpty()) {
				attrs.put("__frameSelector", frameSelector);
			}
			if (insideFrame != null) {
				attrs.put("__insideFrame", String.valueOf(insideFrame));
			}
			if (iframeRelativeRect != null) {
				double iframeX = ((Number) iframeRelativeRect.get("x")).doubleValue();
				double iframeY = ((Number) iframeRelativeRect.get("y")).doubleValue();
				attrs.put("__iframeRelativeX", String.valueOf(iframeX));
				attrs.put("__iframeRelativeY", String.valueOf(iframeY));
			}
		}

		return new ElementProbeResponse((String) data.get("tag"), (String) data.get("type"),
				(String) data.get("inputCategory"), (String) data.get("role"), (String) data.get("selector"),
				(String) data.get("placeholder"), (String) data.get("labelText"), (String) data.get("value"),
				(String) data.get("href"), (Boolean) data.get("contentEditable"), er, metrics, styles, placeholderStyle,
				attrs, isTextControl);
	}

	/**
	 * 
	 * @param map
	 * @param key
	 * @return
	 */
	private static int getIntValue(Map<String, Object> map, String key) {
		Object value = map.get(key);
		if (value instanceof Number) {
			return ((Number) value).intValue();
		}
		return 0;
	}

	@Override
	public String getReactorDescription() {
		return "Probe the DOM element at specified coordinates in the current page of the playwright session";
	}

	@Override
	public String getDescriptionForKey(String key) {
		if (key.equals("sessionId")) {
			return "Playwright session ID that stores information about the history of actions done during that session";
		} else if (key.equals("coords")) {
			return "Coordinates (x,y) to probe the DOM element at. Format: 'x,y' (e.g., '100,200')";
		}

		return super.getDescriptionForKey(key);
	}
}