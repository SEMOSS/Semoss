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
 * 	MERCHANTIBILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.remoteviewer.service;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.microsoft.playwright.Page;

import prerna.reactor.playwright.Selector;
import prerna.remoteviewer.model.RemoteBrowserInputEvent;

/**
 * Enriches socket/browser input events with a selector that is safe to replay.
 *
 * <p>
 * The remote browser frontend renders a screenshot canvas, so it usually only
 * knows coordinates. This service probes the real Playwright page before the
 * action is recorded and stores a selector only when it can prove the selector
 * points to exactly one element. If no unique selector is available, the event is
 * left coordinate-based.
 */
public final class RemoteBrowserSelectorService {

	private static final Logger classLogger = LogManager.getLogger(RemoteBrowserSelectorService.class);

	private static final String JS_PROBE_TARGET = """
			(args) => {
			  const mode = args && args.mode;
			  const x = args && args.x;
			  const y = args && args.y;

			  function escCss(str) {
			    if (window.CSS && CSS.escape) return CSS.escape(str);
			    return String(str).replace(/([ !"#$%&'()*+,.\\/:;<=>?@\\[\\\\\\]^`{|}~])/g, '\\\\$1');
			  }

			  function cssPath(e) {
			    if (!e) return "";
			    const tag = e.tagName.toLowerCase();
			    let p = e.parentElement;
			    if (!p) {
			      const root = e.getRootNode && e.getRootNode();
			      if (root && root.host) {
			        const sib = Array.from(root.children || []).filter(c => c.tagName === e.tagName);
			        const idx = sib.indexOf(e) + 1;
			        return cssPath(root.host) + ">" + tag + ":nth-of-type(" + idx + ")";
			      }
			      return tag;
			    }
			    const sib = Array.from(p.children).filter(c => c.tagName === e.tagName);
			    const idx = sib.indexOf(e) + 1;
			    return cssPath(p) + ">" + tag + ":nth-of-type(" + idx + ")";
			  }

			  function uniqueCss(doc, selector) {
			    if (!selector) return false;
			    try {
			      return doc.querySelectorAll(selector).length === 1;
			    } catch (e) {
			      return false;
			    }
			  }

			  function bestSelector(doc, el) {
			    if (!el || !el.tagName) return null;
			    const css = cssPath(el);
			    if (uniqueCss(doc, css)) {
			      return { strategy: "css", value: css };
			    }
			    return null;
			  }

			  function labelText(el) {
			    if (!el) return "";
			    const aria = el.getAttribute("aria-label");
			    if (aria) return aria;
			    if (el.labels && el.labels.length) return (el.labels[0].innerText || "").trim();
			    const lab = el.closest && el.closest("label");
			    if (lab) return (lab.innerText || "").trim();
			    const placeholder = el.getAttribute("placeholder");
			    if (placeholder) return placeholder.trim();
			    const title = el.getAttribute("title");
			    if (title) return title.trim();
			    const alt = el.getAttribute("alt");
			    if (alt) return alt.trim();
			    // For text controls, visible text can be the value the user just entered.
			    // Never copy that value into the semantic label used by metadata inference.
			    if (isTextControl(el)) return "";
			    const visibleText = (el.innerText || el.textContent || "").replace(/\s+/g, " ").trim();
			    return visibleText.slice(0, 180);
			  }

			  function isTextControl(el) {
			    if (!el) return false;
			    const tag = el.tagName.toLowerCase();
			    const type = (el.type || "").toLowerCase();
			    return tag === "textarea" || el.isContentEditable === true ||
			      (tag === "input" && ["text", "password", "email", "search", "tel", "url", "number"].includes(type));
			  }

			  function deepestFromPoint(win, px, py, framesSoFar) {
			    const doc = win.document;
			    let el = doc.elementFromPoint(px, py);
			    if (!el) return { element: null, doc, frames: framesSoFar || [] };

			    while (el.shadowRoot) {
			      const deeper = el.shadowRoot.elementFromPoint(px, py);
			      if (!deeper || deeper === el) break;
			      el = deeper;
			    }

			    if (el.tagName === "IFRAME") {
			      try {
			        const rect = el.getBoundingClientRect();
			        const childWin = el.contentWindow;
			        if (childWin && childWin.document) {
			          const result = deepestFromPoint(childWin, px - rect.left, py - rect.top, (framesSoFar || []).concat(el));
			          if (result.element) return result;
			        }
			      } catch (e) {}
			    }
			    return { element: el, doc, frames: framesSoFar || [] };
			  }

			  function activeElement(win, framesSoFar) {
			    const doc = win.document;
			    let el = doc.activeElement;
			    if (!el) return { element: null, doc, frames: framesSoFar || [] };
			    if (el.shadowRoot && el.shadowRoot.activeElement) {
			      el = el.shadowRoot.activeElement;
			    }
			    if (el.tagName === "IFRAME") {
			      try {
			        const childWin = el.contentWindow;
			        if (childWin && childWin.document) {
			          const result = activeElement(childWin, (framesSoFar || []).concat(el));
			          if (result.element) return result;
			        }
			      } catch (e) {}
			    }
			    return { element: el, doc, frames: framesSoFar || [] };
			  }

			  const target = mode === "active"
			    ? activeElement(window, [])
			    : deepestFromPoint(window, x, y, []);
			  const el = target.element;
			  if (!el) return null;

			  const selector = bestSelector(target.doc, el);

			  const frames = target.frames || [];
			  if (selector && frames.length) {
			    const frameSelector = cssPath(frames[frames.length - 1]);
			    if (uniqueCss(document, frameSelector)) {
			      selector.frameSelector = frameSelector;
			    }
			  }

			  const tag = el.tagName.toLowerCase();
			  const inputType = (el.type || "").toLowerCase();
			  const rect = el.getBoundingClientRect();
			  return {
			    selector,
			    x: Math.round(rect.left + rect.width / 2),
			    y: Math.round(rect.top + rect.height / 2),
			    tag,
			    label: labelText(el),
			    isPassword: tag === "input" && inputType === "password",
			    storeValue: isTextControl(el) && !(tag === "input" && inputType === "password")
			  };
			}
			""";

	private static final String JS_IS_UNIQUE_SELECTOR = """
			(sel) => {
			  function escCss(str) {
			    if (window.CSS && CSS.escape) return CSS.escape(str);
			    return String(str).replace(/([ !"#$%&'()*+,.\\/:;<=>?@\\[\\\\\\]^`{|}~])/g, '\\\\$1');
			  }
			  function docForSelector(selector) {
			    if (!selector || !selector.frameSelector) return document;
			    try {
			      const frame = document.querySelector(selector.frameSelector);
			      return frame && frame.contentDocument ? frame.contentDocument : null;
			    } catch (e) {
			      return null;
			    }
			  }
			  const doc = docForSelector(sel);
			  if (!doc || !sel || !sel.value) return false;
			  try {
			    if (sel.strategy === "id") {
			      return doc.querySelectorAll("#" + escCss(sel.value)).length === 1;
			    }
			    if (sel.strategy === "css") {
			      return doc.querySelectorAll(sel.value).length === 1;
			    }
			    if (sel.strategy === "xpath") {
			      const result = doc.evaluate(sel.value, doc, null, XPathResult.ORDERED_NODE_SNAPSHOT_TYPE, null);
			      return result.snapshotLength === 1;
			    }
			  } catch (e) {
			    return false;
			  }
			  return false;
			}
			""";

	private static final String JS_FOCUSED_VALUE_IF_MATCHES = """
			(sel) => {
			  function escCss(str) {
			    if (window.CSS && CSS.escape) return CSS.escape(str);
			    return String(str).replace(/([ !"#$%&'()*+,.\\/:;<=>?@\\[\\\\\\]^`{|}~])/g, '\\\\$1');
			  }
			  function resolve(doc, selector) {
			    if (!doc || !selector || !selector.value) return null;
			    try {
			      if (selector.strategy === "id") return doc.querySelector("#" + escCss(selector.value));
			      if (selector.strategy === "css") return doc.querySelector(selector.value);
			      if (selector.strategy === "xpath") {
			        return doc.evaluate(selector.value, doc, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue;
			      }
			    } catch (e) {
			      return null;
			    }
			    return null;
			  }
			  function frameDocument(selector) {
			    if (!selector || !selector.frameSelector) return document;
			    try {
			      const frame = document.querySelector(selector.frameSelector);
			      return frame && frame.contentDocument ? frame.contentDocument : null;
			    } catch (e) {
			      return null;
			    }
			  }
			  const doc = frameDocument(sel);
			  const target = resolve(doc, sel);
			  if (!doc || !target) return null;
			  let active = doc.activeElement;
			  if (active && active.shadowRoot && active.shadowRoot.activeElement) {
			    active = active.shadowRoot.activeElement;
			  }
			  if (!(active === target || (target.contains && target.contains(active)))) {
			    return null;
			  }
			  const tag = target.tagName ? target.tagName.toLowerCase() : "";
			  if (tag === "input" || tag === "textarea" || tag === "select") {
			    return target.value || "";
			  }
			  if (target.isContentEditable) {
			    return target.innerText || target.textContent || "";
			  }
			  return null;
			}
			""";

	private RemoteBrowserSelectorService() {
	}

	public static void enrich(RemoteBrowserSession session, RemoteBrowserInputEvent event) {
		if (session == null || event == null || event.getType() == null) {
			return;
		}

		Page page = session.getActivePage();
		if (page == null || page.isClosed()) {
			return;
		}

		if (event.getSelector() != null) {
			if (isUniqueSelector(page, event.getSelector())) {
				return;
			}
			event.setSelector(null);
		}

		String type = event.getType();
		if ("mouse-click".equals(type) || "wheel".equals(type)) {
			probeAtCoordinates(page, event);
		} else if ("type-text".equals(type) || "key".equals(type)) {
			probeActiveElement(page, event);
		}
	}

	private static void probeAtCoordinates(Page page, RemoteBrowserInputEvent event) {
		if (event.getX() == null || event.getY() == null) {
			return;
		}
		enrichFromProbe(page, event, Map.of("mode", "point", "x", event.getX(), "y", event.getY()));
	}

	private static void probeActiveElement(Page page, RemoteBrowserInputEvent event) {
		enrichFromProbe(page, event, Map.of("mode", "active"));
	}

	@SuppressWarnings("unchecked")
	private static void enrichFromProbe(Page page, RemoteBrowserInputEvent event, Map<String, Object> payload) {
		try {
			Map<String, Object> data = (Map<String, Object>) page.evaluate(JS_PROBE_TARGET, payload);
			if (data == null) {
				return;
			}

			Map<String, Object> selectorMap = (Map<String, Object>) data.get("selector");
			if (selectorMap != null) {
				String strategy = stringValue(selectorMap.get("strategy"));
				String value = stringValue(selectorMap.get("value"));
				String frameSelector = stringValue(selectorMap.get("frameSelector"));
				if ("css".equals(strategy) && value != null && !value.isBlank()) {
					Selector selector = new Selector(strategy, value, frameSelector);
					if (isUniqueSelector(page, selector)) {
						event.setSelector(selector);
					}
				}
			}
			if (event.getX() == null && data.get("x") instanceof Number) {
				event.setX(((Number) data.get("x")).doubleValue());
			}
			if (event.getY() == null && data.get("y") instanceof Number) {
				event.setY(((Number) data.get("y")).doubleValue());
			}
			if (event.getTag() == null) {
				event.setTag(stringValue(data.get("tag")));
			}
			if (event.getLabel() == null) {
				event.setLabel(stringValue(data.get("label")));
			}
			if (event.getIsPassword() == null && data.get("isPassword") instanceof Boolean) {
				event.setIsPassword((Boolean) data.get("isPassword"));
			}
			if (event.getStoreValue() == null && data.get("storeValue") instanceof Boolean) {
				event.setStoreValue((Boolean) data.get("storeValue"));
			}
		} catch (Exception e) {
			classLogger.debug("Could not enrich remote browser event selector: {}", e.getMessage());
		}
	}

	private static boolean isUniqueSelector(Page page, Selector selector) {
		if (selector == null || selector.value() == null || selector.value().isBlank()) {
			return false;
		}
		try {
			return Boolean.TRUE.equals(page.evaluate(JS_IS_UNIQUE_SELECTOR,
					Map.of("strategy", nullToEmpty(selector.strategy()), "value", selector.value(),
							"frameSelector", nullToEmpty(selector.frameSelector()))));
		} catch (Exception e) {
			classLogger.debug("Remote browser selector is not usable: {}", e.getMessage());
			return false;
		}
	}

	public static String focusedValueIfMatches(Page page, Selector selector) {
		if (page == null || page.isClosed() || selector == null || selector.value() == null
				|| selector.value().isBlank()) {
			return null;
		}
		try {
			Object value = page.evaluate(JS_FOCUSED_VALUE_IF_MATCHES,
					Map.of("strategy", nullToEmpty(selector.strategy()), "value", selector.value(),
							"frameSelector", nullToEmpty(selector.frameSelector())));
			return value instanceof String ? (String) value : null;
		} catch (Exception e) {
			classLogger.debug("Could not read focused remote browser value: {}", e.getMessage());
			return null;
		}
	}

	private static String stringValue(Object value) {
		if (value == null) {
			return null;
		}
		String s = String.valueOf(value);
		return s.isBlank() ? null : s;
	}

	private static String nullToEmpty(String value) {
		return value == null ? "" : value;
	}
}
