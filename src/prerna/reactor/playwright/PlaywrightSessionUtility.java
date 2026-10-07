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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.microsoft.playwright.FrameLocator;
import com.microsoft.playwright.JSHandle;
import com.microsoft.playwright.Locator;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.PlaywrightException;
import com.microsoft.playwright.options.AriaRole;
import com.microsoft.playwright.options.LoadState;

/**
 * Utility class for session-related Playwright operations
 */
public class PlaywrightSessionUtility {

	private static final Logger classLogger = LogManager.getLogger(PlaywrightSessionUtility.class);

	/**
	 * Apply a step to the session and detect page changes
	 *
	 * @param session The session to apply the step to
	 * @param step    The step to apply
	 * @param tabId
	 * @return true if page changed, false otherwise
	 */
	public static Map<String, Object> applyStep(PlaywrightSession session, PlaywrightStep step, String tabId) {
		session.getOperationLock().lock();
		try {
			Map<String, Object> response = new HashMap<String, Object>();

			Page page = session.getPage(tabId);
			long startTime = System.currentTimeMillis();
			boolean pageChanged = false;
			response.put("isNewTab", false);
			if (page == null || page.isClosed()) {
				response.put("status", "failed");
				response.put("error", "Recorded tab " + tabId + " is not bound to an open browser page");
				response.put("isPageChanged", false);
				return response;
			}
			response.put("tabTitle", page.title());
			try {
				String urlBefore = page.url();
				AtomicBoolean networkTriggered = new AtomicBoolean(false);

				JSHandle mutationPromise = createMutationObserver(page);

				page.onRequest(req -> {
					if ("xhr".equals(req.resourceType()) || "fetch".equals(req.resourceType())) {
						networkTriggered.set(true);
					}
				});

				if (step.type() == PlaywrightStepType.WAIT) {
					response.put("shouldStop", true);
				} else {
					response.put("shouldStop", false);
					executeStepAction(page, step, urlBefore, session, response);
				}

				if (step.waitAfterMs() != null && step.waitAfterMs() > 0 && (step.type() != PlaywrightStepType.WAIT)) {
					page.waitForTimeout(step.waitAfterMs());
				}

				boolean sameUrl = urlBefore.equals(page.url());
				if (sameUrl && !networkTriggered.get()) {
					waitForPageOrElement(page, step);
					pageChanged = detectPageChange(mutationPromise);
				} else {
					pageChanged = true;
				}

				long elapsed = System.currentTimeMillis() - startTime;
				classLogger.info("[STEP] {} took {} ms (pageChanged={})", step.type(), elapsed, pageChanged);
				response.put("status", "success");
				response.put("isPageChanged", pageChanged);
				return response;

			} catch (Exception e) {
				classLogger.error("Failed to apply Playwright step {}", step.type(), e);
				response.put("status", "failed");
				response.put("error", e.getMessage());
				response.put("isPageChanged", true);
				return response;
			}
		} finally {
			session.getOperationLock().unlock();
		}
	}

	/**
	 * Waits for either the page to be fully loaded or for a specific element to be
	 * present. This is a non-blocking wait with a short timeout.
	 *
	 * @param page The Playwright Page object.
	 * @param step The PlaywrightStep containing selector information.
	 */
	private static void waitForPageOrElement(Page page, PlaywrightStep step) {
		try {
			Selector selector = step.selector();
			String selectorValue = selector != null ? selector.value() : null;

			page.waitForFunction("sel => document.readyState === 'complete' || !!document.querySelector(sel)",
					selectorValue, new Page.WaitForFunctionOptions().setTimeout(800));
		} catch (PlaywrightException e) {
			classLogger.warn("Non-blocking wait timed out while checking page readiness", e);
		}
	}

	/**
	 * Executes the appropriate action based on the step type.
	 *
	 * @param page      The Playwright Page object.
	 * @param step      The PlaywrightStep to execute.
	 * @param urlBefore The URL of the page before the step is executed.
	 * @param session   The PlaywrightSession object.
	 * @param response  The response map to be populated with execution details.
	 */
	private static void executeStepAction(Page page, PlaywrightStep step, String urlBefore, PlaywrightSession session,
			Map<String, Object> response) {
		switch (step.type()) {
		case NAVIGATE -> navigateStep(page, step, response);
		case CLICK -> clickStep(page, step, urlBefore, session, response);
		case TYPE -> typeStep(page, step);
		case SCROLL -> scrollStep(page, step);
		case WAIT -> waitStep(page, step);
		case HOVER -> hoverStep(page, step);
		case CONTEXT -> {
			return;
		}
		}
	}

	/**
	 * Resolves a selector to a Playwright Locator.
	 *
	 * @param page The Playwright Page object.
	 * @param sel  The Selector object to resolve.
	 * @return The resolved Locator, or null if resolution fails.
	 */
	private static Locator resolveLocator(Page page, Selector sel) {
		classLogger.info("Resolving this locator: {}", sel);
		if (sel == null || sel.value() == null) {
			return null;
		}
		String strat = sel.strategy();
		String val = sel.value();
		String frameSelector = sel.frameSelector();

		// Check if element is inside an iframe
		if (frameSelector != null && !frameSelector.isEmpty()) {
			classLogger.info("Element is inside iframe: {} - using frameLocator approach", frameSelector);
			try {
				FrameLocator frameLocator = page.frameLocator(frameSelector);
				Locator loc = switch (strat) {
				case "id" -> frameLocator.locator("#" + cssEscapeIdent(val));
				case "testId" -> frameLocator.getByTestId(val);
				case "label" -> frameLocator.getByLabel(val);
				case "placeholder" -> frameLocator.getByPlaceholder(val);
				case "text" -> frameLocator.getByText(val);
				case "css" -> frameLocator.locator(val);
				case "xpath" -> frameLocator.locator("xpath=" + val);
				case "role" -> {
					AriaRole role;
					try {
						role = AriaRole.valueOf(val.toUpperCase());
					} catch (Exception e) {
						role = null;
					}
					yield (role != null) ? frameLocator.getByRole(role) : null;
				}
				default -> null;
				};
				return loc != null ? loc.first() : null;
			} catch (Exception e) {
				classLogger.error("Unable to resolve locator in iframe '{}'", frameSelector, e);
				return null;
			}
		}

		// Element is on main page
		try {
			Locator loc = switch (strat) {
			case "id" -> page.locator("#" + cssEscapeIdent(val));
			case "testId" -> page.getByTestId(val);
			case "label" -> page.getByLabel(val);
			case "placeholder" -> page.getByPlaceholder(val);
			case "text" -> page.getByText(val);
			case "css" -> page.locator(val);
			case "xpath" -> page.locator("xpath=" + val);
			case "role" -> {
				AriaRole role;
				try {
					role = AriaRole.valueOf(val.toUpperCase());
				} catch (Exception e) {
					role = null;
				}
				yield (role != null) ? page.getByRole(role) : null;
			}
			default -> null;
			};
			if (loc == null) {
				return null;
			}

			return loc.first();
		} catch (Exception e) {
			classLogger.error("Unable to resolve locator on main page for strategy '{}' and value '{}'", strat, val, e);
			return null;
		}
	}

	/**
	 * Escapes a string for use as a CSS identifier.
	 *
	 * @param s The string to escape.
	 * @return The escaped string.
	 */
	private static String cssEscapeIdent(String s) {
		if (s == null || s.isEmpty()) {
			return "";
		}
		StringBuilder out = new StringBuilder(s.length() * 2);
		for (int i = 0; i < s.length(); i++) {
			char ch = s.charAt(i);
			boolean first = (i == 0);
			boolean ident = (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9')
					|| ch == '-' || ch == '_';
			if ((first && Character.isDigit(ch)) || !ident) {
				out.append('\\').append(ch);
			} else {
				out.append(ch);
			}
		}
		return out.toString();
	}

	/**
	 * Attempts to type text into an element, with fallback mechanisms.
	 *
	 * @param page The Playwright Page object.
	 * @param step The PlaywrightStep containing typing information.
	 * @return true if typing was successful, false otherwise.
	 */
	private static boolean typeWithFallback(Page page, PlaywrightStep step) {
		// Try selector
		Locator loc = resolveLocator(page, step.selector());
		if ("select".equalsIgnoreCase(step.tag()) && selectWithFallback(loc, step.text())) {
			return true;
		}
		boolean typed = focusAndType(loc, step.text());

		// Try healed selector / coordinates when the recorded selector is stale.
		if (!typed && step.coords() != null) {
			Locator healed = null;
			try {
				healed = healSelector(page, step.coords().x(), step.coords().y(), step.selector());
			} catch (Exception ignore) {
			}
			typed = focusAndType(healed, step.text());
		}

		if (!typed && step.coords() != null) {
			try {
				page.mouse().click(step.coords().x(), step.coords().y());
				typed = typeFocusedTextControl(page, step.text());
			} catch (Exception ignore) {
			}
		}

		// Last resort for old recordings where TYPE has no coords: use the currently
		// focused input/textarea/contentEditable that a prior CLICK likely focused.
		if (!typed) {
			typed = typeFocusedTextControl(page, step.text());
		}

		if (typed && Boolean.TRUE.equals(step.pressEnter())) {
			try {
				page.keyboard().press("Enter");
			} catch (Exception ignore) {
			}
		}
		return typed;
	}

	private static boolean selectWithFallback(Locator locator, String value) {
		if (!isActionable(locator)) {
			return false;
		}
		try {
			locator.selectOption(value);
			return true;
		} catch (Exception valueFailure) {
			try {
				locator.selectOption(new com.microsoft.playwright.options.SelectOption().setLabel(value));
				return true;
			} catch (Exception labelFailure) {
				return false;
			}
		}
	}

	private static boolean typeFocusedTextControl(Page page, String text) {
		try {
			return Boolean.TRUE.equals(page.evaluate(
					"""
							(value) => {
							  function active(win) {
							    let el = win.document.activeElement;
							    if (!el) return null;
							    if (el.shadowRoot && el.shadowRoot.activeElement) {
							      el = el.shadowRoot.activeElement;
							    }
							    if (el.tagName === "IFRAME") {
							      try {
							        return active(el.contentWindow);
							      } catch (e) {
							        return null;
							      }
							    }
							    return el;
							  }
							  const el = active(window);
							  if (!el) return false;
							  const tag = (el.tagName || "").toLowerCase();
							  const type = (el.type || "").toLowerCase();
							  const isInput = tag === "textarea" ||
							    (tag === "input" && ["text", "password", "email", "search", "tel", "url", "number"].includes(type));
							  const nextValue = value == null ? "" : String(value);
							  if (isInput) {
							    el.focus();
							    el.value = nextValue;
							    el.dispatchEvent(new InputEvent("input", { bubbles: true, inputType: "insertText", data: nextValue }));
							    el.dispatchEvent(new Event("change", { bubbles: true }));
							    return true;
							  }
							  if (el.isContentEditable) {
							    el.focus();
							    el.textContent = nextValue;
							    el.dispatchEvent(new InputEvent("input", { bubbles: true, inputType: "insertText", data: nextValue }));
							    return true;
							  }
							  return false;
							}
							""",
					text));
		} catch (Exception e) {
			return false;
		}
	}

	/**
	 * Checks if a coordinate has a hittable element.
	 *
	 * @param page          The Playwright Page object.
	 * @param x             The x-coordinate.
	 * @param y             The y-coordinate.
	 * @param frameSelector Optional frame selector if checking inside an iframe.
	 * @return true if a hittable element exists at the coordinates, false
	 *         otherwise.
	 */
	private static boolean coordHasHit(Page page, int x, int y, String frameSelector) {
		try {
			// If checking inside an iframe, just verify it exists
			if (frameSelector != null && !frameSelector.isEmpty()) {
				String checkIframeExistsScript = "(sel) => {" + "  const iframe = document.querySelector(sel);"
						+ "  return iframe !== null;" + "}";

				Boolean iframeExists = (Boolean) page.evaluate(checkIframeExistsScript, frameSelector);
				if (iframeExists == null || !iframeExists) {
					classLogger.warn("Iframe not found for selector: {}", frameSelector);
					return false;
				}

				classLogger.info("Iframe found, assuming coordinates are hittable");
				return true;
			}

			// Main page check
			Object raw = page.evaluate(
					"""
							({x,y})=>{
							  const el = document.elementFromPoint(x,y);
							  if(!el) return null;
							  const cs=getComputedStyle(el);
							  return { tag: el.localName, display: cs.display, visibility: cs.visibility, pe: cs.pointerEvents };
							}
							""",
					java.util.Map.of("x", x, "y", y));
			if (raw == null) {
				return false;
			}
			@SuppressWarnings("unchecked")
			Map<String, Object> m = (Map<String, Object>) raw;
			String display = String.valueOf(m.get("display"));
			String visibility = String.valueOf(m.get("visibility"));
			String pe = String.valueOf(m.get("pe"));
			return !"none".equals(display) && !"hidden".equals(visibility) && !"none".equals(pe);
		} catch (Exception ignore) {
			return false;
		}
	}

	/**
	 * Attempts to hover over an element.
	 *
	 * @param page The Playwright Page object.
	 * @param step The PlaywrightStep containing hover information.
	 * @return true if hovering was successful, false otherwise.
	 */
	private static boolean hoverWithFallback(Page page, PlaywrightStep step) {
		boolean hovered = false;

		// Check if target is a canvas element, if yes use coords only
		boolean isCanvas = step.tag() != null && "canvas".equals(step.tag().toLowerCase());
		Locator loc = resolveLocator(page, step.selector());

		// 1) Try selector
		if (loc != null && !(isCanvas && step.coords() != null)) {
			try {
				loc.hover(new Locator.HoverOptions().setTimeout(300));
				hovered = true;
			} catch (Exception ignore) {
			}
		}

		// 2) Try healed selector from coords
		if (!hovered && step.coords() != null && !isCanvas) {
			Locator healed = null;
			try {
				healed = healSelector(page, step.coords().x(), step.coords().y(), step.selector());
			} catch (Exception ignore) {
			}
			if (healed != null) {
				try {
					healed.hover(new Locator.HoverOptions().setTimeout(300));
					hovered = true;
				} catch (Exception ignore) {
				}
			}
		}

		// 3) Try raw coords
		String frameSelector = (step.selector() != null) ? step.selector().frameSelector() : null;
		if (!hovered && step.coords() != null
				&& coordHasHit(page, step.coords().x(), step.coords().y(), frameSelector)) {
			try {
				page.mouse().move(step.coords().x(), step.coords().y());
				hovered = true;
			} catch (Exception ignore) {
			}
		}

		return hovered;
	}

	/**
	 * Attempts to click an element, with fallback mechanisms, and handles new tabs.
	 *
	 * @param page      The Playwright Page object.
	 * @param step      The PlaywrightStep containing click information.
	 * @param beforeUrl The URL before the click.
	 * @param session   The PlaywrightSession object.
	 * @param response  The response map to be populated with execution details.
	 * @return true if the click was successful, false otherwise.
	 */
	private static boolean clickWithFallback(Page page, PlaywrightStep step, String beforeUrl,
			PlaywrightSession session, Map<String, Object> response) {
		response.put("isNewTab", false);
		response.remove("newTabId");
		response.remove("tabTitle");

		List<Page> pagesBeforeClick = new ArrayList<>(session.CTX.pages());

		// Just perform the click
		boolean clicked = tryClick(page, step);

		if (!clicked) {
			return false;
		}

		// heck if new tab appeared
		Page newPage = waitForNewTab(session, page, pagesBeforeClick);

		if (newPage != null) {
			String preferredTabId = step.isTriggerNewTab() != null && step.isTriggerNewTab().isTrue()
					? step.isTriggerNewTab().tabId()
					: null;
			handleNewTab(session, newPage, preferredTabId, response);
		}

		return true;
	}

	/**
	 * Attempts to click an element using various strategies.
	 *
	 * @param page The Playwright Page object.
	 * @param step The PlaywrightStep containing click information.
	 * @return true if the click was successful, false otherwise.
	 */
	private static boolean tryClick(Page page, PlaywrightStep step) {
		// Check if target is a canvas element
		boolean isCanvas = step.tag() != null && "canvas".equals(step.tag().toLowerCase());
		Locator loc = resolveLocator(page, step.selector());

		// 1) Try selector
		if (loc != null && !(isCanvas && step.coords() != null)) {
			try {
				loc.click(new Locator.ClickOptions().setTimeout(300));
				return true;
			} catch (Exception e) {
				classLogger.error("Primary selector click failed for step {}", step.id(), e);
			}
		}

		// 2) Try healed selector from coords
		if (step.coords() != null && !isCanvas) {
			Locator healed = null;
			try {
				healed = healSelector(page, step.coords().x(), step.coords().y(), step.selector());
			} catch (Exception ignore) {
			}

			if (isActionable(healed)) {
				try {
					healed.click(new Locator.ClickOptions().setTimeout(300));
					return true;
				} catch (Exception e) {
					classLogger.error("Healed selector click failed for step {}", step.id(), e);
				}
			}
		}

		// 3) Try raw coords
		String frameSelector = (step.selector() != null) ? step.selector().frameSelector() : null;
		if (step.coords() != null && coordHasHit(page, step.coords().x(), step.coords().y(), frameSelector)) {
			try {
				page.mouse().click(step.coords().x(), step.coords().y());
				return true;
			} catch (Exception ignore) {
			}
		}

		return false;
	}

	/**
	 * Waits for a new tab to be created.
	 *
	 * @param session The PlaywrightSession object.
	 * @return The new Page object, or null if no new tab is created within the
	 *         timeout.
	 */
	private static Page waitForNewTab(PlaywrightSession session, Page sourcePage, List<Page> pagesBeforeClick) {
		Page newPage = findNewPage(session, pagesBeforeClick);
		if (newPage != null) {
			return newPage;
		}
		try {
			sourcePage.waitForTimeout(750);
		} catch (Exception ignored) {
		}
		return findNewPage(session, pagesBeforeClick);
	}

	private static Page findNewPage(PlaywrightSession session, List<Page> pagesBeforeClick) {
		for (Page candidate : session.CTX.pages()) {
			if (!pagesBeforeClick.contains(candidate)) {
				return candidate;
			}
		}
		return null;
	}

	/**
	 * Handles the creation of a new tab.
	 *
	 * @param session  The PlaywrightSession object.
	 * @param newPage  The new Page object.
	 * @param response The response map to be populated with new tab details.
	 */
	private static void handleNewTab(PlaywrightSession session, Page newPage, String preferredTabId,
			Map<String, Object> response) {
		classLogger.info("New tab detected: " + newPage.url());

		try {
			newPage.waitForLoadState(LoadState.LOAD, new Page.WaitForLoadStateOptions().setTimeout(500));
		} catch (Exception e) {
			classLogger.warn("Timed out waiting for new tab to reach load state at URL '{}'", newPage.url(), e);
		}

		try {
			newPage.waitForLoadState(LoadState.NETWORKIDLE, new Page.WaitForLoadStateOptions().setTimeout(500));
		} catch (Exception ignored) {
		}

		response.put("isNewTab", true);
		response.put("tabTitle", newPage.title());
		createNewTabRecord(session, newPage, preferredTabId, response);
	}

	/**
	 * Focuses on an element and types text into it.
	 *
	 * @param loc  The Locator for the element.
	 * @param text The text to type.
	 * @return true if typing was successful, false otherwise.
	 */
	private static boolean focusAndType(Locator loc, String text) {
		if (!isActionable(loc)) {
			return false;
		}
		try {
			loc.click(new Locator.ClickOptions().setTimeout(300));
			if (text != null) {
				String oldVal = null;
				try {
					oldVal = loc.inputValue(new Locator.InputValueOptions().setTimeout(100));
				} catch (Exception ignore) {
				}
				try {
					loc.fill(text, new Locator.FillOptions().setTimeout(500));
				} // reduced from 1000-1200
				catch (Exception e) {
					loc.fill(text, new Locator.FillOptions().setTimeout(700));
				}

				try {
					String newVal = loc.inputValue(new Locator.InputValueOptions().setTimeout(100));
					if (oldVal != null && Objects.equals(oldVal, newVal)) {
						return false;
					}
				} catch (Exception ignore) {
					/* not an input? okay */ }
			}
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	/**
	 * "Heals" a selector by finding the element at the given coordinates and
	 * generating a new selector for it. If the step selector indicates an iframe,
	 * adjusts coordinates and searches within the iframe.
	 *
	 * @param page         The Playwright Page object.
	 * @param x            The x-coordinate.
	 * @param y            The y-coordinate.
	 * @param stepSelector The original step selector (may point to iframe).
	 * @return The healed Locator, or null if healing fails.
	 */
	private static Locator healSelector(Page page, int x, int y, Selector stepSelector) {
		// Check if element is inside an iframe using frameSelector field
		String frameSelector = (stepSelector != null) ? stepSelector.frameSelector() : null;

		if (frameSelector != null && !frameSelector.isEmpty()) {
			classLogger.info("Healing selector inside iframe at coords ({}, {})", x, y);
			return null;
		}

		// main page
		String script = """
				({x,y})=>{
				  const el=document.elementFromPoint(x,y);
				  if(!el) return null;
				  const id=el.id;
				  if(id) return {strategy:'id',value:id};
				  const testId=el.getAttribute('data-testid')||el.getAttribute('data-test-id');
				  if(testId) return {strategy:'testId',value:testId};
				  const role=el.getAttribute('role');
				  if(role) return {strategy:'role',value:role};
				  function css(e){
				    if(!e||e===document.body) return 'body';
				    if(e.id) return '#'+CSS.escape(e.id);
				    let s=e.localName;
				    if(e.classList.length && e.classList.length<=3) s+='.'+[...e.classList].map(c=>CSS.escape(c)).join('.');
				    const p=e.parentElement;
				    if(!p) return s;
				    const sib=[...p.children].filter(c=>c.localName===e.localName);
				    const idx=sib.indexOf(e)+1;
				    return css(p)+' > '+s+`:nth-of-type(${idx})`;
				  }
				  return {strategy:'css', value: css(el)};
				}
				""";

		Map<String, String> sel = evaluateSelectorProbeSafely(page, script, java.util.Map.of("x", x, "y", y));

		if (sel == null) {
			return null;
		}
		// Main page element - no frameSelector
		return resolveLocator(page, new Selector(sel.get("strategy"), sel.get("value"), null));
	}

	/**
	 * Safely evaluates a script on the page to probe for a selector.
	 *
	 * @param page   The Playwright Page object.
	 * @param script The script to evaluate.
	 * @param args   The arguments for the script.
	 * @return A map representing the selector, or null if evaluation fails.
	 */
	private static Map<String, String> evaluateSelectorProbeSafely(Page page, String script, Map<String, Object> args) {
		for (int attempt = 0; attempt < 2; attempt++) {
			try {
				Object raw = page.evaluate(script, args);
				return (Map<String, String>) raw; // may be null
			} catch (PlaywrightException ex) {
				String msg = ex.getMessage();
				boolean ctxDestroyed = msg != null && msg.toLowerCase().contains("execution context was destroyed");
				if (!ctxDestroyed || attempt == 1) {
					return null;
				}
				try {
					page.waitForLoadState(LoadState.DOMCONTENTLOADED,
							new Page.WaitForLoadStateOptions().setTimeout(3000));
				} catch (Exception ignore) {
				}
			} catch (Exception e) {
				return null;
			}
		}
		return null;
	}

	/**
	 * Checks if a locator is visible and enabled.
	 *
	 * @param loc The Locator to check.
	 * @return true if the locator is actionable, false otherwise.
	 */
	private static boolean isActionable(Locator loc) {
		try {
			return loc != null && loc.isVisible() && loc.isEnabled();
		} catch (Exception e) {
			return false;
		}
	}

	/**
	 * Executes a navigation step.
	 *
	 * @param page     The Playwright Page object.
	 * @param step     The PlaywrightStep containing navigation information.
	 * @param response The response map to be populated with execution details.
	 */
	private static void navigateStep(Page page, PlaywrightStep step, Map<String, Object> response) {
		long start = System.currentTimeMillis();
		var opts = new Page.NavigateOptions().setWaitUntil(com.microsoft.playwright.options.WaitUntilState.LOAD)
				.setTimeout(60_000);
		page.navigate(step.url(), opts);

		// Try to wait for network idle without blocking forever
		try {
			page.waitForLoadState(com.microsoft.playwright.options.LoadState.NETWORKIDLE,
					new Page.WaitForLoadStateOptions().setTimeout(4_000));
		} catch (PlaywrightException ignored) {
			// Continue if network idle doesn't happen
		} finally {
			response.put("tabTitle", page.title());
		}
		classLogger.info("Tab Title: {}", response.get("tabTitle"));
		classLogger.info("[ACTION] NAVIGATE took {} ms {}", System.currentTimeMillis() - start, step.url());
	}

	/**
	 * Executes a click step.
	 *
	 * @param page      The Playwright Page object.
	 * @param step      The PlaywrightStep containing click information.
	 * @param beforeUrl The URL before the click.
	 * @param session   The PlaywrightSession object.
	 * @param response  The response map to be populated with execution details.
	 */
	private static void clickStep(Page page, PlaywrightStep step, String beforeUrl, PlaywrightSession session,
			Map<String, Object> response) {
		long start = System.currentTimeMillis();
		if (step.selector() != null) {
			Locator loc = resolveLocator(page, step.selector());
			if (loc == null) {
				// No selector match - don't drop to coords; surface as SELECTOR_NOT_FOUND
				throw new PlaywrightException("SELECTOR_NOT_FOUND: " + step.selector().value());
			}
			// otherwise proceed with the clickable path above
		}
		boolean ok = clickWithFallback(page, step, beforeUrl, session, response);
		if (!ok) {
			throw new PlaywrightException(
					"NO_EFFECT: click had no actionable target (selector not found & no hit at coords).");
		}

		classLogger.info("[ACTION] CLICK took {} ms (selector={})", System.currentTimeMillis() - start,
				step.selector() != null ? step.selector().value() : "coords");
	}

	/**
	 * Executes a type step.
	 *
	 * @param page The Playwright Page object.
	 * @param step The PlaywrightStep containing typing information.
	 */
	private static void typeStep(Page page, PlaywrightStep step) {
		long start = System.currentTimeMillis();
		if (step.selector() != null) {
			Locator loc = resolveLocator(page, step.selector());
			if (loc == null) {
				classLogger.warn("TYPE selector not found for step {}; falling back to coords/focused element: {}",
						step.id(), step.selector().value());
			}
		}
		boolean ok = typeWithFallback(page, step);
		if (!ok) {
			throw new PlaywrightException(
					"NO_EFFECT: type had no focusable text control (selector not found & no focused input/textarea/contentEditable).");
		}

		classLogger.info("[ACTION] TYPE took {} ms (text={})", System.currentTimeMillis() - start,
				step.text() != null ? "\"" + step.text() + "\"" : "null");
	}

	/**
	 * Executes a scroll step.
	 *
	 * @param page The Playwright Page object.
	 * @param step The PlaywrightStep containing scroll information.
	 */
	private static void scrollStep(Page page, PlaywrightStep step) {
		long start = System.currentTimeMillis();
		int deltaY = step.deltaY() != null ? step.deltaY() : 300;
		page.mouse().wheel(0, deltaY);
		classLogger.info("[ACTION] SCROLL took {} ms (deltaY={})", System.currentTimeMillis() - start, deltaY);
	}

	/**
	 * Executes a hover step.
	 *
	 * @param page The Playwright Page object.
	 * @param step The PlaywrightStep containing hover information.
	 */
	private static void hoverStep(Page page, PlaywrightStep step) {
		long start = System.currentTimeMillis();
		if (step.selector() != null) {
			Locator loc = resolveLocator(page, step.selector());
			if (loc == null) {
				// No selector match
				throw new PlaywrightException("SELECTOR_NOT_FOUND: " + step.selector().value());
			}
		}
		boolean ok = hoverWithFallback(page, step);

		if (!ok) {
			throw new PlaywrightException(
					"NO_EFFECT: hover had no actionable target (selector not found & no hit at coords).");
		}

		page.waitForTimeout(100);

		classLogger.info("[ACTION] HOVER took {} ms (selector={})", System.currentTimeMillis() - start,
				step.selector() != null ? step.selector().value() : "coords");
	}

	/**
	 * Executes a wait step.
	 *
	 * @param page The Playwright Page object.
	 * @param step The PlaywrightStep containing wait information.
	 */
	private static void waitStep(Page page, PlaywrightStep step) {
		long start = System.currentTimeMillis();
		int ms = step.waitAfterMs() != null ? step.waitAfterMs() : 300;
		page.waitForTimeout(ms);
		classLogger.info("[ACTION] WAIT took {} ms (timeout={})", System.currentTimeMillis() - start, ms);
	}

	/**
	 * Creates a MutationObserver to detect DOM changes.
	 *
	 * @param page The Playwright Page object.
	 * @return A JSHandle to a Promise that resolves when a mutation is detected.
	 */
	private static JSHandle createMutationObserver(Page page) {
		return page.evaluateHandle(
				"""
						() => new Promise(resolve => {
						  const observer = new MutationObserver(muts => {
						    for (const m of muts) {
						      if (m.type === 'childList' && (m.addedNodes.length > 0 || m.removedNodes.length > 0)) {
						        observer.disconnect(); resolve(true); return;
						      }
						      if (m.type === 'characterData' && m.target.nodeValue && m.target.nodeValue.trim().length > 0) {
						        observer.disconnect(); resolve(true); return;
						      }
						      if (m.type === 'attributes' && m.attributeName !== 'value') {
						        observer.disconnect(); resolve(true); return;
						      }
						    }
						  });
						  observer.observe(document.body, { childList: true, subtree: true, attributes: true, characterData: true });
						  setTimeout(() => { observer.disconnect(); resolve(false); }, 800);
						})
						""");
	}

	/**
	 * Detects if the page has changed by evaluating the mutation promise.
	 *
	 * @param mutationPromise The JSHandle to the mutation promise.
	 * @return true if the DOM changed, false otherwise.
	 */
	private static boolean detectPageChange(JSHandle mutationPromise) {
		try {
			boolean domChanged = (boolean) mutationPromise.evaluate("value => value");
			return domChanged;
		} catch (Exception e) {
			throw new RuntimeException("Failed to evaluate DOM changes: " + e);
		}
	}

	/**
	 * Creates a new tab record in the session history.
	 *
	 * @param session  The PlaywrightSession object.
	 * @param page     The new Page object.
	 * @param response The response map to be populated with the new tab ID.
	 */
	private static void createNewTabRecord(PlaywrightSession session, Page page, String preferredTabId,
			Map<String, Object> response) {
		String tabId = session.registerPage(page, preferredTabId);
		// Store the new tab ID in the response so it can be returned
		response.put("newTabId", tabId);
	}

}
