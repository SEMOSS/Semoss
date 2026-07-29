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
package prerna.remoteviewer.service;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.microsoft.playwright.Locator;
import com.microsoft.playwright.Mouse;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.PlaywrightException;
import com.microsoft.playwright.options.LoadState;
import com.microsoft.playwright.options.MouseButton;
import com.microsoft.playwright.options.WaitUntilState;

import prerna.reactor.playwright.Selector;
import prerna.remoteviewer.model.RemoteBrowserInputEvent;
import prerna.remoteviewer.security.RemoteBrowserUrlSafetyValidator;

/**
 * Maps validated frontend input events onto Playwright browser actions. For
 * CLICK events, uses a 3-tier fallback: selector -> coords -> skip. After
 * each action, respects waitAfterMs and waits for page to settle.
 *
 * All calls must be made from the session's dedicated Playwright thread.
 */
public class RemoteBrowserInputService {

	private static final Logger classLogger = LogManager.getLogger(RemoteBrowserInputService.class);
	private static final Set<String> SAFE_CURSOR_VALUES = Set.of("auto", "default", "none", "context-menu",
			"help", "pointer", "progress", "wait", "cell", "crosshair", "text", "vertical-text", "alias",
			"copy", "move", "no-drop", "not-allowed", "grab", "grabbing", "e-resize", "n-resize",
			"ne-resize", "nw-resize", "s-resize", "se-resize", "sw-resize", "w-resize", "ew-resize",
			"ns-resize", "nesw-resize", "nwse-resize", "col-resize", "row-resize", "all-scroll", "zoom-in",
			"zoom-out");

	private RemoteBrowserInputService() {
	}

	public static Map<String, Object> dispatch(RemoteBrowserSession session, RemoteBrowserInputEvent event) {
		Map<String, Object> result = new HashMap<>();
		try {
			if ("switch-tab".equals(event.getType())) {
				if (!session.setActiveTabId(event.getTargetTabId())) {
					throw new IllegalStateException("Browser tab " + event.getTargetTabId() + " is missing or closed");
				}
				result.put("success", true);
				result.put("url", safeUrl(session.getActivePage()));
				return result;
			}
			if ("prepare-replay".equals(event.getType())) {
				Page replayRoot = Boolean.TRUE.equals(event.getReuseActiveTab())
						? session.getActivePage()
						: session.getContext().newPage();
				String liveTabId = session.getPlaywrightSession().findTabId(replayRoot);
				if (liveTabId == null) {
					liveTabId = session.getPlaywrightSession().registerPage(replayRoot, null);
				}
				session.getPlaywrightSession().beginReplayTabBinding(replayRoot);
				session.setActiveTabId(liveTabId);
				result.put("success", true);
				result.put("url", safeUrl(replayRoot));
				return result;
			}
			if ("switch-replay-tab".equals(event.getType())) {
				Page replayPage = session.getPlaywrightSession().resolveReplayPage(event.getTargetTabId(),
						session.getActivePage());
				String liveTabId = session.getPlaywrightSession().findTabId(replayPage);
				if (liveTabId == null || !session.setActiveTabId(liveTabId)) {
					throw new IllegalStateException(
							"Recorded tab " + event.getTargetTabId() + " has not been opened by playback yet");
				}
				result.put("success", true);
				result.put("url", safeUrl(replayPage));
				return result;
			}
			if ("close-tab".equals(event.getType())) {
				closeLiveTab(session, event.getTargetTabId());
				result.put("success", true);
				result.put("url", safeUrl(session.getActivePage()));
				return result;
			}
		} catch (Exception e) {
			result.put("success", false);
			result.put("error", e.getMessage() == null ? "Browser tab action failed" : e.getMessage());
			return result;
		}

		Page page = session.getActivePage();
		if (page == null || page.isClosed()) {
			result.put("success", false);
			result.put("error", "Remote browser page is unavailable");
			return result;
		}

		String urlBefore = safeUrl(page);
		String type = event.getType();
		classLogger.info("Remote viewer dispatch start session={} event={} urlBefore={}", session.getSessionId(),
				describeEvent(event), urlBefore);
		long start = System.currentTimeMillis();
		try {
			switch (type) {
			case "mouse-click":
				clickWithNavigationWait(page, event);
				break;
			case "mouse-move":
				page.mouse().move(event.getX(), event.getY());
				break;
			case "mouse-down":
				page.mouse().down(buildMouseDownOptions(event));
				break;
			case "mouse-up":
				page.mouse().up(buildMouseUpOptions(event));
				break;
			case "wheel":
				wheel(page, event);
				break;
			case "type-text":
				typeText(page, event);
				break;
			case "key":
				key(page, event);
				break;
			case "navigate":
				navigate(page, event);
				break;
			case "navigate-back":
				goBack(page, event);
				break;
			case "navigate-forward":
				goForward(page, event);
				break;
			case "reload":
				reload(page, event);
				break;
			default:
				classLogger.warn("Unhandled input event type: {}", type);
				result.put("success", false);
				result.put("error", "Unhandled input event type: " + type);
				return result;
			}
		} catch (Exception e) {
			classLogger.warn("Error dispatching event '{}' on session {}: {}", type, session.getSessionId(),
					e.getMessage());
			result.put("success", false);
			result.put("error", e.getMessage() == null ? "Browser action failed" : e.getMessage());
			return result;
		}

		// Post-action: wait as specified, then wait for page to settle
		try {
			postActionWait(page, event, urlBefore);
			result.put("success", true);
			result.put("url", safeUrl(page));
			classLogger.info("Remote viewer dispatch end session={} eventType={} elapsedMs={} urlAfter={}",
					session.getSessionId(), type, System.currentTimeMillis() - start, result.get("url"));
		} catch (Exception e) {
			classLogger.warn("Error settling event '{}' on session {}: {}", type, session.getSessionId(),
					e.getMessage());
			result.put("success", false);
			result.put("error", e.getMessage() == null ? "Browser action did not settle" : e.getMessage());
		}
		return result;
	}

	/** Returns the safe computed CSS cursor beneath the remote pointer. */
	public static String cursorAt(RemoteBrowserSession session, RemoteBrowserInputEvent event) {
		Page page = session == null ? null : session.getActivePage();
		if (page == null || page.isClosed() || event == null || event.getX() == null || event.getY() == null) {
			return "default";
		}
		try {
			Map<String, Double> point = Map.of("x", event.getX(), "y", event.getY());
			Object result = page.evaluate("point => {"
					+ "const element = document.elementFromPoint(point.x, point.y);"
					+ "return element ? getComputedStyle(element).cursor : 'default';"
					+ "}", point);
			String cursor = result == null ? "default" : result.toString().trim().toLowerCase();
			return SAFE_CURSOR_VALUES.contains(cursor) ? cursor : "default";
		} catch (Exception ignored) {
			return "default";
		}
	}

	private static void closeLiveTab(RemoteBrowserSession session, String tabId) {
		if (session.isRecordingEnabled()) {
			throw new IllegalStateException("Tabs cannot be closed while recording");
		}
		long openTabs = session.getPlaywrightSession().getTabPages().values().stream()
				.filter(page -> page != null && !page.isClosed()).count();
		if (openTabs <= 1) {
			throw new IllegalStateException("At least one browser tab must remain open");
		}
		Page page = session.getPlaywrightSession().getLivePage(tabId);
		if (page == null || page.isClosed()) {
			return;
		}
		session.getPlaywrightSession().removeReplayBindings(page);
		page.close();
		if (tabId.equals(session.getActiveTabId())) {
			session.activateFallbackTab();
		}
	}

	// ---- Click with 3-tier fallback ----

	private static void clickWithNavigationWait(Page page, RemoteBrowserInputEvent event) {
		if (isLiveEvent(event)) {
			ensureClickSucceeded(clickWithFallback(page, event, true));
			return;
		}

		boolean likelyNavigation = isLikelyNavigationClick(page, event);
		classLogger.info("Remote viewer click navigationProbe likelyNavigation={} event={}", likelyNavigation,
				describeEvent(event));
		if (!likelyNavigation) {
			ensureClickSucceeded(clickWithFallback(page, event, false));
			return;
		}

		AtomicBoolean clicked = new AtomicBoolean(false);
		try {
			page.waitForNavigation(
					new Page.WaitForNavigationOptions().setTimeout(8_000).setWaitUntil(WaitUntilState.NETWORKIDLE),
					() -> clicked.set(clickWithFallback(page, event, false)));
			classLogger.info("Remote viewer click navigation observed urlAfter={}", safeUrl(page));
		} catch (PlaywrightException e) {
			classLogger.info("Remote viewer click navigation wait timed out; continuing urlAfter={} reason={}",
					safeUrl(page), e.getMessage());
		}
		ensureClickSucceeded(clicked.get());
	}

	private static void ensureClickSucceeded(boolean clicked) {
		if (!clicked) {
			throw new PlaywrightException("Click had no actionable target");
		}
	}

	private static boolean isLikelyNavigationClick(Page page, RemoteBrowserInputEvent event) {
		try {
			Map<String, Object> payload = new HashMap<>();
			payload.put("x", event.getX());
			payload.put("y", event.getY());
			payload.put("selector", selectorPayload(event.getSelector()));
			Object result = page.evaluate("(event) => {" + "let el = null;" + "const sel = event.selector;"
					+ "if (sel && sel.value) {" + "  try {"
					+ "    if (sel.strategy === 'id') el = document.getElementById(sel.value);"
					+ "    else if (sel.strategy === 'css') el = document.querySelector(sel.value);"
					+ "    else if (sel.strategy === 'role') el = document.querySelector('[role=\"' + CSS.escape(sel.value) + '\"]');"
					+ "    else if (sel.strategy === 'xpath') el = document.evaluate(sel.value, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue;"
					+ "  } catch (e) {}" + "}"
					+ "if (!el && event.x != null && event.y != null) el = document.elementFromPoint(event.x, event.y);"
					+ "if (!el) return false;"
					+ "const target = el.closest('a[href], button, input, [role=\"link\"], [role=\"button\"]');"
					+ "if (!target) return false;" + "if (target.matches('a[href], [role=\"link\"]')) return true;"
					+ "if (target.matches('button[type=\"submit\"], input[type=\"submit\"]')) return true;"
					+ "const form = target.closest('form');"
					+ "return !!form && target.matches('button:not([type]), button[type=\"submit\"], input[type=\"submit\"]');"
					+ "}", payload);
			return Boolean.TRUE.equals(result);
		} catch (Exception e) {
			return false;
		}
	}

	private static Map<String, Object> selectorPayload(Selector sel) {
		Map<String, Object> payload = new HashMap<>();
		if (sel == null) {
			return payload;
		}
		payload.put("strategy", sel.strategy() != null ? sel.strategy() : "");
		payload.put("value", sel.value() != null ? sel.value() : "");
		return payload;
	}

	private static boolean clickWithFallback(Page page, RemoteBrowserInputEvent event, boolean noWaitAfter) {
		Selector sel = event.getSelector();

		// 1) Try CSS/ID selector (most reliable, survives minor layout changes)
		if (sel != null && sel.value() != null && !sel.value().isBlank()) {
			try {
				Locator loc = resolveLocator(page, sel);
				if (loc != null) {
					classLogger.info("Remote viewer click attempt method=selector selector={}", describeSelector(sel));
					loc.click(new Locator.ClickOptions().setTimeout(2000).setNoWaitAfter(noWaitAfter));
					classLogger.info("Remote viewer click success method=selector selector={} urlAfter={}",
							describeSelector(sel), safeUrl(page));
					return true;
				}
			} catch (Exception e) {
				classLogger.info("Remote viewer click failed method=selector selector={} reason={} fallback=coords",
						describeSelector(sel), e.getMessage());
			}
		}

		// 2) Fall back to raw coordinates
		if (event.getX() != null && event.getY() != null) {
			try {
				classLogger.info("Remote viewer click attempt method=coords x={} y={}", round(event.getX()),
						round(event.getY()));
				page.mouse().click(event.getX(), event.getY());
				classLogger.info("Remote viewer click success method=coords x={} y={} urlAfter={}", round(event.getX()),
						round(event.getY()), safeUrl(page));
				return true;
			} catch (Exception e) {
				classLogger.warn("Coord click failed at ({}, {}): {}", event.getX(), event.getY(), e.getMessage());
			}
		}

		classLogger.warn("Click could not be performed - no selector and no coords");
		return false;
	}

	private static Locator resolveLocator(Page page, Selector sel) {
		if (sel == null || sel.value() == null) {
			return null;
		}
		String strat = sel.strategy();
		String val = sel.value();
		try {
			if ("id".equals(strat)) {
				return page.locator("#" + val);
			}
			if ("css".equals(strat)) {
				return page.locator(val);
			}
			if ("xpath".equals(strat)) {
				return page.locator("xpath=" + val);
			}
			if ("role".equals(strat)) {
				return page.locator("[role=\"" + val + "\"]");
			}
			if ("text".equals(strat)) {
				return page.getByText(val);
			}
			return page.locator(val); // default: treat as CSS
		} catch (Exception e) {
			return null;
		}
	}

	// ---- Post-action wait ----

	private static void postActionWait(Page page, RemoteBrowserInputEvent event, String urlBefore) {
		String type = event.getType();
		// Always skip wait for mouse-move (high frequency, no meaningful wait)
		if ("mouse-move".equals(type)) {
			return;
		}
		if (isLiveEvent(event)) {
			return;
		}

		// Replay navigation waits for the page to settle before the next action.
		if ("navigate".equals(type) || "navigate-back".equals(type) || "navigate-forward".equals(type)
				|| "reload".equals(type)) {
			try {
				page.waitForLoadState(LoadState.NETWORKIDLE, new Page.WaitForLoadStateOptions().setTimeout(10_000));
				classLogger.info("Remote viewer loadState reached NETWORKIDLE type={} url={}", type, safeUrl(page));
			} catch (PlaywrightException e) {
				classLogger.info("Remote viewer loadState NETWORKIDLE timeout type={} reason={}", type, e.getMessage());
				try {
					page.waitForLoadState(LoadState.LOAD, new Page.WaitForLoadStateOptions().setTimeout(3_000));
					classLogger.info("Remote viewer loadState reached LOAD type={} url={}", type, safeUrl(page));
				} catch (Exception ignored) {
					classLogger.info("Remote viewer loadState LOAD fallback timed out type={} url={}", type,
							safeUrl(page));
				}
			}
			return;
		}

		int waitMs = event.getWaitAfterMs();
		classLogger.debug("Remote viewer postActionWait start type={} waitMs={} urlBefore={}", type, waitMs, urlBefore);

		// For clicks: honour waitAfterMs then check if URL changed (navigation
		// triggered)
		if (waitMs > 0) {
			page.waitForTimeout(waitMs);
		}

		String urlAfter = safeUrl(page);
		if (!urlBefore.equals(urlAfter)) {
			// Click triggered a navigation - wait for it to settle
			classLogger.info("Remote viewer postActionWait detected URL change type={} from={} to={}", type, urlBefore,
					urlAfter);
			try {
				page.waitForLoadState(LoadState.NETWORKIDLE, new Page.WaitForLoadStateOptions().setTimeout(10_000));
				classLogger.info("Remote viewer postActionWait NETWORKIDLE reached type={} url={}", type,
						safeUrl(page));
			} catch (PlaywrightException e) {
				classLogger.info("Remote viewer postActionWait NETWORKIDLE timeout type={} reason={}", type,
						e.getMessage());
				try {
					page.waitForLoadState(LoadState.LOAD, new Page.WaitForLoadStateOptions().setTimeout(3_000));
					classLogger.info("Remote viewer postActionWait LOAD reached type={} url={}", type, safeUrl(page));
				} catch (Exception ignored) {
					classLogger.info("Remote viewer postActionWait LOAD fallback timed out type={} url={}", type,
							safeUrl(page));
				}
			}
		} else {
			classLogger.debug("Remote viewer postActionWait no URL change type={} url={}", type, urlAfter);
		}
	}

	// ---- Other actions ----

	private static void wheel(Page page, RemoteBrowserInputEvent event) {
		classLogger.info("Remote viewer wheel x={} y={} deltaX={} deltaY={}", round(event.getX()), round(event.getY()),
				event.getDeltaX(), event.getDeltaY());
		page.mouse().wheel(event.getDeltaX() != null ? event.getDeltaX() : 0,
				event.getDeltaY() != null ? event.getDeltaY() : 0);
	}

	private static void typeText(Page page, RemoteBrowserInputEvent event) {
		Selector sel = event.getSelector();
		if (sel != null && sel.value() != null && !sel.value().isBlank()) {
			try {
				Locator loc = resolveLocator(page, sel);
				if (loc != null) {
					classLogger.info("Remote viewer type attempt method=selector selector={} textLength={}",
							describeSelector(sel), event.getText() != null ? event.getText().length() : null);
					loc.pressSequentially(event.getText(), new Locator.PressSequentiallyOptions().setTimeout(5_000));
					classLogger.info("Remote viewer type success method=selector selector={}", describeSelector(sel));
					return;
				}
			} catch (Exception e) {
				classLogger.info("Remote viewer type failed method=selector selector={} reason={} fallback=keyboard",
						describeSelector(sel), e.getMessage());
			}
		}
		if (event.getX() != null && event.getY() != null) {
			try {
				classLogger.info("Remote viewer type focus attempt method=coords x={} y={}", round(event.getX()),
						round(event.getY()));
				page.mouse().click(event.getX(), event.getY());
				classLogger.info("Remote viewer type focus success method=coords x={} y={}", round(event.getX()),
						round(event.getY()));
			} catch (Exception e) {
				classLogger.debug("Could not focus type target at ({}, {}): {}", event.getX(), event.getY(),
						e.getMessage());
			}
		}
		page.keyboard().type(event.getText());
		classLogger.info("Remote viewer type success method=keyboard textLength={}",
				event.getText() != null ? event.getText().length() : null);
	}

	private static void key(Page page, RemoteBrowserInputEvent event) {
		String keyCombo = buildKeyCombo(event);
		classLogger.info("Remote viewer key press combo={}", keyCombo);
		page.keyboard().press(keyCombo);
	}

	private static void navigate(Page page, RemoteBrowserInputEvent event) {
		RemoteBrowserUrlSafetyValidator.validate(event.getUrl());
		classLogger.info("Remote viewer navigate url={}", event.getUrl());
		if (isLiveEvent(event)) {
			page.navigate(event.getUrl(), new Page.NavigateOptions().setWaitUntil(WaitUntilState.COMMIT));
		} else {
			page.navigate(event.getUrl());
		}
	}

	private static void goBack(Page page, RemoteBrowserInputEvent event) {
		if (isLiveEvent(event)) {
			page.goBack(new Page.GoBackOptions().setWaitUntil(WaitUntilState.COMMIT));
		} else {
			page.goBack();
		}
	}

	private static void goForward(Page page, RemoteBrowserInputEvent event) {
		if (isLiveEvent(event)) {
			page.goForward(new Page.GoForwardOptions().setWaitUntil(WaitUntilState.COMMIT));
		} else {
			page.goForward();
		}
	}

	private static void reload(Page page, RemoteBrowserInputEvent event) {
		if (isLiveEvent(event)) {
			page.reload(new Page.ReloadOptions().setWaitUntil(WaitUntilState.COMMIT));
		} else {
			page.reload();
		}
	}

	private static boolean isLiveEvent(RemoteBrowserInputEvent event) {
		return event.getWaitAfterMs() == null;
	}

	private static String safeUrl(Page page) {
		try {
			return page.url();
		} catch (Exception e) {
			return "";
		}
	}

	// ---- Helpers ----

	private static MouseButton resolveButton(String btn) {
		if (btn == null) {
			return MouseButton.LEFT;
		}
		switch (btn) {
		case "right":
			return MouseButton.RIGHT;
		case "middle":
			return MouseButton.MIDDLE;
		default:
			return MouseButton.LEFT;
		}
	}

	private static Mouse.DownOptions buildMouseDownOptions(RemoteBrowserInputEvent event) {
		Mouse.DownOptions opts = new Mouse.DownOptions();
		opts.setButton(resolveButton(event.getButton()));
		return opts;
	}

	private static Mouse.UpOptions buildMouseUpOptions(RemoteBrowserInputEvent event) {
		Mouse.UpOptions opts = new Mouse.UpOptions();
		opts.setButton(resolveButton(event.getButton()));
		return opts;
	}

	private static String buildKeyCombo(RemoteBrowserInputEvent event) {
		Map<String, Boolean> mods = event.getModifiers();
		StringBuilder sb = new StringBuilder();
		if (mods != null) {
			if (Boolean.TRUE.equals(mods.get("ctrl"))) {
				sb.append("Control+");
			}
			if (Boolean.TRUE.equals(mods.get("meta"))) {
				sb.append("Meta+");
			}
			if (Boolean.TRUE.equals(mods.get("alt"))) {
				sb.append("Alt+");
			}
			if (Boolean.TRUE.equals(mods.get("shift"))) {
				sb.append("Shift+");
			}
		}
		sb.append(event.getKey());
		return sb.toString();
	}

	private static String describeEvent(RemoteBrowserInputEvent event) {
		if (event == null) {
			return "null";
		}
		StringBuilder sb = new StringBuilder();
		sb.append("type=").append(event.getType());
		sb.append(", x=").append(round(event.getX()));
		sb.append(", y=").append(round(event.getY()));
		sb.append(", selector=").append(describeSelector(event.getSelector()));
		sb.append(", waitAfterMs=").append(event.getWaitAfterMs());
		if (event.getUrl() != null) {
			sb.append(", url=").append(truncate(event.getUrl(), 180));
		}
		if (event.getText() != null) {
			sb.append(", textLength=").append(event.getText().length());
		}
		if (event.getKey() != null) {
			sb.append(", key=").append(event.getKey());
		}
		return sb.toString();
	}

	private static String describeSelector(Selector selector) {
		if (selector == null) {
			return "none";
		}
		return selector.strategy() + ":" + truncate(selector.value(), 120);
	}

	private static String truncate(String value, int max) {
		if (value == null || value.length() <= max) {
			return value;
		}
		return value.substring(0, max) + "...";
	}

	private static Object round(Double value) {
		if (value == null) {
			return null;
		}
		return Math.round(value * 100.0) / 100.0;
	}
}
