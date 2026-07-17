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

import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.playwright.Coords;
import prerna.reactor.playwright.PlaywrightStep;
import prerna.reactor.playwright.PlaywrightStepType;
import prerna.reactor.playwright.Selector;
import prerna.reactor.playwright.Viewport;
import prerna.remoteviewer.model.RemoteBrowserInputEvent;
import prerna.remoteviewer.model.RemoteBrowserRecordedStep;

/**
 * Records remote-browser interactions into the same replayable
 * {@link PlaywrightStep} history used by {@code prerna.reactor.playwright}.
 *
 * <p>
 * The remote viewer sends live typing character-by-character. For replay, those
 * events need to become one TYPE step per target field, so this service merges
 * adjacent TYPE events that share the same selector signature.
 */
public class RemoteBrowserRecordingService {

	private static final Logger classLogger = LogManager.getLogger(RemoteBrowserRecordingService.class);

	private static final String DEFAULT_TAB_ID = "tab-1";
	private static final int DEFAULT_CLICK_WAIT_MS = 300;
	private static final double DEVICE_SCALE_FACTOR = 1.0;

	private RemoteBrowserRecordingService() {
	}

	/**
	 * Records an input event into the temporary browser-session recording buffer
	 * when recording is enabled.
	 */
	public static void record(RemoteBrowserSession session, RemoteBrowserInputEvent event) {
		if (session == null || event == null || event.getType() == null) {
			return;
		}

		if (isRecordingControl(event)) {
			Boolean enabled = event.getRecording() != null ? event.getRecording() : event.getRecord();
			if (enabled != null) {
				session.setRecordingEnabled(enabled.booleanValue());
				if (enabled.booleanValue()) {
					session.clearRecordingBuffer();
					recordCurrentNavigation(session);
				} else if (Boolean.TRUE.equals(event.getDiscard())) {
					session.clearRecordingBuffer();
				}
			}
			return;
		}

		if (!shouldRecord(session, event)) {
			session.clearPendingTypeStep();
			return;
		}

		switch (event.getType()) {
		case "mouse-click":
			session.clearPendingTypeStep();
			recordClick(session, event);
			break;
		case "type-text":
			recordType(session, event);
			break;
		case "key":
			recordKey(session, event);
			break;
		case "navigate":
			session.clearPendingTypeStep();
			appendStep(session,
					buildStep(session, event, PlaywrightStepType.NAVIGATE, event.getUrl(), null, null, null, null),
					true);
			session.startNextRemoteBrowserRecordedStepOnNewPage();
			addLegacyStep(session, event);
			break;
		case "wheel":
			session.clearPendingTypeStep();
			appendStep(session, buildStep(session, event, PlaywrightStepType.SCROLL, null, coords(event), null, null,
					toInteger(event.getDeltaY())), false);
			addLegacyStep(session, event);
			break;
		default:
			session.clearPendingTypeStep();
			break;
		}
	}

	/**
	 * Captures the session's starting URL as the first replay step. This mirrors
	 * the classic recorder shape where tab-1 begins with its own NAVIGATE group,
	 * and the first real interaction starts the next group.
	 */
	public static void recordInitialNavigation(RemoteBrowserSession session, String url) {
		if (session == null || !session.isRecordingEnabled()) {
			return;
		}
		recordNavigation(session, url);
	}

	public static void recordCurrentNavigation(RemoteBrowserSession session) {
		if (session == null) {
			return;
		}
		recordNavigation(session, safeUrl(session));
	}

	public static void discardRecording(RemoteBrowserSession session) {
		if (session == null) {
			return;
		}
		session.setRecordingEnabled(false);
		session.clearRecordingBuffer();
	}

	private static void recordNavigation(RemoteBrowserSession session, String url) {
		if (session == null || url == null || url.isBlank()) {
			return;
		}
		PlaywrightStep step = new PlaywrightStep(0, PlaywrightStepType.NAVIGATE, url, null, null, null, null, null,
				null, null, 100, viewport(session, null), System.currentTimeMillis(), null, null, false, false, null,
				null, Boolean.TRUE, Boolean.FALSE, null, null);
		appendStep(session, step, true);
		session.startNextRemoteBrowserRecordedStepOnNewPage();
	}

	private static void recordClick(RemoteBrowserSession session, RemoteBrowserInputEvent event) {
		PlaywrightStep step = buildStep(session, event, PlaywrightStepType.CLICK, null, coords(event), null,
				waitAfter(event), null);
		appendStep(session, step, false);
		addLegacyStep(session, event);
	}

	private static void recordType(RemoteBrowserSession session, RemoteBrowserInputEvent event) {
		String signature = selectorSignature(event);
		String text = Boolean.TRUE.equals(event.getIsPassword()) ? "" : nullToEmpty(event.getText());
		PlaywrightStep previous = session.getPendingTypeStep(signature);
		if (previous == null) {
			previous = session.getPendingTypeStep();
		}

		if (previous != null) {
			PlaywrightStep updated = new PlaywrightStep(previous.id(), previous.type(), previous.url(),
					coordsOrPrevious(event, previous), previous.multiCoords(), previous.prompt(),
					nullToEmpty(previous.text()) + text, previous.pressEnter(), previous.deltaY(), previous.waitUntil(),
					previous.waitAfterMs(), viewport(session, event), previous.timestamp(), label(event, previous),
					description(event, previous), previous.isPassword(), storeValue(event, previous),
					selectorOrPrevious(event, previous), previous.isTriggerNewTab(), shouldRun(event), required(event),
					sendToPlayground(event), tag(event, previous));
			session.replaceLastRemoteBrowserRecordedStep(DEFAULT_TAB_ID, updated);
			session.setPendingTypeStep(signature, updated);
			return;
		}

		PlaywrightStep step = buildStep(session, event, PlaywrightStepType.TYPE, null, coords(event), text, null, null);
		PlaywrightStep appended = appendStep(session, step, false);
		session.setPendingTypeStep(signature, appended);
		addLegacyStep(session, event);
	}

	private static void recordKey(RemoteBrowserSession session, RemoteBrowserInputEvent event) {
		if (isModifierKey(event.getKey())) {
			return;
		}
		String signature = selectorSignature(event);
		PlaywrightStep previous = session.getPendingTypeStep(signature);
		if (previous == null) {
			previous = session.getPendingTypeStep();
		}
		if (previous != null && isTextDeletingKey(event.getKey())) {
			String currentValue = RemoteBrowserSelectorService.focusedValueIfMatches(session.getPage(),
					selectorOrPrevious(event, previous));
			if (currentValue != null) {
				PlaywrightStep updated = withText(previous,
						Boolean.TRUE.equals(event.getIsPassword()) ? "" : currentValue, event);
				session.replaceLastRemoteBrowserRecordedStep(DEFAULT_TAB_ID, updated);
				session.setPendingTypeStep(selectorSignatureForStep(updated), updated);
			}
			return;
		}
		if (previous != null && "Enter".equalsIgnoreCase(event.getKey())) {
			PlaywrightStep updated = new PlaywrightStep(previous.id(), previous.type(), previous.url(),
					previous.coords(), previous.multiCoords(), previous.prompt(), previous.text(), true,
					previous.deltaY(), previous.waitUntil(), previous.waitAfterMs(), previous.viewport(),
					previous.timestamp(), previous.label(), previous.description(), previous.isPassword(),
					previous.storeValue(), previous.selector(), previous.isTriggerNewTab(), previous.shouldRun(),
					previous.required(), previous.sendToPlayground(), previous.tag());
			session.replaceLastRemoteBrowserRecordedStep(DEFAULT_TAB_ID, updated);
			session.setPendingTypeStep(selectorSignature(event), updated);
			return;
		}
		session.clearPendingTypeStep();
	}

	private static boolean isTextDeletingKey(String key) {
		return "Backspace".equalsIgnoreCase(key) || "Delete".equalsIgnoreCase(key);
	}

	private static PlaywrightStep withText(PlaywrightStep previous, String text, RemoteBrowserInputEvent event) {
		return new PlaywrightStep(previous.id(), previous.type(), previous.url(), coordsOrPrevious(event, previous),
				previous.multiCoords(), previous.prompt(), text, previous.pressEnter(), previous.deltaY(),
				previous.waitUntil(), previous.waitAfterMs(), previous.viewport(), previous.timestamp(),
				label(event, previous), description(event, previous), previous.isPassword(),
				storeValue(event, previous), selectorOrPrevious(event, previous), previous.isTriggerNewTab(),
				shouldRun(event), required(event), sendToPlayground(event), tag(event, previous));
	}

	private static boolean isModifierKey(String key) {
		return "Shift".equals(key) || "Control".equals(key) || "Alt".equals(key) || "Meta".equals(key);
	}

	private static PlaywrightStep appendStep(RemoteBrowserSession session, PlaywrightStep step, boolean startNewPage) {
		boolean resolvedStartNewPage = startNewPage || session.consumeNextRemoteBrowserRecordedStepStartsNewPage();
		return session.appendRemoteBrowserRecordedStep(DEFAULT_TAB_ID, step, resolvedStartNewPage);
	}

	private static PlaywrightStep buildStep(RemoteBrowserSession session, RemoteBrowserInputEvent event,
			PlaywrightStepType type, String url, Coords coords, String text, Integer waitAfterMs, Integer deltaY) {
		boolean isPassword = Boolean.TRUE.equals(event.getIsPassword());
		boolean storeValue = Boolean.TRUE.equals(event.getStoreValue()) && !isPassword;
		return new PlaywrightStep(0, type, url, coords, null, null, text, null, deltaY, event.getWaitUntil(),
				waitAfterMs, viewport(session, event), System.currentTimeMillis(), event.getLabel(),
				event.getDescription(), isPassword, storeValue, event.getSelector(), null, shouldRun(event),
				required(event), sendToPlayground(event), event.getTag());
	}

	private static boolean isRecordingControl(RemoteBrowserInputEvent event) {
		return "recording".equals(event.getType()) || "recording-control".equals(event.getType());
	}

	private static boolean shouldRecord(RemoteBrowserSession session, RemoteBrowserInputEvent event) {
		return event.getRecord() != null ? event.getRecord().booleanValue() : session.isRecordingEnabled();
	}

	private static Integer waitAfter(RemoteBrowserInputEvent event) {
		return event.getWaitAfterMs() != null ? event.getWaitAfterMs() : DEFAULT_CLICK_WAIT_MS;
	}

	private static Viewport viewport(RemoteBrowserSession session, RemoteBrowserInputEvent event) {
		int width = event != null && event.getRecordedViewportWidth() != null && event.getRecordedViewportWidth() > 0
				? event.getRecordedViewportWidth()
				: session.getViewportWidth();
		int height = event != null && event.getRecordedViewportHeight() != null && event.getRecordedViewportHeight() > 0
				? event.getRecordedViewportHeight()
				: session.getViewportHeight();
		return new Viewport(width, height, DEVICE_SCALE_FACTOR);
	}

	private static Coords coords(RemoteBrowserInputEvent event) {
		if (event.getX() == null || event.getY() == null) {
			return null;
		}
		return new Coords((int) Math.round(event.getX()), (int) Math.round(event.getY()));
	}

	private static Integer toInteger(Double value) {
		return value == null ? null : Integer.valueOf((int) Math.round(value));
	}

	private static String selectorSignature(RemoteBrowserInputEvent event) {
		Selector selector = event.getSelector();
		if (selector != null && selector.value() != null && !selector.value().isBlank()) {
			return String.join("|", nullToEmpty(selector.strategy()), selector.value(),
					nullToEmpty(selector.frameSelector()));
		}
		Coords c = coords(event);
		if (c != null) {
			return "coords|" + c.x() + "|" + c.y();
		}
		return "unknown";
	}

	private static String selectorSignatureForStep(PlaywrightStep step) {
		if (step == null) {
			return "unknown";
		}
		Selector selector = step.selector();
		if (selector != null && selector.value() != null && !selector.value().isBlank()) {
			return String.join("|", nullToEmpty(selector.strategy()), selector.value(),
					nullToEmpty(selector.frameSelector()));
		}
		Coords c = step.coords();
		if (c != null) {
			return "coords|" + c.x() + "|" + c.y();
		}
		return "unknown";
	}

	private static String nullToEmpty(String value) {
		return value == null ? "" : value;
	}

	private static Coords coordsOrPrevious(RemoteBrowserInputEvent event, PlaywrightStep previous) {
		Coords c = coords(event);
		return c != null ? c : previous.coords();
	}

	private static Selector selectorOrPrevious(RemoteBrowserInputEvent event, PlaywrightStep previous) {
		return event.getSelector() != null ? event.getSelector() : previous.selector();
	}

	private static String label(RemoteBrowserInputEvent event, PlaywrightStep previous) {
		return event.getLabel() != null ? event.getLabel() : previous.label();
	}

	private static String description(RemoteBrowserInputEvent event, PlaywrightStep previous) {
		return event.getDescription() != null ? event.getDescription() : previous.description();
	}

	private static boolean storeValue(RemoteBrowserInputEvent event, PlaywrightStep previous) {
		return event.getStoreValue() != null ? Boolean.TRUE.equals(event.getStoreValue()) : previous.storeValue();
	}

	private static String tag(RemoteBrowserInputEvent event, PlaywrightStep previous) {
		return event.getTag() != null ? event.getTag() : previous.tag();
	}

	private static Boolean shouldRun(RemoteBrowserInputEvent event) {
		return event.getShouldRun() != null ? event.getShouldRun() : Boolean.TRUE;
	}

	private static Boolean required(RemoteBrowserInputEvent event) {
		return event.getRequired() != null ? event.getRequired() : Boolean.FALSE;
	}

	private static Boolean sendToPlayground(RemoteBrowserInputEvent event) {
		return event.getSendToPlayground();
	}

	private static void addLegacyStep(RemoteBrowserSession session, RemoteBrowserInputEvent event) {
		RemoteBrowserRecordedStep step = new RemoteBrowserRecordedStep().type(event.getType()).url(safeUrl(session))
				.viewport(session.getViewportWidth(), session.getViewportHeight())
				.timestamp(System.currentTimeMillis());
		if (event.getX() != null && event.getY() != null) {
			step.coordinates(event.getX(), event.getY());
		}
		if (event.getSelector() != null) {
			step.selector(event.getSelector().value());
			step.role(event.getSelector().strategy());
		}
		if (Objects.equals(event.getType(), "type-text")) {
			step.text(Boolean.TRUE.equals(event.getIsPassword()) ? "" : event.getText());
		}
		session.getRemoteBrowserRecordedSteps().add(step);
	}

	private static String safeUrl(RemoteBrowserSession session) {
		try {
			return session.getPage().url();
		} catch (Exception e) {
			classLogger.warn("Unable to read current URL for session {}; recording step with empty URL",
					session.getSessionId(), e);
			return "";
		}
	}
}
