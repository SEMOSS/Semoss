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
package prerna.remoteviewer.model;

import java.util.Map;

import prerna.reactor.playwright.Selector;

/**
 * Represents an input event sent from the React frontend to the Java backend
 * over the WebSocket connection.
 *
 * Supported types: mouse-click, mouse-move, mouse-down, mouse-up, wheel,
 * type-text, key, navigate, close-session
 */
public class RemoteBrowserInputEvent {

	// ---- common fields ----
	private String type;

	// ---- mouse / wheel ----
	private Double x;
	private Double y;
	private String button; // "left" | "right" | "middle"
	private Double deltaX;
	private Double deltaY;

	// ---- keyboard ----
	private String text;
	private String key;
	private String code;
	private Map<String, Boolean> modifiers; // alt, ctrl, meta, shift

	// ---- navigate ----
	private String url;

	// ---- optional enrichment from Chrome extension recording ----
	/** CSS/ID/role selector for the target element (preferred over raw coords). */
	private Selector selector;
	/** How long to wait after this action before the next one (ms). */
	private Integer waitAfterMs;
	/** Viewport width used when the action was recorded. */
	private Integer recordedViewportWidth;
	/** Viewport height used when the action was recorded. */
	private Integer recordedViewportHeight;
	/** Toggle future recording for this browser session. */
	private Boolean recording;
	/** Per-event recording override. */
	private Boolean record;
	/** Whether disabling recording should discard the unsaved temporary buffer. */
	private Boolean discard;
	/** Optional action label captured by the frontend recorder. */
	private String label;
	/** Optional action description captured by the frontend recorder. */
	private String description;
	/** Whether this event targets a password/sensitive field. */
	private Boolean isPassword;
	/** Whether a TYPE value should be stored in the replay JSON. */
	private Boolean storeValue;
	/** Optional HTML tag name of the target element. */
	private String tag;
	/** Optional replay navigation wait condition. */
	private String waitUntil;
	/** Optional replay shouldRun flag. */
	private Boolean shouldRun;
	/** Optional replay required flag. */
	private Boolean required;
	/** Optional replay playground flag. */
	private Boolean sendToPlayground;
	/** For switch-tab events: the tab ID to activate. */
	private String targetTabId;
	/** Internal source tab captured before dispatch; clients may omit it. */
	private String tabId;
	/** Internal popup tab detected while dispatching this event. */
	private String triggeredTabId;
	/** Optional client correlation ID for tab-control acknowledgements. */
	private String requestId;
	/** Whether playback should bind tab-1 to the active tab instead of a fresh tab. */
	private Boolean reuseActiveTab;
	/** Recorded child-tab ID expected from a replayed popup-triggering action. */
	private String replayTriggerTabId;

	// ---- getters & setters ----

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public Double getX() {
		return x;
	}

	public void setX(Double x) {
		this.x = x;
	}

	public Double getY() {
		return y;
	}

	public void setY(Double y) {
		this.y = y;
	}

	public String getButton() {
		return button;
	}

	public void setButton(String button) {
		this.button = button;
	}

	public Double getDeltaX() {
		return deltaX;
	}

	public void setDeltaX(Double deltaX) {
		this.deltaX = deltaX;
	}

	public Double getDeltaY() {
		return deltaY;
	}

	public void setDeltaY(Double deltaY) {
		this.deltaY = deltaY;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}

	public Map<String, Boolean> getModifiers() {
		return modifiers;
	}

	public void setModifiers(Map<String, Boolean> modifiers) {
		this.modifiers = modifiers;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public Selector getSelector() {
		return selector;
	}

	public void setSelector(Selector selector) {
		this.selector = selector;
	}

	public Integer getWaitAfterMs() {
		return waitAfterMs;
	}

	public void setWaitAfterMs(Integer waitAfterMs) {
		this.waitAfterMs = waitAfterMs;
	}

	public Integer getRecordedViewportWidth() {
		return recordedViewportWidth;
	}

	public void setRecordedViewportWidth(Integer recordedViewportWidth) {
		this.recordedViewportWidth = recordedViewportWidth;
	}

	public Integer getRecordedViewportHeight() {
		return recordedViewportHeight;
	}

	public void setRecordedViewportHeight(Integer recordedViewportHeight) {
		this.recordedViewportHeight = recordedViewportHeight;
	}

	public Boolean getRecording() {
		return recording;
	}

	public void setRecording(Boolean recording) {
		this.recording = recording;
	}

	public Boolean getRecord() {
		return record;
	}

	public void setRecord(Boolean record) {
		this.record = record;
	}

	public Boolean getDiscard() {
		return discard;
	}

	public void setDiscard(Boolean discard) {
		this.discard = discard;
	}

	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public Boolean getIsPassword() {
		return isPassword;
	}

	public void setIsPassword(Boolean isPassword) {
		this.isPassword = isPassword;
	}

	public Boolean getStoreValue() {
		return storeValue;
	}

	public void setStoreValue(Boolean storeValue) {
		this.storeValue = storeValue;
	}

	public String getTag() {
		return tag;
	}

	public void setTag(String tag) {
		this.tag = tag;
	}

	public String getWaitUntil() {
		return waitUntil;
	}

	public void setWaitUntil(String waitUntil) {
		this.waitUntil = waitUntil;
	}

	public Boolean getShouldRun() {
		return shouldRun;
	}

	public void setShouldRun(Boolean shouldRun) {
		this.shouldRun = shouldRun;
	}

	public Boolean getRequired() {
		return required;
	}

	public void setRequired(Boolean required) {
		this.required = required;
	}

	public Boolean getSendToPlayground() {
		return sendToPlayground;
	}

	public void setSendToPlayground(Boolean sendToPlayground) {
		this.sendToPlayground = sendToPlayground;
	}

	public String getTargetTabId() {
		return targetTabId;
	}

	public void setTargetTabId(String targetTabId) {
		this.targetTabId = targetTabId;
	}

	public String getTabId() {
		return tabId;
	}

	public void setTabId(String tabId) {
		this.tabId = tabId;
	}

	public String getTriggeredTabId() {
		return triggeredTabId;
	}

	public void setTriggeredTabId(String triggeredTabId) {
		this.triggeredTabId = triggeredTabId;
	}

	public String getRequestId() {
		return requestId;
	}

	public void setRequestId(String requestId) {
		this.requestId = requestId;
	}

	public Boolean getReuseActiveTab() {
		return reuseActiveTab;
	}

	public void setReuseActiveTab(Boolean reuseActiveTab) {
		this.reuseActiveTab = reuseActiveTab;
	}

	public String getReplayTriggerTabId() {
		return replayTriggerTabId;
	}

	public void setReplayTriggerTabId(String replayTriggerTabId) {
		this.replayTriggerTabId = replayTriggerTabId;
	}
}
