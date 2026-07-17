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

import prerna.reactor.playwright.Viewport;

/**
 * A recorded interaction step captured during a remote browser session. Steps
 * are stored in memory and can be persisted for later replay.
 */
public class RemoteBrowserRecordedStep {

	/**
	 * Device scale factor for remote browser sessions (see RemoteBrowserSessionManager).
	 */
	private static final double DEVICE_SCALE_FACTOR = 1.0;

	private String type;
	private String url;
	private String selector;
	private String text;
	private String role;
	private Integer deltaY;
	private CoordinatesInfo coordinates;
	private Viewport viewport;
	private long timestamp;

	public static class CoordinatesInfo {
		private double x;
		private double y;

		public CoordinatesInfo(double x, double y) {
			this.x = x;
			this.y = y;
		}

		public double getX() {
			return x;
		}

		public double getY() {
			return y;
		}
	}

	// Builder-style setters
	public RemoteBrowserRecordedStep type(String type) {
		this.type = type;
		return this;
	}

	public RemoteBrowserRecordedStep url(String url) {
		this.url = url;
		return this;
	}

	public RemoteBrowserRecordedStep selector(String selector) {
		this.selector = selector;
		return this;
	}

	public RemoteBrowserRecordedStep text(String text) {
		this.text = text;
		return this;
	}

	public RemoteBrowserRecordedStep role(String role) {
		this.role = role;
		return this;
	}

	public RemoteBrowserRecordedStep deltaY(Integer deltaY) {
		this.deltaY = deltaY;
		return this;
	}

	public RemoteBrowserRecordedStep coordinates(double x, double y) {
		this.coordinates = new CoordinatesInfo(x, y);
		return this;
	}

	public RemoteBrowserRecordedStep viewport(int width, int height) {
		this.viewport = new Viewport(width, height, DEVICE_SCALE_FACTOR);
		return this;
	}

	public RemoteBrowserRecordedStep timestamp(long ts) {
		this.timestamp = ts;
		return this;
	}

	public String getType() {
		return type;
	}

	public String getUrl() {
		return url;
	}

	public String getSelector() {
		return selector;
	}

	public String getText() {
		return text;
	}

	public String getRole() {
		return role;
	}

	public Integer getDeltaY() {
		return deltaY;
	}

	public CoordinatesInfo getCoordinates() {
		return coordinates;
	}

	public Viewport getViewport() {
		return viewport;
	}

	public long getTimestamp() {
		return timestamp;
	}
}
