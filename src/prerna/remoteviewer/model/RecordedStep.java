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

/**
 * A recorded interaction step captured during a remote browser session.
 * Steps are stored in memory and can be persisted for later replay.
 */
public class RecordedStep {

	private String type;
	private String url;
	private String selector;
	private String text;
	private String role;
	private CoordinatesInfo coordinates;
	private ViewportInfo viewport;
	private long timestamp;

	public static class CoordinatesInfo {
		private double x;
		private double y;

		public CoordinatesInfo(double x, double y) {
			this.x = x;
			this.y = y;
		}

		public double getX() { return x; }
		public double getY() { return y; }
	}

	public static class ViewportInfo {
		private int width;
		private int height;

		public ViewportInfo(int width, int height) {
			this.width = width;
			this.height = height;
		}

		public int getWidth() { return width; }
		public int getHeight() { return height; }
	}

	// Builder-style setters
	public RecordedStep type(String type) { this.type = type; return this; }
	public RecordedStep url(String url) { this.url = url; return this; }
	public RecordedStep selector(String selector) { this.selector = selector; return this; }
	public RecordedStep text(String text) { this.text = text; return this; }
	public RecordedStep role(String role) { this.role = role; return this; }
	public RecordedStep coordinates(double x, double y) { this.coordinates = new CoordinatesInfo(x, y); return this; }
	public RecordedStep viewport(int width, int height) { this.viewport = new ViewportInfo(width, height); return this; }
	public RecordedStep timestamp(long ts) { this.timestamp = ts; return this; }

	public String getType() { return type; }
	public String getUrl() { return url; }
	public String getSelector() { return selector; }
	public String getText() { return text; }
	public String getRole() { return role; }
	public CoordinatesInfo getCoordinates() { return coordinates; }
	public ViewportInfo getViewport() { return viewport; }
	public long getTimestamp() { return timestamp; }
}
