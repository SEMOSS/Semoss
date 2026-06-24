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

/**
 * Represents an input event sent from the React frontend to the Java backend
 * over the WebSocket connection.
 *
 * Supported types: mouse-click, mouse-move, mouse-down, mouse-up,
 *                  wheel, type-text, key, navigate, close-session
 */
public class BrowserInputEvent {

	// ---- common fields ----
	private String type;

	// ---- mouse / wheel ----
	private Double x;
	private Double y;
	private String button;   // "left" | "right" | "middle"
	private Double deltaX;
	private Double deltaY;

	// ---- keyboard ----
	private String text;
	private String key;
	private String code;
	private Map<String, Boolean> modifiers; // alt, ctrl, meta, shift

	// ---- navigate ----
	private String url;

	// ---- getters & setters ----

	public String getType() { return type; }
	public void setType(String type) { this.type = type; }

	public Double getX() { return x; }
	public void setX(Double x) { this.x = x; }

	public Double getY() { return y; }
	public void setY(Double y) { this.y = y; }

	public String getButton() { return button; }
	public void setButton(String button) { this.button = button; }

	public Double getDeltaX() { return deltaX; }
	public void setDeltaX(Double deltaX) { this.deltaX = deltaX; }

	public Double getDeltaY() { return deltaY; }
	public void setDeltaY(Double deltaY) { this.deltaY = deltaY; }

	public String getText() { return text; }
	public void setText(String text) { this.text = text; }

	public String getKey() { return key; }
	public void setKey(String key) { this.key = key; }

	public String getCode() { return code; }
	public void setCode(String code) { this.code = code; }

	public Map<String, Boolean> getModifiers() { return modifiers; }
	public void setModifiers(Map<String, Boolean> modifiers) { this.modifiers = modifiers; }

	public String getUrl() { return url; }
	public void setUrl(String url) { this.url = url; }
}
