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
package prerna.reactor.shortcuts.fileupload.job;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class WorkflowActionResult implements Serializable {
	private static final long serialVersionUID = 1L;

	public String status; // SUCCESS, FAILED, RETRY
	public String message; // SUCCESS, FAILED, RETRY

	public Map<String, Object> result; // business output
	public Map<String, Object> meta; // metrics
	public Map<String, Object> error; // error details
	public Map<String, Object> nextNodeOverride; // nextNodeOverride, retry

	public WorkflowActionResult() {
		this.status = "SUCCESS";
		this.result = new HashMap<>();
		this.meta = new HashMap<>();
		this.error = new HashMap<>();
		this.nextNodeOverride = new HashMap<>();
	}

	// -------------------------
	// Status
	// -------------------------

	public WorkflowActionResult success() {
		this.status = "SUCCESS";
		return this;
	}

	public WorkflowActionResult failed(String message) {
		this.status = "FAILED";
		this.error.put("message", message);
		return this;
	}

	public WorkflowActionResult retry(String message) {
		this.status = "RETRY";
		this.error.put("message", message);
		this.nextNodeOverride.put("retry", true);
		return this;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

}
