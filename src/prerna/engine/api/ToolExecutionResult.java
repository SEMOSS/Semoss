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
package prerna.engine.api;

/**
 * Transport-neutral result of executing a tool.
 *
 * <p>The output remains an object so existing direct MCP callers can preserve
 * their payload. The status is carried separately so harness code never has to
 * infer failure from a string prefix or from domain-specific JSON.
 */
public final class ToolExecutionResult {

	public enum Status {
		SUCCESS("success"), ERROR("error"), CANCELLED("cancelled");

		private final String value;

		Status(String value) {
			this.value = value;
		}

		public String getValue() {
			return this.value;
		}
	}

	private final Object output;
	private final Status status;
	private final String error;

	private ToolExecutionResult(Object output, Status status, String error) {
		this.output = output;
		this.status = status;
		this.error = error;
	}

	public static ToolExecutionResult success(Object output) {
		return new ToolExecutionResult(output, Status.SUCCESS, null);
	}

	public static ToolExecutionResult error(Object output, String error) {
		return new ToolExecutionResult(output, Status.ERROR, error);
	}

	public static ToolExecutionResult cancelled(Object output) {
		return new ToolExecutionResult(output, Status.CANCELLED, null);
	}

	public Object getOutput() {
		return this.output;
	}

	public Status getStatus() {
		return this.status;
	}

	public String getStatusValue() {
		return this.status.getValue();
	}

	public String getError() {
		return this.error;
	}

	public boolean isSuccess() {
		return this.status == Status.SUCCESS;
	}
}
