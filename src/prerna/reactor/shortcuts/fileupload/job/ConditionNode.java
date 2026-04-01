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

public class ConditionNode {

	private final String id;
	private final String source;
	private final String key;
	private final String operator;
	private final String value;
	private final String onTrue;
	private final String onFalse;

	public ConditionNode(String id, String source, String key, String operator, String value, String onTrue,
			String onFalse) {
		this.id = id;
		this.source = source;
		this.key = key;
		this.operator = operator;
		this.value = value;
		this.onTrue = onTrue;
		this.onFalse = onFalse;
	}

	/*
	 * @Override public String id() { return id; }
	 */

	public String getOnTrue() {
		return onTrue;
	}

	public String getOnFalse() {
		return onFalse;
	}

	/*
	 * @Override public String execute(ExecutionContext ctx) {
	 * 
	 * Object actual; ExtractionResult extractionResult = (ExtractionResult)
	 * ctx.result.get("result"); switch (source) { case "RESULT" -> actual =
	 * extractionResult.fileType; case "INPUT" -> actual = ctx.input.get(key); case
	 * "META" -> actual = ctx.meta.get(key); default -> throw new
	 * IllegalStateException("Unknown condition source"); }
	 * 
	 * boolean result = actual != null && "EQUALS".equals(operator) &&
	 * actual.toString().equals(value);
	 * 
	 * return result ? onTrue : onFalse; }
	 */

}
