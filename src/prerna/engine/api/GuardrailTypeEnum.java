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

import prerna.engine.impl.guardrail.DetoxifyGuardrailEngine;
import prerna.engine.impl.guardrail.GLiNERGuardrailEngine;
import prerna.engine.impl.guardrail.LocalPythonGuardrailReactorFunctionEngine;
import prerna.engine.impl.guardrail.OnTopicGuardrailEngine;
import prerna.engine.impl.guardrail.PromptInjectionGuardrailEngine;

public enum GuardrailTypeEnum {

	EMBEDDED_DETOXIFY("EMBEDDED_DETOXIFY", DetoxifyGuardrailEngine.class.getName()),
	EMBEDDED_GLINER("EMBEDDED_GLINER", GLiNERGuardrailEngine.class.getName()),
	EMBEDDED_ON_TOPIC("EMBEDDED_ON_TOPIC", OnTopicGuardrailEngine.class.getName()),
	EMBEDDED_PROMPT_INJECTION("EMBEDDED_PROMPT_INJECTION", PromptInjectionGuardrailEngine.class.getName()),
	LOCAL_PYTHON("LOCAL_PYTHON", LocalPythonGuardrailReactorFunctionEngine.class.getName());

	private String guardrailName;
	private String guardrailClass;

	GuardrailTypeEnum(String guardrailName, String guardrailClass) {
		this.guardrailName = guardrailName;
		this.guardrailClass = guardrailClass;
	}

	/**
	 * 
	 * @return
	 */
	public String getGuardrailClass() {
		return this.guardrailClass;
	}

	/**
	 * 
	 * @return
	 */
	public String getGuardrailName() {
		return this.guardrailName;
	}

	/**
	 * 
	 * @param name
	 * @return
	 */
	public static GuardrailTypeEnum getEnumFromName(String name) {
		GuardrailTypeEnum[] allValues = values();
		for (GuardrailTypeEnum v : allValues) {
			if (v.getGuardrailName().equalsIgnoreCase(name)) {
				return v;
			}
		}
		throw new IllegalArgumentException("Invalid input for name " + name);
	}
}
