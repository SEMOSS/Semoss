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
package prerna.engine.impl.guardrail;

/**
 * What a judge guardrail does with the guarded call when its judge gives no
 * usable answer, such as when the judge fails or is not confident enough: block
 * the call, or let it pass. Settings such as {@code JUDGE_ERROR_VERDICT} and
 * {@code LOW_CONFIDENCE_VERDICT} hold one of these.
 */
public enum GuardrailVerdict {

	BLOCK, PASS;

	/**
	 * Reads a verdict setting, ignoring case and surrounding spaces.
	 *
	 * @param value  the setting's value
	 * @param source the setting's name, for the error
	 * @return the verdict
	 * @throws IllegalArgumentException when the value is not BLOCK or PASS
	 */
	public static GuardrailVerdict parse(String value, String source) {
		String normalized = value == null ? "" : value.trim().toUpperCase();
		for (GuardrailVerdict verdict : values()) {
			if (verdict.name().equals(normalized)) {
				return verdict;
			}
		}
		throw new IllegalArgumentException(source + " must be BLOCK or PASS, got '" + value + "'");
	}

	/**
	 * Whether this verdict lets the guarded call through.
	 *
	 * @return true for PASS
	 */
	public boolean passes() {
		return this == PASS;
	}
}
