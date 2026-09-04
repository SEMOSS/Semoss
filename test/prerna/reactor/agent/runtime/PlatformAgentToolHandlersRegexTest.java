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
package prerna.reactor.agent.runtime;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import com.google.re2j.Pattern;
import com.google.re2j.PatternSyntaxException;

class PlatformAgentToolHandlersRegexTest {

	@Test
	void supportsCommonGrepExpressions() {
		Pattern regex = PlatformAgentToolHandlers.compileGrepPattern("error|warn(?:ing)?", false);

		assertTrue(regex.matcher("warning: disk space is low").find());
		assertFalse(regex.matcher("all systems operational").find());
	}

	@Test
	void supportsCaseInsensitiveSearch() {
		Pattern regex = PlatformAgentToolHandlers.compileGrepPattern("failure", true);

		assertTrue(regex.matcher("BUILD FAILURE").find());
	}

	@Test
	void rejectsUnsupportedBacktrackingFeatures() {
		assertThrows(PatternSyntaxException.class,
				() -> PlatformAgentToolHandlers.compileGrepPattern("secret(?=value)", false));
	}

	@Test
	void evaluatesPathologicalNestedQuantifiersInLinearTime() {
		Pattern regex = PlatformAgentToolHandlers.compileGrepPattern("(a+)+$", false);
		String adversarialInput = "a".repeat(200_000) + "!";

		assertTimeoutPreemptively(Duration.ofSeconds(1),
				() -> assertFalse(regex.matcher(adversarialInput).find()));
	}
}
