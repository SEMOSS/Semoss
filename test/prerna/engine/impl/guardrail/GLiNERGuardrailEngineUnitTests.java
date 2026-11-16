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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class GLiNERGuardrailEngineUnitTests {

	private static Map<String, Object> entity(int start, int end, String label) {
		return Map.of("start", start, "end", end, "label", label);
	}

	private static String mask(String prompt, List<Map<String, Object>> flagged) throws Exception {
		Method buildMaskedPrompt = GLiNERGuardrailEngine.class.getDeclaredMethod("buildMaskedPrompt", String.class,
				List.class);
		buildMaskedPrompt.setAccessible(true);
		return (String) buildMaskedPrompt.invoke(new GLiNERGuardrailEngine(), prompt, flagged);
	}

	@Test
	void masksEverySeparateSpan() throws Exception {
		assertEquals("[person] emailed [person]",
				mask("Ada emailed Bob", List.of(entity(12, 15, "person"), entity(0, 3, "person"))));
	}

	@Test
	void aSpanInsideAnotherIsMaskedWithItSoNoPartOfTheEntityStays() throws Exception {
		assertEquals("call [person] now",
				mask("call Adam now", List.of(entity(6, 8, "person"), entity(5, 9, "person"))));
	}

	@Test
	void spansThatPartlyOverlapAreMaskedAsOneSpan() throws Exception {
		assertEquals("[email] sent",
				mask("abcdefghij sent", List.of(entity(0, 5, "email"), entity(3, 10, "person"))));
	}

	@Test
	void aSpanWithUnusableOffsetsIsLeftOut() throws Exception {
		assertEquals("Ada emailed [person]", mask("Ada emailed Bob",
				List.of(entity(-1, 3, "person"), entity(12, 99, "person"), entity(12, 15, "person"))));
	}

	@Test
	void aPromptWithNothingFlaggedIsReturnedUnchanged() throws Exception {
		String prompt = "nothing to mask";
		assertSame(prompt, mask(prompt, List.of()));
	}
}
