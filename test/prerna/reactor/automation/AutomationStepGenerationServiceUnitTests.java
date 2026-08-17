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
package prerna.reactor.automation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

class AutomationStepGenerationServiceUnitTests {

	@Test
	void createsStableHashesForEquivalentSetup() {
		Map<String, Object> firstConfig = new LinkedHashMap<>();
		firstConfig.put(AutomationConstants.CONFIG_PIXEL, "Echo(message=[\"hello\"]);");
		firstConfig.put(AutomationConstants.CONFIG_OPERATION, AutomationConstants.OP_RUN_PIXEL);

		Map<String, Object> secondConfig = new LinkedHashMap<>();
		secondConfig.put(AutomationConstants.CONFIG_OPERATION, AutomationConstants.OP_RUN_PIXEL);
		secondConfig.put(AutomationConstants.CONFIG_PIXEL, "Echo(message=[\"hello\"]);");

		AutomationStepGenerationService.GeneratedStep first =
				AutomationStepGenerationService.generate(AutomationConstants.NODE_APP, firstConfig);
		AutomationStepGenerationService.GeneratedStep second =
				AutomationStepGenerationService.generate(AutomationConstants.NODE_APP, secondConfig);

		assertEquals(first.getSetupHash(), second.getSetupHash());
		assertEquals(first.getSourceHash(), second.getSourceHash());
		assertEquals(AutomationStepGenerationService.sha256(first.getSource()), first.getSourceHash());
		assertEquals(AutomationStepGenerationService.TEMPLATE_VERSION, first.getTemplateVersion());
	}

	@Test
	void previewsOnlyReportsChangesWhenGeneratedSourceDiffers() {
		AutomationStepGenerationService.GeneratedStep current =
				generateAppStep("Echo(message=[\"before\"]);");
		AutomationStepGenerationService.GeneratedStep proposed =
				generateAppStep("Echo(message=[\"after\"]);");

		AutomationStepGenerationService.Preview changed =
				AutomationStepGenerationService.preview(current.getSource(), proposed);
		AutomationStepGenerationService.Preview unchanged =
				AutomationStepGenerationService.preview(current.getSource(), current);

		assertTrue(changed.isChanged());
		assertEquals(current.getSourceHash(), changed.getCurrentSourceHash());
		assertEquals(proposed.getSourceHash(), changed.getProposed().getSourceHash());
		assertNotEquals(changed.getCurrentSource(), changed.getProposed().getSource());
		assertFalse(unchanged.isChanged());
	}

	@Test
	void hashesSourceWithSha256() {
		assertEquals("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
				AutomationStepGenerationService.sha256("abc"));
	}

	private static AutomationStepGenerationService.GeneratedStep generateAppStep(String pixel) {
		return AutomationStepGenerationService.generate(AutomationConstants.NODE_APP, Map.of(
				AutomationConstants.CONFIG_OPERATION, AutomationConstants.OP_RUN_PIXEL,
				AutomationConstants.CONFIG_PIXEL, pixel));
	}
}
