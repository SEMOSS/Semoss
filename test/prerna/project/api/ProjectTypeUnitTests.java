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
package prerna.project.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Properties;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import prerna.util.Constants;

class ProjectTypeUnitTests {

	@Test
	void testHoldsInsights_onlyInsights() {
		assertTrue(IProject.PROJECT_TYPE.INSIGHTS.holdsInsights());
	}

	@ParameterizedTest
	@EnumSource(value = IProject.PROJECT_TYPE.class, names = "INSIGHTS", mode = EnumSource.Mode.EXCLUDE)
	void testHoldsInsights_otherTypesDoNot(IProject.PROJECT_TYPE projectType) {
		assertFalse(projectType.holdsInsights());
	}

	@Test
	void testFromSmss_unsetIsInsights() {
		assertEquals(IProject.PROJECT_TYPE.INSIGHTS, IProject.PROJECT_TYPE.fromSmss(new Properties()));
	}

	@ParameterizedTest
	@ValueSource(strings = { "", "  " })
	void testFromSmss_blankIsInsights(String projectType) {
		assertEquals(IProject.PROJECT_TYPE.INSIGHTS, IProject.PROJECT_TYPE.fromSmss(smss(projectType)));
	}

	@Test
	void testFromSmss_readsTheTypeIgnoringSpaces() {
		assertEquals(IProject.PROJECT_TYPE.BLOCKS, IProject.PROJECT_TYPE.fromSmss(smss(" BLOCKS ")));
	}

	@Test
	void testFromSmss_unknownTypeThrows() {
		assertThrows(IllegalArgumentException.class, () -> IProject.PROJECT_TYPE.fromSmss(smss("APP")));
	}

	private static Properties smss(String projectType) {
		Properties smssProp = new Properties();
		smssProp.setProperty(Constants.PROJECT_ENUM_TYPE, projectType);
		return smssProp;
	}
}
