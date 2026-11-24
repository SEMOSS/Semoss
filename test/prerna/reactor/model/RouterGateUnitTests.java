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
package prerna.reactor.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import prerna.reactor.model.RouterGate.Paired;

/** The noise-based decisions: a difference counts only beyond its own standard error. */
class RouterGateUnitTests {

	@Test
	void pairedMeanAndStandardErrorOfTheDifferences() {
		Paired p = Paired.of(new double[] { 1, 2, 3, 4 });
		assertEquals(2.5, p.mean(), 1e-9);
		assertEquals(Math.sqrt(1.6667 / 4), p.se(), 1e-3);
		assertEquals(0, Paired.of(new double[] { -2, -2, -2 }).se(), 1e-12);
	}

	@Test
	void aQualityLossBeyondTheLimitRefusesHoweverNoisyTheMeasurement() {
		Paired cheaper = new Paired(-10, 1);
		assertTrue(RouterGate.refusal(10, new Paired(-0.20, 0.13), new Paired(-90, 1), 0.05).contains("needs 10 more tasks"));
		assertTrue(RouterGate.refusal(24, new Paired(-0.20, 0.13), cheaper, 0.05).contains("20 points lower"), "a loss not provable as real still refuses");
		assertNull(RouterGate.refusal(24, new Paired(-0.03, 0.13), cheaper, 0.05), "an explicit legacy loss allowance remains supported");
		assertTrue(RouterGate.refusal(24, new Paired(-0.03, 0.13), cheaper, 0.0).contains("3 points lower"), "the default does not accept an observed decline");
		assertNull(RouterGate.refusal(24, new Paired(0.0, 0.13), cheaper, 0.0), "no observed decline is acceptable");
		assertTrue(RouterGate.refusal(24, new Paired(-0.06, 0.01), cheaper, 0.05).contains("6 points lower"));
		assertNull(RouterGate.refusal(24, new Paired(-0.2, 0.01), cheaper, 0.25));
	}

	@Test
	void aSavingOrAnImprovementCountsOnlyBeyondNoise() {
		assertTrue(RouterGate.refusal(24, new Paired(0, 0.05), new Paired(-1, 1), 0.05).contains("neither cheaper nor better"), "a saving within the noise is not a saving");
		assertNull(RouterGate.refusal(24, new Paired(0.1, 0.02), new Paired(2, 5), 0.05), "better at a cost not higher beyond noise");
		assertTrue(RouterGate.refusal(24, new Paired(0.1, 0.02), new Paired(20, 5), 0.05).contains("neither cheaper nor better"));
		assertTrue(RouterGate.refusal(24, new Paired(0.01, 0.2), new Paired(-50, 100), 0.05).contains("neither cheaper nor better"));
	}

	@Test
	void atLeastTwentyTasksAreNeeded() {
		assertTrue(RouterGate.refusal(19, new Paired(0, 0), new Paired(-50, 0), 0.05).contains("needs 1 more tasks"));
		assertNull(RouterGate.refusal(20, new Paired(0, 0), new Paired(-50, 0), 0.05));
	}

}
