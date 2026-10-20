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
package prerna.auth.access;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.auth.access.ModelUsageLimitPath.Layer;
import prerna.auth.access.ModelUsageLimitPath.Reason;
import prerna.auth.access.ModelUsageLimitPath.Step;
import prerna.auth.access.ModelUsageLimitPath.StepLimitType;
import prerna.auth.access.ModelUsageLimitRule.Counts;
import prerna.auth.access.ModelUsageLimitRule.Metric;
import prerna.auth.access.ModelUsageLimitRule.Window;
import prerna.engine.impl.model.usage.ModelUsagePathView;

/**
 * How a usage limit path is written for the path view, without usage: in full
 * for the person and admins, and with the limits that count use on other models
 * left out for the model's editors and owners.
 */
class ModelUsagePathViewUnitTests {

	private static final String MODEL = "model";

	@Test
	void toMap_givesEveryStepsRulesAndEachTeamsTotal() {
		Map<String, Object> view = ModelUsagePathView.toMap(path(), false);

		Map<String, Object> allModels = step(view, StepLimitType.ALL_MODELS);
		assertEquals(1, list(allModels.get("rules")).size());
		assertFalse(allModels.containsKey("rulesHidden"));
		assertEquals(1, list(step(view, StepLimitType.PLATFORM).get("rules")).size());
		Map<String, Object> teamTotal = map(step(view, StepLimitType.TEAM).get("teamTotal"));
		assertEquals(1, list(teamTotal.get("rules")).size());
	}

	@Test
	void toMap_hidingOtherModelsLeavesOutTheirRulesAndEachTeamsTotal() {
		Map<String, Object> view = ModelUsagePathView.toMap(path(), false, true);

		for (StepLimitType limitType : List.of(StepLimitType.ALL_MODELS, StepLimitType.PLATFORM)) {
			Map<String, Object> step = step(view, limitType);
			assertFalse(step.containsKey("rules"), limitType.name());
			assertEquals(true, step.get("rulesHidden"), limitType.name());
			assertEquals("DEFINED", step.get("state"), limitType.name());
		}
		Map<String, Object> team = step(view, StepLimitType.TEAM);
		assertFalse(team.containsKey("teamTotal"));
		assertEquals(1, list(team.get("rules")).size());
		// the limits on this model stay as they are
		assertFalse(step(view, StepLimitType.DIRECT).containsKey("rulesHidden"));
		assertTrue(step(view, StepLimitType.DIRECT).containsKey("rules"));
		Map<String, Object> ceiling = list(view.get("ceilings")).get(0);
		assertEquals(1, list(ceiling.get("rules")).size());
	}

	@Test
	void toMap_withoutHidingIsTheSameAsTheTwoArgumentForm() {
		assertEquals(ModelUsagePathView.toMap(path(), false), ModelUsagePathView.toMap(path(), false, false));
	}

	private static ModelUsageLimitPath path() {
		ModelUsageLimitPath path = new ModelUsageLimitPath(MODEL, "jdoe", "NATIVE");
		path.addCeiling(new Step(Layer.CEILING, StepLimitType.MODEL_TOTAL, ModelUsageLimitKey.modelTotal(MODEL),
				limit(1_000_000L, Counts.SHARED)));
		path.addStep(
				new Step(Layer.BILLED, StepLimitType.DIRECT, ModelUsageLimitKey.direct(MODEL, "jdoe", null), null));
		path.addStep(new Step(Layer.BILLED, StepLimitType.TEAM, ModelUsageLimitKey.team(MODEL, "data", "CUSTOM"),
				limit(500_000L, Counts.SHARED))
				.withTeamTotal(ModelUsageLimitKey.teamTotal("data", "CUSTOM"), limit(9_000_000L, Counts.SHARED)));
		path.addStep(new Step(Layer.BILLED, StepLimitType.PUBLIC, ModelUsageLimitKey.publicUse(MODEL), null)
				.skipped(Reason.NOT_PUBLIC));
		path.addStep(new Step(Layer.BILLED, StepLimitType.ALL_MODELS, ModelUsageLimitKey.allModels("jdoe", "NATIVE"),
				limit(70_000L, Counts.EACH_PERSON)));
		path.addStep(new Step(Layer.BILLED, StepLimitType.PLATFORM, ModelUsageLimitKey.platform(),
				limit(30_000L, Counts.EACH_PERSON)));
		return path;
	}

	private static ModelUsageLimit limit(long amount, Counts counts) {
		return ModelUsageLimit.of(List.of(new ModelUsageLimitRule(Metric.TOKENS, amount, Window.DAY, counts)));
	}

	private static Map<String, Object> step(Map<String, Object> view, StepLimitType limitType) {
		return list(view.get("steps")).stream().filter(step -> limitType.name().equals(step.get("limitType")))
				.findFirst().orElseThrow();
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> map(Object value) {
		return (Map<String, Object>) value;
	}

	@SuppressWarnings("unchecked")
	private static List<Map<String, Object>> list(Object value) {
		return (List<Map<String, Object>>) value;
	}
}
