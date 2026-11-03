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
package prerna.engine.impl.model.usage;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.access.ModelUsageLimit;
import prerna.auth.access.ModelUsageLimitKey;
import prerna.auth.access.ModelUsageLimitPath;
import prerna.auth.access.ModelUsageLimitPath.State;
import prerna.auth.access.ModelUsageLimitPath.Step;
import prerna.auth.access.ModelUsageLimitPath.StepLimitType;
import prerna.auth.access.ModelUsageLimitRule;
import prerna.engine.impl.model.usage.ModelUsageEvaluation.RuleCheck;

/**
 * How limits apply to one person on one model, as the JSON the path view reads:
 *
 * <pre>
 * { "engineId": "...", "person": { "id": "...", "type": "..." },
 *   "billedLimitType": "DIRECT" | "TEAM" | "PUBLIC" | "ALL_MODELS" | "PLATFORM" | null,
 *   "ceilings": [ Step ], "steps": [ Step ] }
 *
 * Step = { "layer": "CEILING" | "BILLED",
 *          "limitType": "MODEL_TOTAL" | "DIRECT" | "TEAM" | "PUBLIC" | "ALL_MODELS" | "PLATFORM",
 *          "id": ... | null, "type": ... | null,
 *          "state": "DEFINED" | "NOT_DEFINED" | "SKIPPED" | "NOT_REACHED" | "CHECKED"
 *                   | "BILLED" | "NEXT" | "REACHED",
 *          "reason": "NOT_PUBLIC" | "NO_GRANT" | null,
 *          "accessSince": "2026-10-12T14:00:00Z" | null,
 *          "unlimited": bool,
 *          "rules": [ RuleUsage ],
 *          "rulesHidden": true, only when the rules are left out,
 *          "teamTotal": { "unlimited": bool, "rules": [ RuleUsage ] } | null }
 *
 * RuleUsage = { "metric", "amount", "window",
 *               "windowLength": minutes or hours | null for 1, "counts",
 *               "used": number | null, "resetsAt": "...Z" | null }
 * </pre>
 *
 * <p>
 * With usage, each step's state is set from what is used: a ceiling with usage
 * left is {@code CHECKED}, the source billed is {@code BILLED}, a team calls
 * move on to is {@code NEXT}, and a step with nothing left is {@code REACHED}.
 * Times are UTC.
 * </p>
 *
 * <p>
 * A path shown to someone other than the person and outside admin mode hides
 * the limits that are not on this model: the {@code ALL_MODELS} and
 * {@code PLATFORM} steps have no {@code rules} and say
 * {@code "rulesHidden": true}, and {@code TEAM} steps have no {@code teamTotal}
 * key.
 * </p>
 */
public final class ModelUsagePathView {

	private ModelUsagePathView() {

	}

	/**
	 * Writes how limits apply to one person on one model as the JSON the path view
	 * reads, showing every limit on the path.
	 *
	 * @param path      how limits apply to the person; with usage, its steps'
	 *                  states are set from what is used
	 * @param withUsage whether to read what each rule has used and when it resets
	 * @return the path as JSON-ready maps, with every step's rules
	 */
	public static Map<String, Object> toMap(ModelUsageLimitPath path, boolean withUsage) {
		return toMap(path, withUsage, false);
	}

	/**
	 * Writes how limits apply to one person on one model as the JSON the path view
	 * reads, in the shape the class javadoc shows.
	 *
	 * @param path            how limits apply to the person; with usage, its steps'
	 *                        states are set from what is used
	 * @param withUsage       whether to read what each rule has used and when it
	 *                        resets
	 * @param hideOtherModels whether to leave out the limits that are not on this
	 *                        model, for a path shown to someone other than the
	 *                        person outside admin mode: the all-models default's
	 *                        and the platform default's rules, and each team's
	 *                        total
	 * @return the path as JSON-ready maps
	 */
	public static Map<String, Object> toMap(ModelUsageLimitPath path, boolean withUsage, boolean hideOtherModels) {
		ModelUsageEvaluation evaluation = withUsage ? ModelUsageGuard.evaluateForView(path) : null;
		if (evaluation != null) {
			applyStates(path, evaluation);
		}
		Map<String, Object> map = new LinkedHashMap<>();
		map.put("engineId", path.getEngineId());
		Map<String, Object> person = new LinkedHashMap<>();
		person.put("id", path.getUserId());
		person.put("type", path.getUserType());
		map.put("person", person);
		map.put("billedLimitType", path.getBilledLimitType() == null ? null : path.getBilledLimitType().name());
		List<Map<String, Object>> ceilings = new ArrayList<>();
		for (Step step : path.getCeilings()) {
			ceilings.add(stepMap(step, evaluation, withUsage, hideOtherModels));
		}
		map.put("ceilings", ceilings);
		List<Map<String, Object>> steps = new ArrayList<>();
		for (Step step : path.getSteps()) {
			steps.add(stepMap(step, evaluation, withUsage, hideOtherModels));
		}
		map.put("steps", steps);
		return map;
	}

	private static void applyStates(ModelUsageLimitPath path, ModelUsageEvaluation evaluation) {
		for (Step step : path.getCeilings()) {
			State state = evaluation.stateOf(step);
			if (state != null) {
				step.setState(state);
			}
		}
		for (Step step : path.getSteps()) {
			State state = evaluation.stateOf(step);
			if (state != null) {
				step.setState(state);
			}
		}
	}

	private static Map<String, Object> stepMap(Step step, ModelUsageEvaluation evaluation, boolean withUsage,
			boolean hideOtherModels) {
		Map<String, Object> map = new LinkedHashMap<>();
		ModelUsageLimitKey key = step.getKey();
		map.put("layer", step.getLayer().name());
		map.put("limitType", step.getLimitType().name());
		map.put("id", key == null ? null : key.subjectId());
		map.put("type", key == null ? null : key.subjectType());
		map.put("state", step.getState() == null ? null : step.getState().name());
		map.put("reason", step.getReason() == null ? null : step.getReason().name());
		map.put("accessSince", step.getAccessSince());
		map.put("unlimited", step.isUnlimited());
		boolean acrossModels = step.getLimitType() == StepLimitType.ALL_MODELS
				|| step.getLimitType() == StepLimitType.PLATFORM;
		if (hideOtherModels && acrossModels) {
			map.put("rulesHidden", true);
		} else {
			map.put("rules", rules(step, step.getLimit(), false, evaluation, withUsage));
		}
		if (hideOtherModels && step.getLimitType() == StepLimitType.TEAM) {
			// a team's total counts its use on every model
			return map;
		}
		if (step.getTeamTotalKey() != null || step.getTeamTotal() != null) {
			ModelUsageLimit teamTotal = step.getTeamTotal();
			if (teamTotal == null) {
				map.put("teamTotal", null);
			} else {
				Map<String, Object> total = new LinkedHashMap<>();
				total.put("unlimited", teamTotal.isUnlimited());
				total.put("rules", rules(step, teamTotal, true, evaluation, withUsage));
				map.put("teamTotal", total);
			}
		} else {
			map.put("teamTotal", null);
		}
		return map;
	}

	private static List<Map<String, Object>> rules(Step step, ModelUsageLimit limit, boolean teamTotal,
			ModelUsageEvaluation evaluation, boolean withUsage) {
		List<Map<String, Object>> rules = new ArrayList<>();
		if (limit == null || limit.isUnlimited()) {
			return rules;
		}
		List<RuleCheck> checks = evaluation == null ? List.of() : evaluation.checksOf(step, teamTotal);
		for (ModelUsageLimitRule rule : limit.getRules()) {
			Map<String, Object> map = new LinkedHashMap<>();
			map.put("metric", rule.getMetric() == null ? null : rule.getMetric().name());
			map.put("amount", rule.getAmount());
			map.put("window", rule.getWindow() == null ? null : rule.getWindow().name());
			map.put("windowLength", rule.getWindowLength() > 1 ? rule.getWindowLength() : null);
			map.put("counts", rule.getCounts() == null ? null : rule.getCounts().name());
			RuleCheck check = null;
			for (RuleCheck candidate : checks) {
				if (candidate.rule == rule) {
					check = candidate;
					break;
				}
			}
			if (withUsage && check != null && check.read) {
				map.put("used", check.usedSoFar());
				map.put("resetsAt", ModelUsageWindows.iso(check.resetsAt(evaluation.getNow())));
			} else {
				map.put("used", null);
				map.put("resetsAt", null);
			}
			rules.add(map);
		}
		return rules;
	}
}
