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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockStatic;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.auth.access.ModelUsageLimit;
import prerna.auth.access.ModelUsageLimitKey;
import prerna.auth.access.ModelUsageLimitPath;
import prerna.auth.access.ModelUsageLimitPath.Layer;
import prerna.auth.access.ModelUsageLimitPath.Reason;
import prerna.auth.access.ModelUsageLimitPath.State;
import prerna.auth.access.ModelUsageLimitPath.Step;
import prerna.auth.access.ModelUsageLimitPath.StepLimitType;
import prerna.auth.access.ModelUsageLimitRule;
import prerna.auth.access.ModelUsageLimitRule.Counts;
import prerna.auth.access.ModelUsageLimitRule.Metric;
import prerna.auth.access.ModelUsageLimitRule.Window;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.inferencetracking.ModelUsageLogTotals;
import prerna.util.Utility;

/**
 * The guard on hand-built paths, as {@code ModelUsageLimitResolver} would
 * resolve them, with counters in memory and a clock moved by hand.
 */
public class ModelUsageGuardUnitTests {

	private static final String MODEL = "model-m";
	private static final String PERSON = "eli";
	private static final String LOGIN = "NATIVE";
	private static final Instant NOW = Instant.parse("2026-10-12T14:30:15Z");

	private MutableTestClock clock;
	private LocalModelUsageCounterStore store;

	@BeforeEach
	void setUp() {
		clock = new MutableTestClock(NOW);
		store = new LocalModelUsageCounterStore(clock);
		ModelUsageGuard.setCounterStore(store);
		ModelUsageGuard.setClock(clock);
		ModelUsageGuard.setSeeder(null);
		ModelUsageGuard.setPrices(engineId -> null);
	}

	@AfterEach
	void tearDown() {
		ModelUsageGuard.setCounterStore(null);
		ModelUsageGuard.setClock(null);
		ModelUsageGuard.resetSeeder();
		ModelUsageGuard.setPrices(null);
	}

	@Test
	void teamsAreBilledInOrderAndTheCallMovesOnWhenOneIsUsedUp() {
		Step ops = team("Ops", limit(rule(Metric.TOKENS, 1000L, Window.DAY, Counts.SHARED)), null);
		Step data = team("Data", limit(rule(Metric.TOKENS, 5000L, Window.DAY, Counts.SHARED)), null);
		ModelUsageLimitPath path = teamPath(ops, data);

		ModelUsageTicket first = admit(path, 100, 200L);
		assertEquals(new ModelUsageBilledTo("TEAM", "Ops", "CUSTOM"), first.billedTo());
		first.bill(report(700, 200));

		// Ops has 100 left: the call fits in it and is capped to it
		ModelUsageTicket second = admit(path, 100, 200L);
		assertEquals("Ops", second.billedTo().subjectId());
		assertEquals(100L, second.pythonCeiling().get("tokens"));
		second.bill(report(80, 20));

		// Ops is used up, so the call moves on to Data: Python gets what Data has left,
		// and
		// the estimate is reserved
		ModelUsageTicket third = admit(path, 100, 200L);
		assertEquals("Data", third.billedTo().subjectId());
		assertEquals(5000L, third.pythonCeiling().get("tokens"));
		assertEquals(300L, third.getTokensReserved());
		third.bill(report(100, 50));

		assertEquals(1000, used(ModelUsageCounterScope.teamShared(MODEL, "Ops", "CUSTOM"), Metric.TOKENS, Window.DAY));
		assertEquals(150, used(ModelUsageCounterScope.teamShared(MODEL, "Data", "CUSTOM"), Metric.TOKENS, Window.DAY));
	}

	@Test
	void aTeamWithNothingLeftInItsTeamTotalPassesTheCallOn() {
		// Ops is Unlimited on the model, but its total across all models is used up
		Step ops = team("Ops", ModelUsageLimit.unlimited(),
				limit(rule(Metric.TOKENS, 500L, Window.MONTH, Counts.SHARED)));
		Step data = team("Data", limit(rule(Metric.TOKENS, 5000L, Window.DAY, Counts.SHARED)), null);
		ModelUsageLimitPath path = teamPath(ops, data);
		charge(ModelUsageCounterScope.teamTotal("Ops", "CUSTOM"), Metric.TOKENS, Window.MONTH, 500);

		ModelUsageTicket ticket = admit(path, 100, 200L);
		assertEquals("Data", ticket.billedTo().subjectId());
	}

	@Test
	void anUnlimitedTeamIsBilledWithNoRules() {
		Step lab = team("Lab", ModelUsageLimit.unlimited(), null);
		Step data = team("Data", limit(rule(Metric.TOKENS, 10L, Window.DAY, Counts.SHARED)), null);
		ModelUsageTicket ticket = admit(teamPath(lab, data), 100, 200L);
		assertEquals("Lab", ticket.billedTo().subjectId());
		assertNull(ticket.pythonCeiling());
	}

	@Test
	void noTeamLeftRefusesWithTheSoonestReset() {
		Step ops = team("Ops", limit(rule(Metric.TOKENS, 1000L, Window.DAY, Counts.SHARED)), null);
		Step data = team("Data", limit(rule(Metric.TOKENS, 5000L, Window.WEEK, Counts.SHARED)), null);
		ModelUsageLimitPath path = teamPath(ops, data);
		charge(ModelUsageCounterScope.teamShared(MODEL, "Ops", "CUSTOM"), Metric.TOKENS, Window.DAY, 1000);
		charge(ModelUsageCounterScope.teamShared(MODEL, "Data", "CUSTOM"), Metric.TOKENS, Window.WEEK, 5000);

		ModelUsageLimitException e = assertThrows(ModelUsageLimitException.class, () -> admit(path, 100, 200L));
		assertEquals(StepLimitType.TEAM, e.getLimitType());
		assertFalse(e.isRateLimit());
		assertEquals("2026-10-13T00:00:00Z", e.getResetsAt());
		assertTrue(e.getMessage().contains("Ops"));
		assertTrue(e.getMessage().contains("Data"));
		assertFalse(e.isUnavailable());
	}

	@Test
	void aRuleAtExactlyItsAmountRefuses() {
		ModelUsageLimitPath path = directPath(limit(rule(Metric.TOKENS, 1000L, Window.DAY, Counts.EACH_PERSON)));
		charge(ModelUsageCounterScope.personModel(MODEL, PERSON, LOGIN), Metric.TOKENS, Window.DAY, 999);
		assertNotNull(admit(path, 10, 10L));
		clock.advance(Duration.ofMinutes(16));
		charge(ModelUsageCounterScope.personModel(MODEL, PERSON, LOGIN), Metric.TOKENS, Window.DAY, 1);
		ModelUsageLimitException e = assertThrows(ModelUsageLimitException.class, () -> admit(path, 10, 10L));
		assertEquals(StepLimitType.DIRECT, e.getLimitType());
	}

	@Test
	void aDirectLimitWithNothingLeftRefusesAndDoesNotFallThrough() {
		ModelUsageLimitPath path = directPath(limit(rule(Metric.TOKENS, 100L, Window.DAY, Counts.EACH_PERSON)));
		charge(ModelUsageCounterScope.personModel(MODEL, PERSON, LOGIN), Metric.TOKENS, Window.DAY, 100);
		ModelUsageLimitException e = assertThrows(ModelUsageLimitException.class, () -> admit(path, 10, 10L));
		assertEquals(StepLimitType.DIRECT, e.getLimitType());
	}

	@Test
	void theModelTotalRefusesEvenUnlimitedPeople() {
		ModelUsageLimitPath withTotal = new ModelUsageLimitPath(MODEL, PERSON, LOGIN);
		Step total = new Step(Layer.CEILING, StepLimitType.MODEL_TOTAL, ModelUsageLimitKey.modelTotal(MODEL),
				limit(rule(Metric.TOKENS, 1000L, Window.MONTH, Counts.SHARED)));
		total.setState(State.CHECKED);
		withTotal.addCeiling(total);
		Step direct = directStep(ModelUsageLimit.unlimited());
		withTotal.addStep(direct);
		withTotal.setBilled(StepLimitType.DIRECT, List.of(direct));

		assertNotNull(admit(withTotal, 10, 10L));
		charge(ModelUsageCounterScope.modelTotal(MODEL), Metric.TOKENS, Window.MONTH, 1000);
		ModelUsageLimitException e = assertThrows(ModelUsageLimitException.class, () -> admit(withTotal, 10, 10L));
		assertEquals(StepLimitType.MODEL_TOTAL, e.getLimitType());
		assertEquals("2026-11-01T00:00:00Z", e.getResetsAt());
	}

	@Test
	void reservationsAreSharedByCallsInFlight() {
		ModelUsageLimitPath path = directPath(limit(rule(Metric.TOKENS, 1000L, Window.DAY, Counts.EACH_PERSON)));
		ModelUsageTicket first = admit(path, 100, 500L);
		assertEquals(1000L, first.pythonCeiling().get("tokens"));
		assertEquals(600L, first.getTokensReserved());
		// the second call sees only what the first leaves
		ModelUsageTicket second = admit(path, 100, 500L);
		assertEquals(400L, second.pythonCeiling().get("tokens"));
		assertEquals(400L, second.getTokensReserved());
		// with everything reserved, a third is refused
		assertThrows(ModelUsageLimitException.class, () -> admit(path, 100, 500L));

		first.release();
		ModelUsageTicket fourth = admit(path, 100, 500L);
		assertEquals(600L, fourth.pythonCeiling().get("tokens"));
		assertEquals(600L, fourth.getTokensReserved());
	}

	@Test
	void billingReplacesTheReservationWithWhatWasUsed() {
		ModelUsageLimitPath path = directPath(limit(rule(Metric.TOKENS, 1000L, Window.DAY, Counts.EACH_PERSON)));
		ModelUsageTicket ticket = admit(path, 100, 500L);
		ticket.bill(report(150, 50));
		String key = key(ModelUsageCounterScope.personModel(MODEL, PERSON, LOGIN), Metric.TOKENS, Window.DAY);
		ModelUsageCount count = store.get(List.of(key)).get(key);
		assertEquals(200, count.used());
		assertEquals(0, count.reserved());
		Map<String, Object> restriction = ticket.responseUsageRestriction();
		assertEquals("token", restriction.get("restrictedBy"));
		assertEquals(200L, restriction.get("currentValue"));
		assertEquals(1000L, restriction.get("maxValue"));
		// billing again does nothing
		ticket.bill(report(150, 50));
		assertEquals(200, store.get(List.of(key)).get(key).used());
	}

	@Test
	void anEachMemberRuleCapsEachMemberInsideTheSharedTotal() {
		ModelUsageLimit research = limit(rule(Metric.TOKENS, 200L, Window.DAY, Counts.SHARED),
				rule(Metric.TOKENS, 80L, Window.DAY, Counts.EACH_PERSON));
		Step team = team("Research", research, null);
		ModelUsageLimitPath path = teamPath(team);
		ModelUsageTicket ticket = admit(path, 10, 1000L);
		// each member's 80 is less than the 200 shared
		assertEquals(80L, ticket.pythonCeiling().get("tokens"));
		ticket.bill(report(60, 20));
		assertThrows(ModelUsageLimitException.class, () -> admit(path, 10, 10L));
		// another member still has their own 80, out of the 120 the team has left
		ModelUsageLimitPath kai = new ModelUsageLimitPath(MODEL, "kai", LOGIN);
		Step kaiTeam = team("Research", research, null);
		kai.addStep(kaiTeam);
		kai.setBilled(StepLimitType.TEAM, List.of(kaiTeam));
		assertEquals(80L, admit(kai, 10, 1000L).pythonCeiling().get("tokens"));
	}

	@Test
	void minuteRulesAreRateLimitsThatSlide() {
		ModelUsageLimitPath path = directPath(limit(rule(Metric.TOKENS, 1000L, Window.MINUTE, Counts.EACH_PERSON)));
		ModelUsageTicket ticket = admit(path, 100, 900L);
		ticket.bill(report(900, 100));
		ModelUsageLimitException e = assertThrows(ModelUsageLimitException.class, () -> admit(path, 10, 10L));
		assertTrue(e.isRateLimit());
		// 15 seconds into the next minute, three quarters of the last one still counts
		clock.set(Instant.parse("2026-10-12T14:31:15Z"));
		ModelUsageTicket next = admit(path, 10, 10000L);
		assertEquals(250L, next.pythonCeiling().get("tokens"));
		next.release();
		// 45 seconds in, a quarter of it still counts
		clock.set(Instant.parse("2026-10-12T14:31:45Z"));
		ModelUsageTicket later = admit(path, 10, 10000L);
		assertEquals(750L, later.pythonCeiling().get("tokens"));
	}

	@Test
	void aRuleCanSlideOverSeveralMinutes() {
		ModelUsageLimitPath path = directPath(
				limit(new ModelUsageLimitRule(Metric.TOKENS, 1000L, Window.MINUTE, 10, Counts.EACH_PERSON)));
		ModelUsageTicket ticket = admit(path, 100, 900L);
		ticket.bill(report(900, 100));
		ModelUsageLimitException e = assertThrows(ModelUsageLimitException.class, () -> admit(path, 10, 10L));
		assertTrue(e.isRateLimit());
		// eight minutes later the same 10 minutes still hold all of it
		clock.set(Instant.parse("2026-10-12T14:38:15Z"));
		assertThrows(ModelUsageLimitException.class, () -> admit(path, 10, 10L));
		// halfway through the next 10 minutes, half of the last 10 still counts
		clock.set(Instant.parse("2026-10-12T14:45:00Z"));
		ModelUsageTicket next = admit(path, 10, 10000L);
		assertEquals(500L, next.pythonCeiling().get("tokens"));
	}

	@Test
	void aMinuteRuleAndATenMinuteRuleCountSeparately() {
		ModelUsageLimitPath path = directPath(limit(rule(Metric.TOKENS, 1000L, Window.MINUTE, Counts.EACH_PERSON),
				new ModelUsageLimitRule(Metric.TOKENS, 5000L, Window.MINUTE, 10, Counts.EACH_PERSON)));
		ModelUsageTicket ticket = admit(path, 100, 800L);
		ticket.bill(report(800, 100));
		// the minute rule has 100 left and the 10-minute rule 4,100
		assertEquals(100L, admit(path, 10, 10000L).pythonCeiling().get("tokens"));
		// two minutes later the minute rule starts over, and the 10-minute rule still
		// counts the 900
		clock.set(Instant.parse("2026-10-12T14:32:15Z"));
		assertEquals(1000L, admit(path, 10, 10000L).pythonCeiling().get("tokens"));
	}

	@Test
	void computeTimeIsCheckedButNotReserved() {
		ModelUsageLimitPath path = directPath(limit(rule(Metric.COMPUTE_TIME, 1000L, Window.DAY, Counts.EACH_PERSON)));
		ModelUsageTicket ticket = admit(path, 100, 100L);
		assertNull(ticket.pythonCeiling());
		// a call that started runs to the end and is billed for all of its time
		ticket.bill(new ModelUsageReport(100, 100, 0, 0, 0, 1500));
		assertEquals(1500,
				used(ModelUsageCounterScope.personModel(MODEL, PERSON, LOGIN), Metric.COMPUTE_TIME, Window.DAY));
		assertThrows(ModelUsageLimitException.class, () -> admit(path, 100, 100L));
	}

	@Test
	void costRulesCountOnlyOnModelsWithKnownPrices() {
		ModelUsageLimitPath path = directPath(limit(rule(Metric.COST, 1_000_000L, Window.DAY, Counts.EACH_PERSON)));
		// no known prices: the cost rule adds nothing
		ModelUsageTicket unpriced = admit(path, 1000, 1000L);
		assertNull(unpriced.pythonCeiling());
		assertNull(unpriced.bill(report(1000, 1000)));

		Map<String, Object> entry = new LinkedHashMap<>();
		entry.put(ModelUsageCost.SERVING_PROVIDER, "openai");
		entry.put(ModelUsageCost.MODEL_ID, "gpt-5");
		entry.put(ModelUsageCost.INPUT, 2.0);
		entry.put(ModelUsageCost.OUTPUT, 10.0);
		ModelUsageTicket priced = ModelUsageGuard.admit(path, "GPT-5", entry, ModelUsageRequest.ask(1000, 1000L));
		Map<String, Object> ceiling = priced.pythonCeiling();
		// Python gets what is left; 1,000 prompt tokens at $2 and 1,000 output tokens
		// at $10, per million, are reserved
		assertEquals(1_000_000L, ceiling.get("cost_micros"));
		assertEquals(12_000L, priced.getCostReserved());
		assertEquals(1000L, ceiling.get("max_output_tokens"));
		assertNull(ceiling.get("tokens"));
		assertEquals(2.0, ((Map<?, ?>) ceiling.get("rates")).get("input"));
		assertEquals(256L, ceiling.get("min_output_tokens"));
		assertEquals(0.05, ceiling.get("margin"));
		assertEquals(Long.valueOf(7000L), priced.bill(report(1000, 500)));
		assertEquals(7000, used(ModelUsageCounterScope.personModel(MODEL, PERSON, LOGIN), Metric.COST, Window.DAY));
	}

	@Test
	void personalCountersCountAllUseWhicheverSourceIsBilled() {
		// public use has a rule for each person, but a team is billed
		Step ops = team("Ops", limit(rule(Metric.TOKENS, 10_000L, Window.DAY, Counts.SHARED)), null);
		ModelUsageLimitPath path = new ModelUsageLimitPath(MODEL, PERSON, LOGIN);
		path.addStep(directStep(null).skipped(Reason.NO_GRANT));
		path.addStep(ops);
		Step publicUse = new Step(Layer.BILLED, StepLimitType.PUBLIC, ModelUsageLimitKey.publicUse(MODEL),
				limit(rule(Metric.TOKENS, 500L, Window.DAY, Counts.EACH_PERSON),
						rule(Metric.TOKENS, 100_000L, Window.DAY, Counts.SHARED)));
		publicUse.setState(State.NOT_REACHED);
		path.addStep(publicUse);
		Step allModels = new Step(Layer.BILLED, StepLimitType.ALL_MODELS, ModelUsageLimitKey.allModels(PERSON, LOGIN),
				limit(rule(Metric.TOKENS, 50_000L, Window.DAY, Counts.EACH_PERSON)));
		allModels.setState(State.NOT_REACHED);
		path.addStep(allModels);
		path.setBilled(StepLimitType.TEAM, List.of(ops));

		admit(path, 100, 100L).bill(report(300, 100));

		assertEquals(400, used(ModelUsageCounterScope.teamShared(MODEL, "Ops", "CUSTOM"), Metric.TOKENS, Window.DAY));
		// the person's own counters count it; public use's shared total does not
		assertEquals(400, used(ModelUsageCounterScope.personModel(MODEL, PERSON, LOGIN), Metric.TOKENS, Window.DAY));
		assertEquals(400, used(ModelUsageCounterScope.personAllModels(PERSON, LOGIN), Metric.TOKENS, Window.DAY));
		assertEquals(0, used(ModelUsageCounterScope.publicShared(MODEL), Metric.TOKENS, Window.DAY));
	}

	@Test
	void aRoutersOwnCallLeavesTheAllModelsCountersToTheRoutedModel() {
		Step allModels = new Step(Layer.BILLED, StepLimitType.ALL_MODELS, ModelUsageLimitKey.allModels(PERSON, LOGIN),
				limit(rule(Metric.TOKENS, 50_000L, Window.DAY, Counts.EACH_PERSON)));
		ModelUsageLimitPath path = new ModelUsageLimitPath(MODEL, PERSON, LOGIN);
		path.addStep(allModels);
		path.setBilled(StepLimitType.ALL_MODELS, List.of(allModels));

		ModelUsageGuard.admit(path, "Router", null, ModelUsageRequest.ask(100, 100L).forRouter())
				.bill(report(300, 100));
		assertEquals(0, used(ModelUsageCounterScope.personAllModels(PERSON, LOGIN), Metric.TOKENS, Window.DAY));

		ModelUsageGuard.admit(path, "GPT-5", null, ModelUsageRequest.ask(100, 100L)).bill(report(300, 100));
		assertEquals(400, used(ModelUsageCounterScope.personAllModels(PERSON, LOGIN), Metric.TOKENS, Window.DAY));
	}

	@Test
	void countersThatCannotBeReachedRefuseOnlyLimitedCalls() {
		ModelUsageGuard.setCounterStore(new UnreachableStore());
		ModelUsageLimitPath limited = directPath(limit(rule(Metric.TOKENS, 1000L, Window.DAY, Counts.EACH_PERSON)));
		ModelUsageLimitException e = assertThrows(ModelUsageLimitException.class, () -> admit(limited, 10, 10L));
		assertTrue(e.isUnavailable());
		assertEquals(ModelUsageLimitException.UNAVAILABLE_MESSAGE, e.getMessage());

		// an Unlimited person whose all-models default has rules is not limited on
		// this model: the call runs, uncounted
		Step direct = directStep(ModelUsageLimit.unlimited());
		Step allModels = new Step(Layer.BILLED, StepLimitType.ALL_MODELS, ModelUsageLimitKey.allModels(PERSON, LOGIN),
				limit(rule(Metric.TOKENS, 50_000L, Window.DAY, Counts.EACH_PERSON)));
		ModelUsageLimitPath unlimited = new ModelUsageLimitPath(MODEL, PERSON, LOGIN);
		unlimited.addStep(direct);
		unlimited.addStep(allModels);
		unlimited.setBilled(StepLimitType.DIRECT, List.of(direct));
		ModelUsageTicket ticket = admit(unlimited, 10, 10L);
		assertEquals("USER", ticket.billedTo().sourceType());
		ticket.bill(report(10, 10));
	}

	@Test
	void aCallWithNoLimitTouchesNoCounters() {
		ModelUsageGuard.setCounterStore(new UnreachableStore());
		ModelUsageLimitPath path = new ModelUsageLimitPath(MODEL, PERSON, LOGIN);
		path.addStep(directStep(null).skipped(Reason.NO_GRANT));
		path.setBilled(null, List.of());
		ModelUsageTicket ticket = admit(path, 10, 10L);
		assertNull(ticket.billedTo());
		assertNull(ticket.pythonCeiling());
		assertNull(ticket.responseUsageLimits(false));
		ticket.bill(report(10, 10));
	}

	@Test
	void dayCountersAreFilledFromTheLogsOnce() {
		int[] reads = { 0 };
		ModelUsageGuard.setSeeder((scope, metric, start, end) -> {
			reads[0]++;
			assertEquals(Instant.parse("2026-10-12T00:00:00Z"), start);
			assertEquals(Instant.parse("2026-10-13T00:00:00Z"), end);
			return 900L;
		});
		ModelUsageLimitPath path = directPath(limit(rule(Metric.TOKENS, 1000L, Window.DAY, Counts.EACH_PERSON)));
		ModelUsageTicket ticket = admit(path, 10, 500L);
		assertEquals(100L, ticket.pythonCeiling().get("tokens"));
		ticket.release();
		admit(path, 10, 500L).release();
		assertEquals(1, reads[0]);
	}

	@Test
	void thePathViewSetsEachStepsStateFromWhatIsUsed() {
		Step ops = team("Ops", limit(rule(Metric.TOKENS, 1000L, Window.DAY, Counts.SHARED)), null);
		Step data = team("Data", limit(rule(Metric.TOKENS, 5000L, Window.DAY, Counts.SHARED)), null);
		Step lab = team("Lab", limit(rule(Metric.TOKENS, 5000L, Window.DAY, Counts.SHARED)), null);
		ModelUsageLimitPath path = teamPath(ops, data, lab);
		charge(ModelUsageCounterScope.teamShared(MODEL, "Ops", "CUSTOM"), Metric.TOKENS, Window.DAY, 1000);
		charge(ModelUsageCounterScope.teamShared(MODEL, "Data", "CUSTOM"), Metric.TOKENS, Window.DAY, 120);

		Map<String, Object> view = ModelUsagePathView.toMap(path, true);
		assertEquals("TEAM", view.get("billedLimitType"));
		List<?> steps = (List<?>) view.get("steps");
		Map<?, ?> opsView = (Map<?, ?>) steps.get(1);
		Map<?, ?> dataView = (Map<?, ?>) steps.get(2);
		Map<?, ?> labView = (Map<?, ?>) steps.get(3);
		assertEquals("REACHED", opsView.get("state"));
		assertEquals("BILLED", dataView.get("state"));
		assertEquals("NEXT", labView.get("state"));
		Map<?, ?> dataRule = (Map<?, ?>) ((List<?>) dataView.get("rules")).get(0);
		assertEquals(120L, dataRule.get("used"));
		assertEquals(5000L, dataRule.get("amount"));
		assertEquals("2026-10-13T00:00:00Z", dataRule.get("resetsAt"));
		assertEquals("Data", dataView.get("id"));
		assertEquals("CUSTOM", dataView.get("type"));

		Map<String, Object> withoutUsage = ModelUsagePathView.toMap(
				teamPath(team("Ops", limit(rule(Metric.TOKENS, 1000L, Window.DAY, Counts.SHARED)), null)), false);
		Map<?, ?> rule = (Map<?, ?>) ((List<?>) ((Map<?, ?>) ((List<?>) withoutUsage.get("steps")).get(1)).get("rules"))
				.get(0);
		assertNull(rule.get("used"));
		assertNull(rule.get("resetsAt"));
	}

	@Test
	void pythonIsGivenWhatIsLeftAndTheCountersHoldTheEstimate() {
		ModelUsageLimitPath path = directPath(limit(rule(Metric.TOKENS, 100_000L, Window.DAY, Counts.EACH_PERSON)));
		ModelUsageTicket ticket = admit(path, 300, 700L);
		// an estimate that is too low, such as for a PDF, does not refuse the call
		assertEquals(100_000L, ticket.pythonCeiling().get("tokens"));
		assertEquals(1000L, ticket.getTokensReserved());
		String key = key(ModelUsageCounterScope.personModel(MODEL, PERSON, LOGIN), Metric.TOKENS, Window.DAY);
		assertEquals(1000, store.get(List.of(key)).get(key).reserved());
	}

	@Test
	void aCallWithNoOutputMaximumIsReservedForTheDefault() {
		ModelUsageLimitPath path = directPath(limit(rule(Metric.TOKENS, 100_000L, Window.DAY, Counts.EACH_PERSON)));
		ModelUsageTicket ticket = admit(path, 100, null);
		assertEquals(100L + ModelUsageRequest.DEFAULT_MAX_OUTPUT_TOKENS, ticket.getTokensReserved());
		assertEquals(ModelUsageRequest.DEFAULT_MAX_OUTPUT_TOKENS, ticket.pythonCeiling().get("max_output_tokens"));
		// the call does not hold all that is left, so a second one fits beside it
		ModelUsageTicket second = admit(path, 100, null);
		assertEquals(100_000L - 100 - ModelUsageRequest.DEFAULT_MAX_OUTPUT_TOKENS,
				second.pythonCeiling().get("tokens"));
	}

	@Test
	void aCostReservationWithNoOutputMaximumIsBoundedToo() {
		ModelUsageLimitPath path = directPath(limit(rule(Metric.COST, 10_000_000L, Window.DAY, Counts.EACH_PERSON)));
		Map<String, Object> entry = new LinkedHashMap<>();
		entry.put(ModelUsageCost.SERVING_PROVIDER, "openai");
		entry.put(ModelUsageCost.INPUT, 2.0);
		entry.put(ModelUsageCost.OUTPUT, 10.0);
		ModelUsageTicket ticket = ModelUsageGuard.admit(path, "GPT-5", entry, ModelUsageRequest.ask(1000, null));
		assertEquals(1000L * 2 + ModelUsageRequest.DEFAULT_MAX_OUTPUT_TOKENS * 10, ticket.getCostReserved());
		assertEquals(10_000_000L, ticket.pythonCeiling().get("cost_micros"));
	}

	@Test
	void embeddingsHaveNoOutputMaximum() {
		ModelUsageLimitPath path = directPath(limit(rule(Metric.TOKENS, 1000L, Window.DAY, Counts.EACH_PERSON)));
		ModelUsageTicket ticket = ModelUsageGuard.admit(path, "GPT-5", null, ModelUsageRequest.embeddings(300));
		assertEquals(300L, ticket.getTokensReserved());
		assertNull(ticket.pythonCeiling().get("max_output_tokens"));
	}

	@Test
	void aRoutersOwnCallLeavesTheTeamTotalToTheRoutedModel() {
		Step ops = team("Ops", limit(rule(Metric.TOKENS, 10_000L, Window.DAY, Counts.SHARED)),
				limit(rule(Metric.TOKENS, 50_000L, Window.MONTH, Counts.SHARED)));
		ModelUsageLimitPath path = teamPath(ops);

		ModelUsageTicket router = ModelUsageGuard.admit(path, "Router", null,
				ModelUsageRequest.ask(100, 100L).forRouter());
		// the team total is checked for the router's call, but not reserved or billed
		assertEquals(10_000L, router.pythonCeiling().get("tokens"));
		router.bill(report(300, 100));
		assertEquals(400, used(ModelUsageCounterScope.teamShared(MODEL, "Ops", "CUSTOM"), Metric.TOKENS, Window.DAY));
		assertEquals(0, used(ModelUsageCounterScope.teamTotal("Ops", "CUSTOM"), Metric.TOKENS, Window.MONTH));

		ModelUsageGuard.admit(path, "GPT-5", null, ModelUsageRequest.ask(100, 100L)).bill(report(300, 100));
		assertEquals(400, used(ModelUsageCounterScope.teamTotal("Ops", "CUSTOM"), Metric.TOKENS, Window.MONTH));
		// and the logs fill a team total without the router's rows
		assertTrue(ModelUsageCounterScope.teamTotal("Ops", "CUSTOM").logFilter().skipRouterRows());
		assertFalse(ModelUsageCounterScope.teamShared(MODEL, "Ops", "CUSTOM").logFilter().skipRouterRows());
	}

	@Test
	void thePromptIsEstimatedOnlyWhenATokenOrCostRuleApplies() {
		int[] estimates = { 0 };
		LongSupplier estimate = () -> {
			estimates[0]++;
			return 100L;
		};
		ModelUsageLimitPath noLimit = new ModelUsageLimitPath(MODEL, PERSON, LOGIN);
		noLimit.addStep(directStep(null).skipped(Reason.NO_GRANT));
		noLimit.setBilled(null, List.of());
		ModelUsageGuard.admit(noLimit, "GPT-5", null, ModelUsageRequest.ask(estimate, 100L)).release();
		ModelUsageLimitPath computeOnly = directPath(
				limit(rule(Metric.COMPUTE_TIME, 1000L, Window.DAY, Counts.EACH_PERSON)));
		ModelUsageGuard.admit(computeOnly, "GPT-5", null, ModelUsageRequest.ask(estimate, 100L)).release();
		// no cost rule counts on a model with no known prices
		ModelUsageLimitPath unpricedCost = directPath(limit(rule(Metric.COST, 1000L, Window.DAY, Counts.EACH_PERSON)));
		ModelUsageGuard.admit(unpricedCost, "GPT-5", null, ModelUsageRequest.ask(estimate, 100L)).release();
		assertEquals(0, estimates[0]);

		ModelUsageLimitPath tokens = directPath(limit(rule(Metric.TOKENS, 1000L, Window.DAY, Counts.EACH_PERSON)));
		ModelUsageTicket ticket = ModelUsageGuard.admit(tokens, "GPT-5", null, ModelUsageRequest.ask(estimate, 100L));
		assertEquals(1, estimates[0]);
		assertEquals(200L, ticket.getTokensReserved());
	}

	@Test
	void logsThatCannotBeReadAreNotReadAgainForAMinute() {
		int[] reads = { 0 };
		ModelUsageGuard.setSeeder((scope, metric, start, end) -> {
			reads[0]++;
			return null;
		});
		ModelUsageLimitPath path = directPath(limit(rule(Metric.TOKENS, 1000L, Window.DAY, Counts.EACH_PERSON)));
		admit(path, 10, 10L).release();
		admit(path, 10, 10L).release();
		assertEquals(1, reads[0]);
		// the counter is not filled with nothing, so the logs are read again later
		String key = key(ModelUsageCounterScope.personModel(MODEL, PERSON, LOGIN), Metric.TOKENS, Window.DAY);
		assertFalse(store.get(List.of(key)).get(key).seeded());
		clock.advance(Duration.ofSeconds(61));
		admit(path, 10, 10L).release();
		assertEquals(2, reads[0]);
	}

	@Test
	void callsThatFillTheSameCounterShareOneReadOfTheLogs() throws Exception {
		CountDownLatch seeding = new CountDownLatch(1);
		CountDownLatch bothRead = new CountDownLatch(2);
		CountDownLatch finish = new CountDownLatch(1);
		AtomicInteger reads = new AtomicInteger();
		store = new LocalModelUsageCounterStore(clock) {
			@Override
			public synchronized Map<String, ModelUsageCount> get(Collection<String> keys) {
				Map<String, ModelUsageCount> counts = super.get(keys);
				bothRead.countDown();
				return counts;
			}
		};
		ModelUsageGuard.setCounterStore(store);
		ModelUsageGuard.setSeeder((scope, metric, start, end) -> {
			reads.incrementAndGet();
			seeding.countDown();
			try {
				finish.await(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			return 900L;
		});
		ModelUsageLimitPath path = directPath(limit(rule(Metric.TOKENS, 1000L, Window.DAY, Counts.EACH_PERSON)));
		ExecutorService pool = Executors.newFixedThreadPool(2);
		try {
			Future<ModelUsageTicket> first = pool.submit(() -> admit(path, 10, 40L));
			assertTrue(seeding.await(5, TimeUnit.SECONDS));
			Future<ModelUsageTicket> second = pool.submit(() -> admit(path, 10, 40L));
			// the second call has read the counter, unfilled, and waits for the first
			assertTrue(bothRead.await(5, TimeUnit.SECONDS));
			Thread.sleep(100);
			finish.countDown();
			// both count the 900 the logs hold
			assertEquals(100L, first.get(5, TimeUnit.SECONDS).pythonCeiling().get("tokens"));
			assertEquals(100L, second.get(5, TimeUnit.SECONDS).pythonCeiling().get("tokens"));
			assertEquals(1, reads.get());
		} finally {
			finish.countDown();
			pool.shutdownNow();
		}
	}

	@Test
	void costFromBeforeCostsWereRecordedIsPricedAtTheBaseRates() {
		Map<String, Object> entry = new LinkedHashMap<>();
		entry.put(ModelUsageCost.SERVING_PROVIDER, "anthropic");
		entry.put(ModelUsageCost.INPUT, 5.0);
		entry.put(ModelUsageCost.OUTPUT, 25.0);
		entry.put(ModelUsageCost.CONTEXT_OVER_200K, Map.of(ModelUsageCost.INPUT, 10.0, ModelUsageCost.OUTPUT, 37.5));
		ModelUsageGuard.setPrices(engineId -> entry);
		Instant start = Instant.parse("2026-10-12T00:00:00Z");
		Instant end = Instant.parse("2026-10-13T00:00:00Z");
		try (MockedStatic<Utility> utility = mockStatic(Utility.class);
				MockedStatic<ModelInferenceLogsUtils> logs = mockStatic(ModelInferenceLogsUtils.class)) {
			utility.when(Utility::isModelInferenceLogsEnabled).thenReturn(true);
			logs.when(() -> ModelInferenceLogsUtils.sumModelUsage(eq(Metric.COST), any(), any(), any()))
					.thenReturn(1000L);
			// a whole day of calls adds up to more than 200,000 prompt tokens
			logs.when(() -> ModelInferenceLogsUtils.sumModelUsageBeforeCost(any(), any(), any()))
					.thenReturn(List.of(new ModelUsageLogTotals(MODEL, 300_000, 0, 0, 1000, 0)));
			Long used = ModelUsageGuard.usedInLogs(ModelUsageCounterScope.personModel(MODEL, PERSON, LOGIN),
					Metric.COST, start, end);
			assertEquals(1000L + 300_000L * 5 + 1000L * 25, used);
		}
	}

	/*
	 * Paths, as the resolver builds them
	 */

	private static ModelUsageLimitPath directPath(ModelUsageLimit limit) {
		ModelUsageLimitPath path = new ModelUsageLimitPath(MODEL, PERSON, LOGIN);
		path.addCeiling(new Step(Layer.CEILING, StepLimitType.MODEL_TOTAL, ModelUsageLimitKey.modelTotal(MODEL), null));
		Step direct = directStep(limit);
		path.addStep(direct);
		path.setBilled(StepLimitType.DIRECT, List.of(direct));
		return path;
	}

	private static Step directStep(ModelUsageLimit limit) {
		return new Step(Layer.BILLED, StepLimitType.DIRECT, ModelUsageLimitKey.direct(MODEL, PERSON, null), limit);
	}

	private static ModelUsageLimitPath teamPath(Step... teams) {
		ModelUsageLimitPath path = new ModelUsageLimitPath(MODEL, PERSON, LOGIN);
		path.addCeiling(new Step(Layer.CEILING, StepLimitType.MODEL_TOTAL, ModelUsageLimitKey.modelTotal(MODEL), null));
		path.addStep(directStep(null).skipped(Reason.NO_GRANT));
		for (Step team : teams) {
			path.addStep(team);
		}
		path.addStep(new Step(Layer.BILLED, StepLimitType.PUBLIC, ModelUsageLimitKey.publicUse(MODEL), null)
				.skipped(Reason.NOT_PUBLIC));
		path.setBilled(StepLimitType.TEAM, List.of(teams));
		return path;
	}

	private static Step team(String id, ModelUsageLimit limit, ModelUsageLimit teamTotal) {
		return new Step(Layer.BILLED, StepLimitType.TEAM, ModelUsageLimitKey.team(MODEL, id, "CUSTOM"), limit)
				.withTeamTotal(ModelUsageLimitKey.teamTotal(id, "CUSTOM"), teamTotal);
	}

	private static ModelUsageLimit limit(ModelUsageLimitRule... rules) {
		return ModelUsageLimit.of(List.of(rules));
	}

	private static ModelUsageLimitRule rule(Metric metric, Long amount, Window window, Counts counts) {
		return new ModelUsageLimitRule(metric, amount, window, counts);
	}

	/*
	 * Calls and counters
	 */

	private static ModelUsageTicket admit(ModelUsageLimitPath path, long promptEstimate, Long maxOutput) {
		return ModelUsageGuard.admit(path, "GPT-5", null, ModelUsageRequest.ask(promptEstimate, maxOutput));
	}

	private static ModelUsageReport report(long prompt, long response) {
		return new ModelUsageReport(prompt, response, 0, 0, 0, 0);
	}

	private String key(ModelUsageCounterScope scope, Metric metric, Window window) {
		return ModelUsageEvaluation.keyOf(scope, metric, window,
				ModelUsageWindows.bucketStart(window, clock.instant()));
	}

	private long used(ModelUsageCounterScope scope, Metric metric, Window window) {
		String key = key(scope, metric, window);
		return store.get(List.of(key)).get(key).used();
	}

	private void charge(ModelUsageCounterScope scope, Metric metric, Window window, long amount) {
		Instant bucketStart = ModelUsageWindows.bucketStart(window, clock.instant());
		store.settle(null, null, List.of(new ModelUsageCharge(key(scope, metric, window), amount,
				ModelUsageWindows.counterExpiresAt(window, bucketStart))));
	}

	/** Counters that cannot be reached, like Redis down on a cluster */
	private static final class UnreachableStore implements IModelUsageCounterStore {
		@Override
		public Map<String, ModelUsageCount> get(Collection<String> keys) {
			throw new ModelUsageStoreException("down", null);
		}

		@Override
		public ModelUsageReservation reserve(List<ModelUsageHold> holds, Duration expiry) {
			throw new ModelUsageStoreException("down", null);
		}

		@Override
		public void settle(ModelUsageReservation reservation, Map<String, Long> released,
				List<ModelUsageCharge> charges) {
			throw new ModelUsageStoreException("down", null);
		}

		@Override
		public void seed(String key, long amount, long expiresAtMillis) {
			throw new ModelUsageStoreException("down", null);
		}

		@Override
		public boolean markOnce(String key, Duration ttl) {
			throw new ModelUsageStoreException("down", null);
		}
	}
}
