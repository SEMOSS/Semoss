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

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import prerna.auth.access.ModelUsageLimit;
import prerna.auth.access.ModelUsageLimitKey;
import prerna.auth.access.ModelUsageLimitPath;
import prerna.auth.access.ModelUsageLimitPath.State;
import prerna.auth.access.ModelUsageLimitPath.Step;
import prerna.auth.access.ModelUsageLimitPath.StepLimitType;
import prerna.auth.access.ModelUsageLimitRule;
import prerna.auth.access.ModelUsageLimitRule.Counts;
import prerna.auth.access.ModelUsageLimitRule.Metric;
import prerna.auth.access.ModelUsageLimitRule.Window;

/**
 * How one person's path on one model stands against the counters at one moment:
 * what each rule has counted, which ceilings have usage left, and which source
 * a call is billed to. The path itself is not changed, since resolved paths are
 * cached and shared; the states found here are kept on the side.
 *
 * <ol>
 * <li>{@link #of}: one {@link RuleCheck} for each rule of the ceilings and of
 * the steps a call can be billed to, with the counter key of the window bucket
 * that holds now.</li>
 * <li>{@link #read}: one read of every counter, current and previous buckets,
 * filling cold counters from the inference logs once.</li>
 * <li>{@link #decide}: which ceiling refuses the call, or which source it is
 * billed to.</li>
 * <li>{@link #left}, {@link #tightest} and {@link #checkForCall}: what is left
 * and the limits a reservation must stay within.</li>
 * </ol>
 */
final class ModelUsageEvaluation {

	/** The start of every counter key, for model usage limits */
	static final String KEY_PREFIX = "mul|";
	/**
	 * How long a counter whose logs could not be read waits before they are read
	 * again
	 */
	static final Duration SEED_RETRY = Duration.ofMinutes(1);

	/**
	 * The counters being filled from the logs right now, so the first reads of a
	 * counter after a restart share one query of the logs
	 */
	private static final ConcurrentHashMap<String, CompletableFuture<Long>> SEEDING = new ConcurrentHashMap<>();
	/** Counters whose logs could not be read, with when they are read again */
	private static final Cache<String, Instant> SEED_FAILURES = Caffeine.newBuilder().maximumSize(100_000)
			.expireAfterWrite(SEED_RETRY).build();

	/**
	 * Fills a counter from the inference logs: day and longer windows, and sliding
	 * windows of an hour or longer
	 */
	@FunctionalInterface
	interface Seeder {
		/**
		 * Adds up what the inference logs hold for a counter's scope and metric between
		 * the start and the end of its bucket.
		 *
		 * @return what the logs say was used in the window, or null when they cannot
		 *         say
		 */
		Long usedInLogs(ModelUsageCounterScope scope, Metric metric, Instant start, Instant end);
	}

	/** One rule of one step, with its counter and what the counter holds */
	static final class RuleCheck {
		final Step step;
		final boolean teamTotal;
		final ModelUsageLimitRule rule;
		final long amount;
		final ModelUsageCounterScope scope;
		final String key;
		/** The previous bucket's counter, for a sliding window; null otherwise */
		final String previousKey;
		final Instant bucketStart;
		final Instant bucketEnd;
		final Instant previousStart;
		final long expiresAtMillis;
		/**
		 * False for a cost rule on a model with no known prices, which never refuses
		 */
		final boolean applies;
		final Instant now;
		// what the counters held when they were read: the current bucket's used and
		// reserved, the previous bucket's total (used and reserved) and used, and
		// the previous total's weighted share that still counts toward the amount
		long used;
		long reserved;
		long previousTotal;
		long previousUsed;
		long carried;
		boolean read;

		RuleCheck(Step step, boolean teamTotal, ModelUsageLimitRule rule, long amount, ModelUsageCounterScope scope,
				Instant now, boolean applies) {
			this.step = step;
			this.teamTotal = teamTotal;
			this.rule = rule;
			this.amount = amount;
			this.scope = scope;
			this.applies = applies;
			this.now = now;
			Window window = rule.getWindow();
			int length = rule.getWindowLength();
			this.bucketStart = ModelUsageWindows.bucketStart(window, length, now);
			this.bucketEnd = ModelUsageWindows.bucketEnd(window, length, now);
			this.key = keyOf(scope, rule.getMetric(), window, length, this.bucketStart);
			this.previousStart = ModelUsageWindows.isSliding(window)
					? ModelUsageWindows.previousBucketStart(window, length, now)
					: null;
			this.previousKey = this.previousStart == null ? null
					: keyOf(scope, rule.getMetric(), window, length, this.previousStart);
			this.expiresAtMillis = ModelUsageWindows.counterExpiresAt(window, length, this.bucketStart);
		}

		Window window() {
			return rule.getWindow();
		}

		/**
		 * The rule's window length, which its buckets and counter keys are worked out
		 * with.
		 *
		 * @return how many minutes or hours a sliding window is, 1 otherwise
		 */
		int length() {
			return rule.getWindowLength();
		}

		/**
		 * Whether a cold counter of the rule is filled from the inference logs the
		 * first time it is read.
		 *
		 * @return whether the rule's counters are filled from the logs the first time
		 *         they are read: day and longer windows, and sliding windows of an hour
		 *         or longer, so a restart does not give them a fresh count
		 */
		boolean seedsFromLogs() {
			return !ModelUsageWindows.isSliding(window())
					|| ModelUsageWindows.length(window(), length()) >= ModelUsageWindows.HOUR_MILLIS;
		}

		Metric metric() {
			return rule.getMetric();
		}

		/**
		 * What the rule has counted against its amount, from the read of the counters,
		 * which decides whether it has usage left and how much.
		 *
		 * @return what counts toward the amount now: what is used and reserved, and a
		 *         sliding window's share of the previous bucket
		 */
		long counted() {
			return used + reserved + carried;
		}

		/**
		 * What the rule has used, which the path view and the response's usage limits
		 * show; reservations are left out.
		 *
		 * @return what is used, without what calls in flight hold
		 */
		long usedSoFar() {
			return used + ModelUsageWindows.carried(window(), length(), now, previousUsed);
		}

		boolean hasUsageLeft() {
			return !applies || counted() < amount;
		}

		long left() {
			return Math.max(0, amount - counted());
		}

		Instant resetsAt(Instant now) {
			return ModelUsageWindows.resetsAt(window(), length(), now, previousTotal, used + reserved, amount);
		}
	}

	private final ModelUsageLimitPath path;
	private final Instant now;
	private final boolean pricesKnown;
	private final List<RuleCheck> checks = new ArrayList<>();
	private final Map<Step, List<RuleCheck>> limitChecks = new IdentityHashMap<>();
	private final Map<Step, List<RuleCheck>> teamTotalChecks = new IdentityHashMap<>();
	private final Map<Step, State> states = new IdentityHashMap<>();
	private Step billed;
	private Step refusedBy;
	private RuleCheck refusedRule;
	private final List<Step> teamsReached = new ArrayList<>();
	private boolean decided;

	private ModelUsageEvaluation(ModelUsageLimitPath path, Instant now, boolean pricesKnown) {
		this.path = path;
		this.now = now;
		this.pricesKnown = pricesKnown;
	}

	/**
	 * Sets up the evaluation of a path at a moment: a rule check for each rule of
	 * the ceilings and of the steps it counts, and of a defined team's Team Total.
	 *
	 * @param path        the person's path on the model
	 * @param allSteps    whether to count every step with rules, for the path view,
	 *                    or only the ceilings and the steps a call can be billed to
	 * @param pricesKnown whether the model has known prices; cost rules count only
	 *                    when it does
	 * @param now         the moment
	 * @return the evaluation, with its rules not read yet
	 */
	static ModelUsageEvaluation of(ModelUsageLimitPath path, boolean allSteps, boolean pricesKnown, Instant now) {
		ModelUsageEvaluation evaluation = new ModelUsageEvaluation(path, now, pricesKnown);
		for (Step ceiling : path.getCeilings()) {
			evaluation.addChecks(ceiling, false);
		}
		Set<Step> order = Collections.newSetFromMap(new IdentityHashMap<>());
		order.addAll(path.getBillingOrder());
		for (Step step : path.getSteps()) {
			if (allSteps || order.contains(step)) {
				evaluation.addChecks(step, false);
				// a defined team is checked on its Team Total too, since a team with
				// nothing left there passes the call on like one with nothing left on the
				// model
				if (step.getLimitType() == StepLimitType.TEAM && step.isDefined()) {
					evaluation.addChecks(step, true);
				}
			}
		}
		return evaluation;
	}

	private void addChecks(Step step, boolean teamTotal) {
		ModelUsageLimit limit = teamTotal ? step.getTeamTotal() : step.getLimit();
		List<RuleCheck> added = new ArrayList<>();
		if (limit != null && !limit.isUnlimited()) {
			for (ModelUsageLimitRule rule : limit.getRules()) {
				Long amount = rule.getAmount();
				if (amount == null || rule.getMetric() == null || rule.getWindow() == null) {
					continue;
				}
				ModelUsageCounterScope scope = scopeOf(path, step, rule, teamTotal);
				// a cost rule is kept for the path view on a model with no known prices,
				// but it never refuses a call there
				boolean applies = rule.getMetric() != Metric.COST || pricesKnown;
				RuleCheck check = new RuleCheck(step, teamTotal, rule, amount, scope, now, applies);
				added.add(check);
				checks.add(check);
			}
		}
		(teamTotal ? teamTotalChecks : limitChecks).put(step, added);
	}

	/**
	 * Works out whose use a rule counts, from the step's limit type and the rule's
	 * counts: the model for the Model Total, the person on the model for a Direct
	 * Limit, the team or each member for a Team Limit, the team for its Team Total,
	 * public use or the person for the Public Use Limit, and the person across all
	 * models for the User's Total Model Limit and the General Platform Limit.
	 *
	 * @param path      the person's path
	 * @param step      a step
	 * @param rule      one of its rules
	 * @param teamTotal whether the rule is the team's total
	 * @return whose use the rule counts
	 */
	static ModelUsageCounterScope scopeOf(ModelUsageLimitPath path, Step step, ModelUsageLimitRule rule,
			boolean teamTotal) {
		String engineId = path.getEngineId();
		String userId = path.getUserId();
		String userType = path.getUserType();
		switch (step.getLimitType()) {
		case MODEL_TOTAL:
			return ModelUsageCounterScope.modelTotal(engineId);
		case DIRECT:
			return ModelUsageCounterScope.personModel(engineId, userId, userType);
		case TEAM:
			if (teamTotal) {
				ModelUsageLimitKey key = step.getTeamTotalKey() == null ? step.getKey() : step.getTeamTotalKey();
				return ModelUsageCounterScope.teamTotal(key.subjectId(), key.subjectType());
			}
			if (rule.getCounts() == Counts.SHARED) {
				return ModelUsageCounterScope.teamShared(engineId, step.getKey().subjectId(),
						step.getKey().subjectType());
			}
			return ModelUsageCounterScope.teamMember(engineId, step.getKey().subjectId(), step.getKey().subjectType(),
					userId, userType);
		case PUBLIC:
			if (rule.getCounts() == Counts.SHARED) {
				return ModelUsageCounterScope.publicShared(engineId);
			}
			return ModelUsageCounterScope.personModel(engineId, userId, userType);
		case ALL_MODELS:
		case PLATFORM:
			return ModelUsageCounterScope.personAllModels(userId, userType);
		default:
			throw new IllegalStateException("Unknown step " + step.getLimitType());
		}
	}

	/**
	 * Builds a counter's key, for a window of length 1.
	 *
	 * @param scope       whose use the counter counts
	 * @param metric      what it counts
	 * @param window      its window
	 * @param bucketStart the start of its bucket
	 * @return the counter's key
	 */
	static String keyOf(ModelUsageCounterScope scope, Metric metric, Window window, Instant bucketStart) {
		return keyOf(scope, metric, window, 1, bucketStart);
	}

	/**
	 * Builds a counter's key from {@link #KEY_PREFIX}, the scope, the metric and
	 * the bucket's {@link ModelUsageWindows#bucketId}, so every server of a cluster
	 * uses the same key.
	 *
	 * @param scope       whose use the counter counts
	 * @param metric      what it counts
	 * @param window      its window
	 * @param length      how many minutes or hours a sliding window is
	 * @param bucketStart the start of its bucket
	 * @return the counter's key
	 */
	static String keyOf(ModelUsageCounterScope scope, Metric metric, Window window, int length, Instant bucketStart) {
		return KEY_PREFIX + scope.id() + "|" + metric.name() + "|"
				+ ModelUsageWindows.bucketId(window, length, bucketStart);
	}

	/**
	 * Reads every rule's counter, and fills day and longer counters, and the
	 * buckets of sliding windows of an hour or longer, from the logs the first time
	 * they are read.
	 *
	 * @param store  the counters
	 * @param seeder fills counters from the logs, or null to leave them as they are
	 * @throws ModelUsageStoreException when the counters cannot be reached
	 */
	void read(IModelUsageCounterStore store, Seeder seeder) {
		if (checks.isEmpty()) {
			return;
		}
		Set<String> keys = new LinkedHashSet<>();
		for (RuleCheck check : checks) {
			keys.add(check.key);
			if (check.previousKey != null) {
				keys.add(check.previousKey);
			}
		}
		// one read of the store for every counter, so a call costs one round trip to
		// Redis however many rules apply
		Map<String, ModelUsageCount> counts = store.get(keys);
		// what this read found in the logs, so rules that share a counter query once
		Map<String, Long> seeded = new java.util.HashMap<>();
		Set<String> unseedable = new HashSet<>();
		for (RuleCheck check : checks) {
			ModelUsageCount count = counts.getOrDefault(check.key, ModelUsageCount.EMPTY);
			check.used = count.used();
			check.reserved = count.reserved();
			boolean seeds = seeder != null && check.seedsFromLogs();
			if (seeds) {
				Long fromLogs = seedOnce(store, seeder, seeded, unseedable, check, check.key, count, check.bucketStart,
						check.bucketEnd, check.expiresAtMillis);
				// the larger, never the sum: calls the counter counted itself may
				// already be in the logs
				if (fromLogs != null) {
					check.used = Math.max(check.used, fromLogs);
				}
			}
			if (check.previousKey != null) {
				ModelUsageCount previous = counts.getOrDefault(check.previousKey, ModelUsageCount.EMPTY);
				check.previousTotal = previous.total();
				check.previousUsed = previous.used();
				if (seeds) {
					// the previous bucket ends where the current one starts
					Long fromLogs = seedOnce(store, seeder, seeded, unseedable, check, check.previousKey, previous,
							check.previousStart, check.bucketStart,
							ModelUsageWindows.counterExpiresAt(check.window(), check.length(), check.previousStart));
					// raise used to what the logs say and the total with it, keeping what
					// the previous bucket still has reserved
					if (fromLogs != null && fromLogs > check.previousUsed) {
						check.previousTotal += fromLogs - check.previousUsed;
						check.previousUsed = fromLogs;
					}
				}
				// the previous bucket counts by how much of it is still inside the window
				check.carried = ModelUsageWindows.carried(check.window(), check.length(), now, check.previousTotal);
			}
			check.read = true;
		}
	}

	/**
	 * Fills a counter from the logs the first time it is read, once per counter.
	 * The store's seeded flag says whether any server has filled it already;
	 * {@code seeded} and {@code unseedable} hold what this read found, so two rules
	 * on one counter query the logs once.
	 *
	 * @return what the logs say was used in the bucket, or null when the counter is
	 *         already filled or the logs cannot say
	 */
	private static Long seedOnce(IModelUsageCounterStore store, Seeder seeder, Map<String, Long> seeded,
			Set<String> unseedable, RuleCheck check, String key, ModelUsageCount count, Instant start, Instant end,
			long expiresAtMillis) {
		if (count.seeded()) {
			return null;
		}
		Long fromLogs = seeded.get(key);
		if (fromLogs == null && !unseedable.contains(key)) {
			fromLogs = seedFromLogs(store, seeder, check, key, start, end, expiresAtMillis);
			if (fromLogs == null) {
				unseedable.add(key);
			} else {
				seeded.put(key, fromLogs);
			}
		}
		return fromLogs;
	}

	/**
	 * Reads the logs for a counter and fills it, one read at a time for each
	 * counter: a call that finds another reading the same counter waits for its
	 * answer. When the logs cannot be read, the counter is left unfilled, and they
	 * are not read again for {@link #SEED_RETRY}.
	 *
	 * @return what the logs say was used in the bucket, or null when they cannot
	 *         say
	 * @throws ModelUsageStoreException when the counters cannot be reached
	 */
	private static Long seedFromLogs(IModelUsageCounterStore store, Seeder seeder, RuleCheck check, String key,
			Instant start, Instant end, long expiresAtMillis) {
		// a failure is remembered for SEED_RETRY, so logs that cannot be read are not
		// queried again on every call; the call goes on with what the counter holds
		Instant retryAt = SEED_FAILURES.getIfPresent(key);
		if (retryAt != null && check.now.isBefore(retryAt)) {
			return null;
		}
		// single flight on this server: the first call for a key reads the logs, and
		// the calls that come in meanwhile wait for its answer
		CompletableFuture<Long> reading = new CompletableFuture<>();
		CompletableFuture<Long> running = SEEDING.putIfAbsent(key, reading);
		if (running != null) {
			try {
				return running.join();
			} catch (CompletionException e) {
				if (e.getCause() instanceof RuntimeException) {
					throw (RuntimeException) e.getCause();
				}
				throw e;
			} catch (CancellationException e) {
				return null;
			}
		}
		try {
			Long fromLogs = seeder.usedInLogs(check.scope, check.metric(), start, end);
			if (fromLogs == null) {
				SEED_FAILURES.put(key, check.now.plus(SEED_RETRY));
			} else {
				// the store marks the counter seeded, so other servers of a cluster
				// leave it as it is
				store.seed(key, fromLogs, expiresAtMillis);
				SEED_FAILURES.invalidate(key);
			}
			reading.complete(fromLogs);
			return fromLogs;
		} catch (RuntimeException | Error e) {
			// the waiting calls get the same failure, such as a store that cannot be
			// reached
			reading.completeExceptionally(e);
			throw e;
		} finally {
			SEEDING.remove(key, reading);
		}
	}

	/**
	 * Forgets which counters' logs could not be read, so they are read again.
	 */
	static void clearSeeding() {
		SEED_FAILURES.invalidateAll();
	}

	/**
	 * Works out which ceilings have usage left and which source a call is billed
	 * to.
	 *
	 * <ul>
	 * <li>A ceiling with nothing left refuses the call, whichever source is
	 * billed.</li>
	 * <li>A source that is not a team is the first in the billing order, and
	 * refuses the call when it has nothing left.</li>
	 * <li>Teams are tried in the billing order: the first with usage left in both
	 * its limit on the model and its Team Total is billed, and later ones with
	 * usage left are where calls go next. With no team left, the call is refused
	 * with the team rule that resets soonest.</li>
	 * </ul>
	 */
	void decide() {
		if (decided) {
			return;
		}
		decided = true;
		// every ceiling with rules is marked, so the path view shows each one, but
		// only the first with nothing left is named in the refusal
		for (Step ceiling : path.getCeilings()) {
			List<RuleCheck> ceilingChecks = limitChecks.getOrDefault(ceiling, List.of());
			if (!ceiling.isDefined() || ceiling.isUnlimited() || ceilingChecks.isEmpty()) {
				continue;
			}
			RuleCheck failing = firstUsedUp(ceilingChecks);
			states.put(ceiling, failing == null ? State.CHECKED : State.REACHED);
			if (failing != null && refusedBy == null) {
				refusedBy = ceiling;
				refusedRule = failing;
			}
		}

		StepLimitType billedLimitType = path.getBilledLimitType();
		List<Step> order = path.getBillingOrder();
		// no source with a limit defined: only the ceilings apply
		if (billedLimitType == null || order.isEmpty()) {
			return;
		}
		// a direct, public use, all-models or platform limit does not pass a call on
		if (billedLimitType != StepLimitType.TEAM) {
			Step step = order.get(0);
			RuleCheck failing = step.isUnlimited() ? null : firstUsedUp(limitChecks.getOrDefault(step, List.of()));
			states.put(step, failing == null ? State.BILLED : State.REACHED);
			if (failing == null) {
				billed = step;
			} else if (refusedBy == null) {
				refusedBy = step;
				refusedRule = failing;
			}
			return;
		}

		// spill-over: a team with nothing left passes the call on to the next team in
		// the
		// billing order; every team is marked, for the path view
		RuleCheck soonest = null;
		for (Step team : order) {
			RuleCheck failing = teamUsedUp(team);
			if (failing == null && billed == null) {
				billed = team;
				states.put(team, State.BILLED);
			} else if (failing == null) {
				states.put(team, State.NEXT);
			} else {
				states.put(team, State.REACHED);
				teamsReached.add(team);
				if (soonest == null || failing.resetsAt(now).isBefore(soonest.resetsAt(now))) {
					soonest = failing;
				}
			}
		}
		// no team has usage left: the refusal names the rule that resets soonest, so
		// its
		// resetsAt is when a call can next get through
		if (billed == null && refusedBy == null) {
			refusedBy = order.get(0);
			refusedRule = soonest;
		}
	}

	/**
	 * Checks a team's limit on the model, unless it is unlimited, and then its Team
	 * Total, for the first rule with nothing left.
	 *
	 * @param team a team step
	 * @return the first rule with nothing left in the team's limit or its total, or
	 *         null when the team can take a call
	 */
	private RuleCheck teamUsedUp(Step team) {
		RuleCheck failing = team.isUnlimited() ? null : firstUsedUp(limitChecks.getOrDefault(team, List.of()));
		if (failing == null) {
			failing = firstUsedUp(teamTotalChecks.getOrDefault(team, List.of()));
		}
		return failing;
	}

	private static RuleCheck firstUsedUp(List<RuleCheck> ruleChecks) {
		for (RuleCheck check : ruleChecks) {
			if (!check.hasUsageLeft()) {
				return check;
			}
		}
		return null;
	}

	/**
	 * Decides what happens when the counters cannot be reached: a call a rule can
	 * refuse is refused as unavailable, and any other call runs uncounted.
	 *
	 * @return whether a rule can refuse the call: a ceiling or a source billed has
	 *         a rule that counts
	 */
	boolean enforces() {
		for (RuleCheck check : checksForCall()) {
			if (check.applies) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Collects the rules the call is checked against, which {@link #enforces},
	 * {@link #left}, {@link #tightest} and {@link #checkForCall} work from.
	 *
	 * @return the rules a call must fit in: the ceilings' and, once decided, those
	 *         of the source billed with a team's total; before deciding, those of
	 *         every step a call can be billed to
	 */
	List<RuleCheck> checksForCall() {
		List<RuleCheck> forCall = new ArrayList<>();
		for (Step ceiling : path.getCeilings()) {
			forCall.addAll(limitChecks.getOrDefault(ceiling, List.of()));
		}
		List<Step> steps = decided ? (billed == null ? List.of() : List.of(billed)) : path.getBillingOrder();
		for (Step step : steps) {
			if (!step.isUnlimited()) {
				forCall.addAll(limitChecks.getOrDefault(step, List.of()));
			}
			forCall.addAll(teamTotalChecks.getOrDefault(step, List.of()));
		}
		return forCall;
	}

	/**
	 * Works out the usage a call has left for a metric, which the ticket sends the
	 * Python call as its ceiling.
	 *
	 * @param metric a metric
	 * @return what is left on the tightest rule of the metric that the call must
	 *         fit in, or null when no such rule counts
	 */
	Long left(Metric metric) {
		Long left = null;
		for (RuleCheck check : checksForCall()) {
			if (check.applies && check.metric() == metric) {
				left = left == null ? check.left() : Math.min(left, check.left());
			}
		}
		return left;
	}

	/**
	 * Finds the token or cost rule closest to its amount, comparing them by the
	 * share of the amount left, which a refusal for a call that does not fit names.
	 *
	 * @return the token or cost rule the call must fit in with the least left, or
	 *         null
	 */
	RuleCheck tightest() {
		RuleCheck tightest = null;
		for (RuleCheck check : checksForCall()) {
			if (!check.applies || check.metric() == Metric.COMPUTE_TIME) {
				continue;
			}
			// tokens and cost are compared by the share of the amount left, cross
			// multiplied so an amount of 0 does not divide
			if (tightest == null || check.left() * (double) tightest.amount < tightest.left() * (double) check.amount) {
				tightest = check;
			}
		}
		return tightest;
	}

	/**
	 * Finds the rule a hold on a counter is checked against, so the store refuses
	 * the hold when another call filled the counter since the read.
	 *
	 * @param key a counter key
	 * @return a rule the call must fit in that counts on the counter, the one with
	 *         the lowest amount when several do, or null when the call is only
	 *         billed on the counter
	 */
	RuleCheck checkForCall(String key) {
		RuleCheck found = null;
		for (RuleCheck check : checksForCall()) {
			if (check.applies && check.key.equals(key) && (found == null || check.amount < found.amount)) {
				found = check;
			}
		}
		return found;
	}

	/**
	 * The rule checks of a step's limit, or of its Team Total when
	 * {@code teamTotal} is true, which the path view and the response's usage
	 * limits show.
	 *
	 * @param step      a step
	 * @param teamTotal whether to give the checks of a team step's Team Total
	 *                  rather than of its limit on the model
	 * @return the checks, or none when that limit has no rules
	 */
	List<RuleCheck> checksOf(Step step, boolean teamTotal) {
		return (teamTotal ? teamTotalChecks : limitChecks).getOrDefault(step, List.of());
	}

	/**
	 * The state {@link #decide} found for a step, such as billed, next or reached,
	 * for the path view.
	 *
	 * @param step a step
	 * @return the state found for the step, or null to keep the resolver's
	 */
	State stateOf(Step step) {
		return states.get(step);
	}

	ModelUsageLimitPath getPath() {
		return path;
	}

	Instant getNow() {
		return now;
	}

	boolean isPricesKnown() {
		return pricesKnown;
	}

	Step getBilled() {
		return billed;
	}

	boolean isRefused() {
		return refusedBy != null;
	}

	Step getRefusedBy() {
		return refusedBy;
	}

	RuleCheck getRefusedRule() {
		return refusedRule;
	}

	List<Step> getTeamsReached() {
		return Collections.unmodifiableList(teamsReached);
	}

	List<RuleCheck> getChecks() {
		return Collections.unmodifiableList(checks);
	}
}
