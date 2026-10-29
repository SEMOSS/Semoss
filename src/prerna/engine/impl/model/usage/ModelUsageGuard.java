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

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import prerna.auth.User;
import prerna.auth.access.ModelUsageLimit;
import prerna.auth.access.ModelUsageLimitPath;
import prerna.auth.access.ModelUsageLimitPath.Step;
import prerna.auth.access.ModelUsageLimitPath.StepLimitType;
import prerna.auth.access.ModelUsageLimitRule;
import prerna.auth.access.ModelUsageLimitRule.Counts;
import prerna.auth.access.ModelUsageLimitRule.Metric;
import prerna.auth.utils.ModelUsageLimitResolver;
import prerna.auth.utils.SecurityModelUsageLimitUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.inferencetracking.ModelUsageLogFilter;
import prerna.engine.impl.model.inferencetracking.ModelUsageLogTotals;
import prerna.engine.impl.model.usage.ModelUsageEvaluation.RuleCheck;
import prerna.engine.impl.model.usage.ModelUsageTicket.ChargeTarget;
import prerna.redis.RedisConnectionConfig;
import prerna.util.Utility;
import prerna.util.ValueUtils;

/**
 * Admits model calls under the usage limits that apply to the person calling.
 *
 * <p>
 * Every model call passes through here. {@code AbstractModelEngine} (ask and
 * embeddings), {@code TypeSafeEngine}, {@code EmbeddedModelEngine} and
 * {@code ModelBatchManager} call {@code ModelUsageRestrictionUtility.admit},
 * which calls {@link #admit(User, IModelEngine, ModelUsageRequest)}, before the
 * call starts. The call then runs with its usage left passed to Python as the
 * {@code usage_ceiling} keyword argument
 * ({@link ModelUsageTicket#pythonCeiling()}). When it ends,
 * {@link ModelUsageTicket#bill(ModelUsageReport)} replaces the reservation with
 * what was used; when it fails, {@link ModelUsageTicket#release()} gives the
 * reservation back. An async call is first checked with
 * {@link #check(User, IModelEngine)}, so a refusal is answered before its job
 * starts.
 * </p>
 *
 * <ol>
 * <li><b>Resolve</b>: the person's path on the model, from
 * {@link ModelUsageLimitResolver}, cached for 30 seconds by primary login and
 * model and read again as soon as
 * {@link SecurityModelUsageLimitUtils#version()} changes. The version is per
 * server, so on a cluster the other servers see a change once their cached path
 * expires, within 30 seconds.</li>
 * <li><b>Read</b>: each rule's counter for the window bucket that holds now
 * ({@link ModelUsageWindows}), with the previous bucket too for a sliding
 * window.</li>
 * <li><b>Check</b>: every ceiling must have usage left. The source billed is
 * the first one with a limit defined: a direct, public use, all-models or
 * platform limit with nothing left refuses the call; a team with nothing left
 * in its limit on the model or in its team total passes the call on to the next
 * team in the billing order, and no team left refuses it. A rule has nothing
 * left when what is used and reserved is at or above its amount.</li>
 * <li><b>Reserve</b>: what is reserved, for tokens and for cost, is the lesser
 * of what is left on the tightest rule and a generous estimate of the prompt
 * plus the most output the call asks for, which is always bounded: the caller's
 * maximum, else the model's, else
 * {@link ModelUsageRequest#DEFAULT_MAX_OUTPUT_TOKENS}. So no call reserves the
 * whole of what is left, and calls in flight share it. It is reserved on every
 * token and cost counter the call is billed on. The Python call is given what
 * is left and fits in it with its own count of the prompt; it refuses the call
 * with {@link ModelUsageTicket#DOES_NOT_FIT_REASON} when less than
 * {@link ModelUsageTicket#MIN_OUTPUT_TOKENS} would be left for output. The
 * estimate is worked out only when a token or cost rule applies. Compute time
 * rules are checked but not reserved, and a call that starts runs to the end,
 * so it may go over.</li>
 * <li><b>Bill</b>: {@link ModelUsageTicket#bill(ModelUsageReport)}.</li>
 * </ol>
 *
 * <p>
 * A refusal is a {@link ModelUsageLimitException}, which the endpoints answer
 * with 429 and its {@code resetsAt}. Counters are kept by
 * {@link LocalModelUsageCounterStore} on a single node and by
 * {@link RedisModelUsageCounterStore} when SEMOSS runs in a cluster with Redis
 * turned on. When the
 * counters cannot be reached, calls that a limit applies to are refused with
 * {@link ModelUsageLimitException#unavailable()}, which the endpoints answer
 * with 503; calls with no limit still run. Day and longer counters, and sliding
 * windows of an hour or longer, are filled from the inference logs the first
 * time they are read.
 * </p>
 */
public final class ModelUsageGuard {

	private static final Logger classLogger = LogManager.getLogger(ModelUsageGuard.class);

	/**
	 * How long a resolved path is kept; it is also the longest another server of a
	 * cluster keeps a path after a limit changes, since the version is per server
	 */
	private static final Duration PATH_CACHE_TIME = Duration.ofSeconds(30);
	private static final long MAXIMUM_PATHS = 50_000L;
	/**
	 * How many times a call reads the counters again when another call reserved
	 * what it found left between its read and its reserve
	 */
	private static final int RESERVE_ATTEMPTS = 3;
	/**
	 * A character no login id, login type or engine id holds, so cache keys cannot
	 * collide
	 */
	private static final String KEY_SEPARATOR = "\u0000";

	/**
	 * Resolved paths by primary login and model, with the limits version they were
	 * read at
	 */
	private static final Cache<String, CachedPath> PATHS = Caffeine.newBuilder().maximumSize(MAXIMUM_PATHS)
			.expireAfterWrite(PATH_CACHE_TIME).build();
	private static final Object STORE_LOCK = new Object();

	// the counters are made once, on first use (see store()); each of these can
	// be replaced through its setter at the end of the class
	private static volatile IModelUsageCounterStore counterStore;
	private static volatile Clock clock = Clock.systemUTC();
	private static volatile ModelUsageEvaluation.Seeder seeder = ModelUsageGuard::usedInLogs;
	private static volatile Function<String, Map<String, Object>> prices = ModelUsageCost::priceEntry;

	/** A resolved path, with the limits version it was resolved at */
	private record CachedPath(ModelUsageLimitPath path, long version) {
	}

	private ModelUsageGuard() {

	}

	/**
	 * Admits a call and reserves its ceiling.
	 *
	 * @param user    the person calling; a call with no user, made by SEMOSS
	 *                itself, has no limits
	 * @param engine  the model
	 * @param request what the call asks for
	 * @return the ticket to bill when the call ends, or to release when it fails
	 * @throws ModelUsageLimitException when a limit refuses the call, or the limits
	 *                                  cannot be checked
	 * @throws IllegalArgumentException for an anonymous user, who cannot use models
	 */
	public static ModelUsageTicket admit(User user, IModelEngine engine, ModelUsageRequest request) {
		if (user == null || engine == null || ValueUtils.isBlank(engine.getEngineId())) {
			return ModelUsageTicket.none();
		}
		ModelUsageLimitPath path = pathOf(user, engine.getEngineId());
		// a caller that names nothing is admitted as an ask with no prompt estimate
		// and the default output maximum
		ModelUsageRequest call = request == null ? ModelUsageRequest.ask(0, null) : request;
		return admit(path, modelName(engine), prices.apply(engine.getEngineId()), call);
	}

	/**
	 * Checks that a call would be admitted, without reserving anything, such as
	 * before a streaming call is started.
	 *
	 * @param user   the person calling
	 * @param engine the model
	 * @throws ModelUsageLimitException when a limit would refuse the call, or the
	 *                                  limits cannot be checked
	 */
	public static void check(User user, IModelEngine engine) {
		if (user == null || engine == null || ValueUtils.isBlank(engine.getEngineId())) {
			return;
		}
		ModelUsageLimitPath path = pathOf(user, engine.getEngineId());
		Map<String, Object> priceEntry = prices.apply(engine.getEngineId());
		ModelUsageEvaluation evaluation = ModelUsageEvaluation.of(path, false, priceEntry != null, clock.instant());
		try {
			evaluation.read(store(), seeder);
		} catch (ModelUsageStoreException e) {
			// as in admit: counters that cannot be reached refuse only a call a rule
			// applies to
			if (evaluation.enforces()) {
				throw ModelUsageLimitException.unavailable();
			}
			return;
		}
		evaluation.decide();
		if (evaluation.isRefused()) {
			throw ModelUsageFormat.refusal(evaluation, modelName(engine));
		}
	}

	/**
	 * The person's path on the model. It is cached for {@link #PATH_CACHE_TIME} by
	 * primary login and model, and resolved again as soon as
	 * {@link SecurityModelUsageLimitUtils#version()} differs from the version it
	 * was cached at. The version changes only on the server a limit, grant or team
	 * was written on, so the other servers of a cluster see the change when their
	 * cached path expires.
	 *
	 * @param user     the person
	 * @param engineId the model
	 * @return how limits apply to the person on the model; kept briefly and shared,
	 *         so it must not be changed
	 * @throws IllegalArgumentException for an anonymous user
	 */
	public static ModelUsageLimitPath pathOf(User user, String engineId) {
		if (user == null || user.isAnonymous()) {
			// throws, with the reason
			ModelUsageLimitResolver.personOf(user);
		}
		// keyed by the primary login, the login the person's counters are kept under
		Pair<String, String> primary = User.getPrimaryUserIdAndTypePair(user);
		String key = primary.getValue0() + KEY_SEPARATOR + primary.getValue1() + KEY_SEPARATOR + engineId;
		// read before resolving: a change written while the path resolves bumps
		// the version past this one, so the next call resolves again
		long version = SecurityModelUsageLimitUtils.version();
		CachedPath cached = PATHS.getIfPresent(key);
		if (cached != null && cached.version() == version) {
			return cached.path();
		}
		ModelUsageLimitPath path = ModelUsageLimitResolver.resolve(ModelUsageLimitResolver.personOf(user), engineId);
		PATHS.put(key, new CachedPath(path, version));
		return path;
	}

	/**
	 * A ticket that bills usage without checking or reserving anything, for usage
	 * that comes in after its reservation is gone, such as the results of a batch
	 * submitted on another server. The source billed is the one a call would be
	 * billed to now, or the first in the billing order when none has usage left.
	 *
	 * @param user   the person the usage belongs to
	 * @param engine the model
	 * @return the ticket; bill each part with
	 *         {@link ModelUsageTicket#billPart(ModelUsageReport)}
	 */
	public static ModelUsageTicket forBilling(User user, IModelEngine engine) {
		if (user == null || engine == null || ValueUtils.isBlank(engine.getEngineId())) {
			return ModelUsageTicket.none();
		}
		ModelUsageLimitPath path = pathOf(user, engine.getEngineId());
		Map<String, Object> priceEntry = prices.apply(engine.getEngineId());
		boolean pricesKnown = priceEntry != null;
		ModelUsageEvaluation evaluation = ModelUsageEvaluation.of(path, false, pricesKnown, clock.instant());
		IModelUsageCounterStore store = hasRules(path) ? store() : null;
		if (store != null) {
			try {
				evaluation.read(store, seeder);
			} catch (ModelUsageStoreException e) {
				// the usage has already happened, so counters that cannot be reached
				// leave it unbilled rather than fail the read of the results
				store = null;
			}
		}
		evaluation.decide();
		Step billed = evaluation.getBilled();
		// nothing is refused here: usage that came in is billed even when no source
		// has usage left, to the first source in the billing order
		if (billed == null && !path.getBillingOrder().isEmpty()) {
			billed = path.getBillingOrder().get(0);
		}
		List<ChargeTarget> targets = store == null ? List.of() : chargeTargets(path, billed, false, pricesKnown);
		return new ModelUsageTicket(store, clock, evaluation, modelName(engine), priceEntry, targets, null, Map.of(),
				ModelUsageTicket.Allowance.NONE, false, billedTo(path, billed));
	}

	/**
	 * Marks something as done once across every server, such as one batch result
	 * billed, so a second server or a second read does not do it again. On a
	 * cluster the mark is a Redis {@code SET NX}, so exactly one server makes it.
	 *
	 * @param key the mark
	 * @param ttl how long the mark is kept
	 * @return whether this call made the mark
	 * @throws ModelUsageStoreException when the counters cannot be reached
	 */
	public static boolean markOnce(String key, Duration ttl) {
		return store().markOnce(key, ttl);
	}

	/**
	 * The core of {@link #admit(User, IModelEngine, ModelUsageRequest)}, for a path
	 * already resolved.
	 *
	 * <ol>
	 * <li>Read every rule's counter and decide: refused, or the source billed, with
	 * a team that has nothing left passing the call on to the next team.</li>
	 * <li>Work out what is left on the tightest token and cost rule the call must
	 * fit in, and the estimate, the prompt estimate plus the bounded output
	 * maximum. Each token and cost counter reserves the lesser of the two.</li>
	 * <li>Reserve on every token and cost counter the call is billed on, all or
	 * none. When another call took what was left between the read and the reserve,
	 * read again, up to {@link #RESERVE_ATTEMPTS} times, then refuse the call as
	 * one that does not fit.</li>
	 * </ol>
	 *
	 * @param path       the person's path on the model
	 * @param modelName  the model's name, for messages
	 * @param priceEntry the model's price entry, or null when it has no known
	 *                   prices
	 * @param request    what the call asks for
	 * @return the ticket
	 */
	static ModelUsageTicket admit(ModelUsageLimitPath path, String modelName, Map<String, Object> priceEntry,
			ModelUsageRequest request) {
		boolean pricesKnown = priceEntry != null;
		// a path with no rules has no counters, so the call is only told the source
		// it is billed to
		IModelUsageCounterStore store = hasRules(path) ? store() : null;
		for (int attempt = 1;; attempt++) {
			Instant now = clock.instant();
			ModelUsageEvaluation evaluation = ModelUsageEvaluation.of(path, false, pricesKnown, now);
			if (store != null) {
				try {
					evaluation.read(store, seeder);
				} catch (ModelUsageStoreException e) {
					// counters that cannot be reached, such as Redis on a cluster, refuse
					// only a call a rule applies to (503); any other call runs uncounted
					if (evaluation.enforces()) {
						throw ModelUsageLimitException.unavailable();
					}
					return unbilled(evaluation, modelName, priceEntry, request);
				}
			}
			evaluation.decide();
			if (evaluation.isRefused()) {
				throw ModelUsageFormat.refusal(evaluation, modelName);
			}

			List<ChargeTarget> targets = store == null ? List.of()
					: chargeTargets(path, evaluation.getBilled(), request.isRouter(), pricesKnown);
			// what is left on the tightest rule of the ceilings, the
			// source billed and a billed team's total; null when no rule of the
			// metric applies
			Long tokensLeft = evaluation.left(Metric.TOKENS);
			Long costLeft = pricesKnown ? evaluation.left(Metric.COST) : null;
			// the estimate reads the whole request, media included, so it is worked out
			// only when a token or cost rule needs it
			boolean estimates = tokensLeft != null || costLeft != null || reservesOn(targets);
			long promptEstimate = estimates ? request.getPromptEstimateTokens() : 0;
			long tokensEstimate = estimates ? request.estimatedTokens() : 0;
			Long costEstimate = pricesKnown && estimates
					? ModelUsageCost.estimateMicros(priceEntry, promptEstimate, request.getMaxOutputTokens(),
							request.isEmbeddings())
					: null;
			// the reservation is the lesser of what is left and the estimate, so no call
			// holds the whole of what is left while it runs; with no rule of the
			// metric, the counters it is billed on still hold the estimate
			long tokensReserve = tokensLeft == null ? tokensEstimate : Math.min(tokensLeft, tokensEstimate);
			long costReserve = costLeft == null ? orZero(costEstimate)
					: costEstimate == null ? costLeft : Math.min(costLeft, costEstimate);
			// what is left goes to Python as the ceiling; what is reserved stays here
			ModelUsageTicket.Allowance allowance = new ModelUsageTicket.Allowance(tokensLeft, costLeft,
					tokensLeft == null ? null : tokensReserve, costLeft == null ? null : costReserve, promptEstimate,
					request.isEmbeddings() ? null : request.getMaxOutputTokens());

			List<ModelUsageHold> holds = new ArrayList<>();
			Map<String, Metric> reservedMetrics = new LinkedHashMap<>();
			for (ChargeTarget target : targets) {
				// compute time is known only once the call ends, so it is checked but
				// never reserved
				if (target.metric() == Metric.COMPUTE_TIME) {
					continue;
				}
				long amount = target.metric() == Metric.TOKENS ? tokensReserve : costReserve;
				if (amount <= 0) {
					continue;
				}
				Instant bucketStart = ModelUsageWindows.bucketStart(target.window(), target.length(), now);
				String key = ModelUsageEvaluation.keyOf(target.scope(), target.metric(), target.window(),
						target.length(), bucketStart);
				// a counter a rule of this call is checked on carries its amount, so the
				// store refuses the hold if another call filled it since the read; a
				// counter the call is only billed on has no limit and always fits
				RuleCheck check = evaluation.checkForCall(key);
				holds.add(new ModelUsageHold(key, amount, check == null ? null : check.amount,
						check == null ? 0 : check.carried,
						ModelUsageWindows.counterExpiresAt(target.window(), target.length(), bucketStart)));
				reservedMetrics.put(key, target.metric());
			}

			ModelUsageReservation reservation = null;
			// holds come only from counters, so a path with no store has none
			if (store != null && !holds.isEmpty()) {
				try {
					// all holds or none; the reservation expires after the request's
					// reservation time, so a call lost with its server stops holding usage
					reservation = store.reserve(holds, request.getReservation());
				} catch (ModelUsageStoreException e) {
					if (evaluation.enforces()) {
						throw ModelUsageLimitException.unavailable();
					}
					return unbilled(evaluation, modelName, priceEntry, request);
				}
				if (reservation == null) {
					// another call reserved what was left in the meantime
					if (attempt < RESERVE_ATTEMPTS) {
						continue;
					}
					throw ModelUsageFormat.doesNotFit(evaluation, modelName, request.isEmbeddings());
				}
			}
			return new ModelUsageTicket(store, clock, evaluation, modelName, priceEntry, targets, reservation,
					reservedMetrics, allowance, request.isEmbeddings(), billedTo(path, evaluation.getBilled()));
		}
	}

	/**
	 * Whether a call billed on these counters reserves anything, which it does when
	 * any of them counts tokens or cost.
	 *
	 * @return whether any of the counters holds tokens or cost, so a reservation is
	 *         made on it
	 */
	private static boolean reservesOn(List<ChargeTarget> targets) {
		for (ChargeTarget target : targets) {
			if (target.metric() == Metric.TOKENS || target.metric() == Metric.COST) {
				return true;
			}
		}
		return false;
	}

	/**
	 * A ticket for a call whose counters cannot be reached but that no limit can
	 * refuse: it runs, and is not counted.
	 */
	private static ModelUsageTicket unbilled(ModelUsageEvaluation evaluation, String modelName,
			Map<String, Object> priceEntry, ModelUsageRequest request) {
		// TODO: the call is in the inference logs but not in the counters it would
		// add to, such as the person's User's Total Model Limit, and a counter still
		// filled when the counters are back never picks it up. Charge those counters
		// once they can be reached again, or fill them from the logs again.
		classLogger.warn("Model usage counters cannot be reached; a call with no limit runs without being counted");
		evaluation.decide();
		return new ModelUsageTicket(null, clock, evaluation, modelName, priceEntry, List.of(), null, Map.of(),
				ModelUsageTicket.Allowance.NONE, request.isEmbeddings(),
				billedTo(evaluation.getPath(), evaluation.getBilled()));
	}

	/**
	 * Reads what each rule on a person's path has counted and decides the source
	 * billed, for the path view. Nothing is reserved.
	 *
	 * @param path a person's path on a model
	 * @return the path for the path view, with what each rule has counted, or null
	 *         when the counters cannot be reached
	 */
	static ModelUsageEvaluation evaluateForView(ModelUsageLimitPath path) {
		Map<String, Object> priceEntry = prices.apply(path.getEngineId());
		ModelUsageEvaluation evaluation = ModelUsageEvaluation.of(path, true, priceEntry != null, clock.instant());
		if (!evaluation.getChecks().isEmpty()) {
			try {
				evaluation.read(store(), seeder);
			} catch (ModelUsageStoreException e) {
				return null;
			}
		}
		evaluation.decide();
		return evaluation;
	}

	/**
	 * Whether a person's path on a model has any rules, on its ceilings, its steps
	 * or a team's Team Total. A path with none needs no counters.
	 *
	 * @param path a person's path on a model
	 * @return whether any step or ceiling has rules, so the call has counters
	 */
	static boolean hasRules(ModelUsageLimitPath path) {
		for (Step step : path.getCeilings()) {
			if (hasRules(step.getLimit())) {
				return true;
			}
		}
		for (Step step : path.getSteps()) {
			if (hasRules(step.getLimit()) || hasRules(step.getTeamTotal())) {
				return true;
			}
		}
		return false;
	}

	private static boolean hasRules(ModelUsageLimit limit) {
		return limit != null && !limit.isUnlimited() && !limit.getRules().isEmpty();
	}

	/**
	 * Every counter a call on the path is billed on. The counters that count across
	 * models leave out a router model's own call, since the model it routes to
	 * counts the call there, so a routed call counts once.
	 *
	 * <ul>
	 * <li>The ceilings, such as the model's total: always.</li>
	 * <li>The source billed and, for a team, its Team Total. A router's own call
	 * skips the Team Total, and skips the source billed when that is the User's
	 * Total Model Limit or the General Platform Limit.</li>
	 * <li>The person's own counters on the model, for the Direct Limit and the
	 * Public Use Limit's each-person rules: on every call, whichever source is
	 * billed, since they count all of the person's use of the model.</li>
	 * <li>The person's counters across all models, for the User's Total Model Limit
	 * and the General Platform Limit: on every call but a router's own.</li>
	 * </ul>
	 *
	 * @param path        the person's path
	 * @param billed      the source billed, or null
	 * @param router      whether the call is a router model's own
	 * @param pricesKnown whether the model has known prices, so cost counts
	 * @return the counters, each once
	 */
	static List<ChargeTarget> chargeTargets(ModelUsageLimitPath path, Step billed, boolean router,
			boolean pricesKnown) {
		// keyed by counter, so a counter reached two ways, such as a billed Direct
		// Limit and the Direct step below, is charged once
		Map<String, ChargeTarget> targets = new LinkedHashMap<>();
		for (Step ceiling : path.getCeilings()) {
			addTargets(targets, path, ceiling, ceiling.getLimit(), false, pricesKnown, false);
		}
		// the User's Total Model Limit and the General Platform Limit count across
		// models, so a router's own call is not billed to them
		boolean acrossModels = billed != null && (billed.getLimitType() == StepLimitType.ALL_MODELS
				|| billed.getLimitType() == StepLimitType.PLATFORM);
		if (billed != null && !(router && acrossModels)) {
			addTargets(targets, path, billed, billed.getLimit(), false, pricesKnown, false);
			// the Team Total counts the team's calls on every model, so it leaves out
			// a router's own call too
			if (billed.getLimitType() == StepLimitType.TEAM && !router) {
				addTargets(targets, path, billed, billed.getTeamTotal(), true, pricesKnown, false);
			}
		}
		// the person's own counters, on the model and across all models, are charged
		// whichever source is billed, so each holds all of the person's use when it
		// is next checked
		for (Step step : path.getSteps()) {
			switch (step.getLimitType()) {
			case DIRECT:
				addTargets(targets, path, step, step.getLimit(), false, pricesKnown, false);
				break;
			case PUBLIC:
				// only each-person rules: the shared public counter counts only the
				// calls billed to public use
				addTargets(targets, path, step, step.getLimit(), false, pricesKnown, true);
				break;
			case ALL_MODELS:
			case PLATFORM:
				if (!router) {
					addTargets(targets, path, step, step.getLimit(), false, pricesKnown, false);
				}
				break;
			default:
				break;
			}
		}
		return new ArrayList<>(targets.values());
	}

	/**
	 * Adds a counter for each rule of the limit, keyed by scope, metric and window
	 * so two rules on the same counter share it.
	 */
	private static void addTargets(Map<String, ChargeTarget> targets, ModelUsageLimitPath path, Step step,
			ModelUsageLimit limit, boolean teamTotal, boolean pricesKnown, boolean eachPersonOnly) {
		if (!hasRules(limit)) {
			return;
		}
		for (ModelUsageLimitRule rule : limit.getRules()) {
			if (rule.getMetric() == null || rule.getWindow() == null) {
				continue;
			}
			// a model with no known prices has no cost to count
			if (rule.getMetric() == Metric.COST && !pricesKnown) {
				continue;
			}
			if (eachPersonOnly && rule.getCounts() != Counts.EACH_PERSON) {
				continue;
			}
			ModelUsageCounterScope scope = ModelUsageEvaluation.scopeOf(path, step, rule, teamTotal);
			String id = scope.id() + "|" + rule.getMetric().name() + "|" + rule.getWindow().name() + "|"
					+ rule.getWindowLength();
			targets.putIfAbsent(id,
					new ChargeTarget(scope, rule.getMetric(), rule.getWindow(), rule.getWindowLength()));
		}
	}

	/**
	 * Describes the source billed as the inference logs record it: its source type
	 * and, for a Direct Limit, a Team Limit or a User's Total Model Limit, the id
	 * and type of whom it belongs to.
	 *
	 * @param path   the person's path
	 * @param billed the source billed, or null
	 * @return the source as the inference logs record it, or null
	 */
	static ModelUsageBilledTo billedTo(ModelUsageLimitPath path, Step billed) {
		if (billed == null) {
			return null;
		}
		String sourceType = ModelUsageBilledTo.sourceTypeOf(billed.getLimitType());
		switch (billed.getLimitType()) {
		case DIRECT:
			return new ModelUsageBilledTo(sourceType, path.getUserId(), path.getUserType());
		case TEAM:
		case ALL_MODELS:
			return new ModelUsageBilledTo(sourceType, billed.getKey().subjectId(), billed.getKey().subjectType());
		default:
			return new ModelUsageBilledTo(sourceType, null, null);
		}
	}

	private static long orZero(Long value) {
		return value == null ? 0 : Math.max(0, value);
	}

	private static String modelName(IModelEngine engine) {
		String name = engine.getEngineName();
		return name == null || name.isBlank() ? engine.getEngineId() : name;
	}

	/**
	 * The cost a person's usage limit on a model counts in a window, worked out
	 * from the inference logs as a cold counter is filled, whether or not a limit
	 * applies.
	 *
	 * @param engineId the model
	 * @param userId   the primary login's id
	 * @param userType the primary login's type
	 * @param start    the window's start, inclusive
	 * @param end      the window's end, exclusive
	 * @return the cost in millionths of a dollar, or null when the model has no
	 *         known prices, or the logs are off or cannot be read
	 */
	public static Long costUsedInLogs(String engineId, String userId, String userType, Instant start, Instant end) {
		// a model with no known prices has no cost to count
		if (prices.apply(engineId) == null) {
			return null;
		}
		return usedInLogs(ModelUsageCounterScope.personModel(engineId, userId, userType), Metric.COST, start, end);
	}

	/**
	 * Fills a counter from the inference logs: what its rows add up to in the
	 * window. Cost also prices the rows written before calls recorded their cost,
	 * at each model's current base prices. This is the
	 * {@link ModelUsageEvaluation.Seeder} a cold counter is filled with, once, so a
	 * restart does not give it a fresh count.
	 *
	 * @return what the rows add up to, or null when the logs are off or cannot be
	 *         read, which leaves the counter unfilled for a while
	 */
	static Long usedInLogs(ModelUsageCounterScope scope, Metric metric, Instant start, Instant end) {
		if (!Utility.isModelInferenceLogsEnabled()) {
			return null;
		}
		try {
			ModelUsageLogFilter filter = scope.logFilter();
			long used = ModelInferenceLogsUtils.sumModelUsage(metric, filter, start, end);
			if (metric == Metric.COST) {
				for (ModelUsageLogTotals totals : ModelInferenceLogsUtils.sumModelUsageBeforeCost(filter, start, end)) {
					Map<String, Object> entry = prices.apply(totals.engineId());
					if (entry != null) {
						// the rows are added up per model, so their prompt tokens are the whole
						// window's; a tier picked by that total would price every call as a long
						// prompt, so the base rates are used
						used += ModelUsageCost.costMicrosAtBaseRates(entry,
								new ModelUsageReport(totals.promptTokens(), totals.responseTokens(),
										totals.cacheReadTokens(), totals.cacheWriteTokens(), totals.thinkingTokens(),
										0));
					}
				}
			}
			return used;
		} catch (Exception e) {
			classLogger.warn("Unable to fill a model usage counter from the inference logs: " + e.getMessage());
			return null;
		}
	}

	/**
	 * The counter store every usage limit is counted in, made on first use.
	 *
	 * @return the counters: in Redis when SEMOSS runs in a cluster with Redis
	 *         turned on, since counters on each server would allow each server the
	 *         whole limit; otherwise in memory
	 */
	static IModelUsageCounterStore store() {
		IModelUsageCounterStore current = counterStore;
		if (current == null) {
			synchronized (STORE_LOCK) {
				if (counterStore == null) {
					counterStore = useRedisStore() ? new RedisModelUsageCounterStore()
							: new LocalModelUsageCounterStore();
				}
				current = counterStore;
			}
		}
		return current;
	}

	private static boolean useRedisStore() {
		if (!ClusterUtil.IS_CLUSTER) {
			return false;
		}
		// a cluster without Redis turned on cannot share counters; fall back to memory
		if (RedisConnectionConfig.fromDIHelper() == null) {
			classLogger.warn("SEMOSS runs in a cluster but Redis is not enabled; model usage limits are counted per server");
			return false;
		}
		return true;
	}

	/**
	 * Sets the counters every call is counted in; null makes the default store
	 * again on its next use: Redis on a cluster with Redis turned on, memory
	 * otherwise.
	 */
	static void setCounterStore(IModelUsageCounterStore store) {
		counterStore = store;
	}

	/**
	 * Sets the clock windows, resets and reservations are timed by; null is the
	 * system's UTC clock.
	 */
	static void setClock(Clock newClock) {
		clock = newClock == null ? Clock.systemUTC() : newClock;
	}

	/**
	 * Sets how a cold counter is filled, or null to leave counters as they are, and
	 * forgets which counters' logs could not be read.
	 */
	static void setSeeder(ModelUsageEvaluation.Seeder newSeeder) {
		seeder = newSeeder;
		ModelUsageEvaluation.clearSeeding();
	}

	/**
	 * Fills cold counters from the inference logs again, and forgets which
	 * counters' logs could not be read.
	 */
	static void resetSeeder() {
		seeder = ModelUsageGuard::usedInLogs;
		ModelUsageEvaluation.clearSeeding();
	}

	/**
	 * Sets where models' price entries come from; null reads each model's stored
	 * prices.
	 */
	static void setPrices(Function<String, Map<String, Object>> newPrices) {
		prices = newPrices == null ? ModelUsageCost::priceEntry : newPrices;
	}
}
