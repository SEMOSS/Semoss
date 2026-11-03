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
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.access.ModelUsageLimitPath.Step;
import prerna.auth.access.ModelUsageLimitRule.Metric;
import prerna.auth.access.ModelUsageLimitRule.Window;
import prerna.engine.impl.model.responses.AbstractModelEngineResponse;
import prerna.engine.impl.model.responses.AskErrorModelEngineResponse;
import prerna.engine.impl.model.usage.ModelUsageEvaluation.RuleCheck;

/**
 * A call admitted by {@link ModelUsageGuard}: the source it is billed to, the
 * usage left it has to fit in, and what it holds on its counters while it runs.
 *
 * <p>
 * The usage left is what is left on the tightest token and cost rules, and is
 * what the Python call fits in, with its own count of the prompt. What is
 * reserved on the counters is the lesser of the usage left and the call's
 * estimate, so calls in flight share what is left.
 * </p>
 *
 * <p>
 * When the call ends, {@link #bill(ModelUsageReport)} replaces the reservation
 * with what was used: tokens, cost and milliseconds, on every counter of the
 * source billed, the person's counters on the model, the ceilings and, unless
 * the call is a router's own, a team's total and the person's counters across
 * all models. A call that fails before the model answers calls
 * {@link #release()} instead.
 * </p>
 *
 * <p>
 * A ticket finishes once: the first of {@link #bill(ModelUsageReport)} and
 * {@link #release()} settles it and the other does nothing, so a call is never
 * billed twice and never billed after it was released. A batch bills its
 * results with {@link #billPart(ModelUsageReport)} as they come in, and
 * releases what is left when it ends. The source billed, {@link #billedTo()},
 * goes on the response's {@code usageLimits} and on the call's inference log
 * rows.
 * </p>
 */
public class ModelUsageTicket {

	private static final Logger classLogger = LogManager.getLogger(ModelUsageTicket.class);

	/**
	 * The keyword argument the Python {@code ask} and {@code embeddings} calls take
	 */
	public static final String USAGE_CEILING = "usage_ceiling";
	/**
	 * The error reason a Python client answers with when a call does not fit in its
	 * ceiling
	 */
	public static final String DOES_NOT_FIT_REASON = AskErrorModelEngineResponse.REASON_USAGE_LIMIT_DOES_NOT_FIT;
	/**
	 * Less than this left for output refuses the call: the Python client answers
	 * with {@link #DOES_NOT_FIT_REASON} before the provider sees the call, and a
	 * batch is refused when its requests' equal share of the output is under it
	 */
	public static final long MIN_OUTPUT_TOKENS = 256L;
	/** Kept free when the prompt count is an estimate */
	public static final double MARGIN = 0.05;

	/**
	 * One counter a call is billed on, whose bucket is the one holding the moment
	 * it is billed; length is how many minutes or hours a sliding window is
	 */
	record ChargeTarget(ModelUsageCounterScope scope, Metric metric, Window window, int length) {
	}

	/**
	 * What a call may use, for each of tokens and cost: what is left on the
	 * tightest rule, which the Python call fits in, and what is reserved on the
	 * counters, the lesser of what is left and the call's estimate. Both are null
	 * when no rule of the metric applies.
	 *
	 * @param tokensLeft           the tokens left, for prompt and output
	 * @param costLeft             the cost left, in millionths of a dollar
	 * @param tokensReserved       the tokens reserved
	 * @param costReserved         the cost reserved
	 * @param promptEstimateTokens the call's prompt estimate, which picks the tier
	 *                             of the rates sent to Python
	 * @param maxOutputTokens      the call's bounded output maximum, or null for
	 *                             embeddings
	 */
	record Allowance(Long tokensLeft, Long costLeft, Long tokensReserved, Long costReserved, long promptEstimateTokens,
			Long maxOutputTokens) {

		static final Allowance NONE = new Allowance(null, null, null, null, 0, null);
	}

	private final IModelUsageCounterStore store;
	private final Clock clock;
	private final ModelUsageEvaluation evaluation;
	private final String modelName;
	private final Map<String, Object> priceEntry;
	private final List<ChargeTarget> targets;
	private final ModelUsageReservation reservation;
	private final Map<String, Metric> reservedMetrics;
	private final Allowance allowance;
	private final boolean embeddings;
	private final ModelUsageBilledTo billedTo;

	// set by the first of bill and release, after which neither does anything
	private boolean finished;
	private Long costMicros;
	private ModelUsageReport billedReport;

	ModelUsageTicket(IModelUsageCounterStore store, Clock clock, ModelUsageEvaluation evaluation, String modelName,
			Map<String, Object> priceEntry, List<ChargeTarget> targets, ModelUsageReservation reservation,
			Map<String, Metric> reservedMetrics, Allowance allowance, boolean embeddings, ModelUsageBilledTo billedTo) {
		this.store = store;
		this.clock = clock == null ? Clock.systemUTC() : clock;
		this.evaluation = evaluation;
		this.modelName = modelName;
		this.priceEntry = priceEntry;
		this.targets = targets == null ? List.of() : List.copyOf(targets);
		this.reservation = reservation;
		this.reservedMetrics = reservedMetrics == null ? Map.of() : Map.copyOf(reservedMetrics);
		this.allowance = allowance == null ? Allowance.NONE : allowance;
		this.embeddings = embeddings;
		this.billedTo = billedTo;
	}

	/**
	 * Makes the ticket for a call with no limits to check, such as one SEMOSS makes
	 * with no user. It has no ceiling, and billing or releasing it does nothing.
	 *
	 * @return a ticket for a call no limit applies to, which bills nothing
	 */
	public static ModelUsageTicket none() {
		return new ModelUsageTicket(null, null, null, null, null, null, null, null, Allowance.NONE, false, null);
	}

	/**
	 * The {@code usage_ceiling} keyword argument for the Python call. Its
	 * {@code tokens} and {@code cost_micros} are what is left on the tightest
	 * rules, not what is reserved: the client fits the call in what is left with
	 * its own count of the prompt.
	 *
	 * <pre>
	 * { "tokens": 120000 | None,         most tokens the call may use, prompt and output
	 *   "cost_micros": 2500000 | None,   most cost the call may use, in millionths of a dollar
	 *   "rates": { "input", "output", "cache_read", "cache_write", "reasoning" } | None,
	 *                                    dollars per million tokens, when cost_micros is set
	 *   "max_output_tokens": 8192 | None, the output maximum the call is reserved for: the
	 *                                    caller's, else the model's, else a default; None
	 *                                    for embeddings
	 *   "min_output_tokens": 256,        less than this left for output refuses the call
	 *   "margin": 0.05 }                 kept free when the prompt count is an estimate
	 * </pre>
	 *
	 * @return the ceiling, or null when no token or cost rule applies to the call
	 */
	public Map<String, Object> pythonCeiling() {
		Long tokensLeft = allowance.tokensLeft();
		Long costLeft = allowance.costLeft();
		if (tokensLeft == null && costLeft == null) {
			return null;
		}
		Map<String, Object> ceiling = new LinkedHashMap<>();
		ceiling.put("tokens", tokensLeft);
		ceiling.put("cost_micros", costLeft);
		ceiling.put("rates", costLeft == null || priceEntry == null ? null
				: ModelUsageCost.pythonRates(priceEntry, allowance.promptEstimateTokens()));
		ceiling.put("max_output_tokens", embeddings ? null : allowance.maxOutputTokens());
		ceiling.put("min_output_tokens", MIN_OUTPUT_TOKENS);
		ceiling.put("margin", MARGIN);
		return ceiling;
	}

	/**
	 * Replaces the call's reservation with what it used. Billing twice, or after a
	 * release, does nothing.
	 *
	 * @param report what the call used
	 * @return the call's cost in millionths of a dollar, or null when the model has
	 *         no known prices
	 */
	public synchronized Long bill(ModelUsageReport report) {
		if (finished) {
			return costMicros;
		}
		finished = true;
		// a call with no report bills nothing, but still gives back its reservation
		ModelUsageReport used = report == null ? new ModelUsageReport(0, 0, 0, 0, 0, 0) : report;
		long cost = priceEntry == null ? 0 : ModelUsageCost.costMicros(priceEntry, used);
		this.costMicros = priceEntry == null ? null : cost;
		// kept so the response's usage counts this call in what each rule has used
		this.billedReport = used;
		// releases the whole reservation and adds what was used, in one step
		settle(null, charges(used, cost));
		return costMicros;
	}

	/**
	 * Bills part of what the call holds, such as the results of a batch that have
	 * come in, and keeps holding the rest.
	 *
	 * @param report what that part used
	 * @return its cost in millionths of a dollar, or null when the model has no
	 *         known prices
	 */
	public synchronized Long billPart(ModelUsageReport report) {
		if (finished || report == null) {
			return null;
		}
		long cost = priceEntry == null ? 0 : ModelUsageCost.costMicros(priceEntry, report);
		// each counter gives back what the part used, never more than it still
		// holds, so the reservation shrinks as results come in
		Map<String, Long> released = new LinkedHashMap<>();
		for (Map.Entry<String, Metric> reserved : reservedMetrics.entrySet()) {
			released.put(reserved.getKey(), reserved.getValue() == Metric.COST ? cost : report.totalTokens());
		}
		settle(released, charges(report, cost));
		return priceEntry == null ? null : cost;
	}

	/**
	 * Gives back whatever the call still holds, for a call that failed or was
	 * cancelled. Releasing after billing does nothing.
	 */
	public synchronized void release() {
		if (finished) {
			return;
		}
		finished = true;
		settle(null, List.of());
	}

	private void settle(Map<String, Long> released, List<ModelUsageCharge> charges) {
		if (store == null || (reservation == null && charges.isEmpty())) {
			return;
		}
		try {
			store.settle(reservation, released, charges);
		} catch (RuntimeException e) {
			// the call has already run, so a failure is logged rather than thrown;
			// the reservation expires on its own
			classLogger.warn("Unable to bill a model call to its usage limit counters: " + e.getMessage());
		}
	}

	private List<ModelUsageCharge> charges(ModelUsageReport report, long cost) {
		List<ModelUsageCharge> charges = new ArrayList<>();
		if (targets.isEmpty()) {
			return charges;
		}
		// usage goes in the bucket that holds the moment of billing, which can be
		// after the bucket the call was reserved in
		Instant now = clock.instant();
		for (ChargeTarget target : targets) {
			long amount;
			switch (target.metric()) {
			case TOKENS:
				amount = report.totalTokens();
				break;
			case COST:
				amount = priceEntry == null ? 0 : cost;
				break;
			case COMPUTE_TIME:
				// compute time is checked before the call but never reserved, so a
				// long call can take its rule over the amount
				amount = report.getResponseTimeMillis();
				break;
			default:
				amount = 0;
			}
			if (amount <= 0) {
				continue;
			}
			Instant bucketStart = ModelUsageWindows.bucketStart(target.window(), target.length(), now);
			charges.add(new ModelUsageCharge(
					ModelUsageEvaluation.keyOf(target.scope(), target.metric(), target.window(), target.length(),
							bucketStart),
					amount, ModelUsageWindows.counterExpiresAt(target.window(), target.length(), bucketStart)));
		}
		return charges;
	}

	/**
	 * The tokens the call holds on its token counters while it runs, which a batch
	 * must fit in.
	 *
	 * @return the tokens reserved for the call, the lesser of what is left and its
	 *         estimate; null when no token rule applies
	 */
	public Long getTokensReserved() {
		return allowance.tokensReserved();
	}

	/**
	 * The cost the call holds on its cost counters while it runs, which a batch
	 * must fit in.
	 *
	 * @return the cost reserved for the call, in millionths of a dollar, the lesser
	 *         of what is left and its estimate; null when no cost rule applies
	 */
	public Long getCostReserved() {
		return allowance.costReserved();
	}

	/**
	 * The prices of the model that the call's cost is worked out from.
	 *
	 * @return the model's price entry, or null when it has no known prices
	 */
	public Map<String, Object> getPriceEntry() {
		return priceEntry;
	}

	/**
	 * The source the call is billed to, which goes on the response's
	 * {@code usageLimits} and the call's inference log rows.
	 *
	 * @return the source the call is billed to, for the inference logs, or null
	 *         when no limit applies
	 */
	public ModelUsageBilledTo billedTo() {
		return billedTo;
	}

	/**
	 * Turns a model client's refusal, for a call that did not fit in its ceiling,
	 * into one that names the limit.
	 *
	 * @param cause the client's refusal
	 * @return the refusal to throw
	 */
	public ModelUsageLimitException doesNotFit(ModelUsageLimitException cause) {
		if (evaluation == null) {
			return cause;
		}
		return ModelUsageFormat.doesNotFit(evaluation, modelName, embeddings);
	}

	/**
	 * The {@code usageLimits} of the model response:
	 *
	 * <pre>
	 * { "billedTo": { "sourceType", "subjectId", "subjectType", "limitType" } | null,
	 *                                                  the source billed, and the type of limit billed
	 *   "unlimited": bool,
	 *   "rules": [ RuleUsage ],                     the source's rules, and a team's total's
	 *   "ceilings": [ { "limitType": "MODEL_TOTAL", "rules": [ RuleUsage ] } ],
	 *   "capped": bool }                            the answer stopped at a capped maximum
	 * RuleUsage = { "metric", "amount", "window", "windowLength",
	 *               "counts", "teamTotal", "used", "resetsAt" }
	 * </pre>
	 *
	 * @param capped whether the answer stopped at the output maximum the ceiling
	 *               lowered
	 * @return the usage limits, or null when no limit applies
	 */
	public synchronized Map<String, Object> responseUsageLimits(boolean capped) {
		if (evaluation == null || (billedTo == null && evaluation.checksForCall().isEmpty())) {
			return null;
		}
		Step billed = evaluation.getBilled();
		Map<String, Object> limits = new LinkedHashMap<>();
		Map<String, Object> source = null;
		if (billedTo != null) {
			source = billedTo.toMap();
			source.put("limitType", billed == null ? null : billed.getLimitType().name());
		}
		limits.put("billedTo", source);
		limits.put("unlimited", billed != null && billed.isUnlimited());
		List<Map<String, Object>> rules = new ArrayList<>();
		if (billed != null) {
			for (RuleCheck check : evaluation.checksOf(billed, false)) {
				rules.add(ruleUsage(check));
			}
			for (RuleCheck check : evaluation.checksOf(billed, true)) {
				rules.add(ruleUsage(check));
			}
		}
		limits.put("rules", rules);
		List<Map<String, Object>> ceilings = new ArrayList<>();
		for (Step ceiling : evaluation.getPath().getCeilings()) {
			List<RuleCheck> checks = evaluation.checksOf(ceiling, false);
			if (checks.isEmpty()) {
				continue;
			}
			Map<String, Object> ceilingMap = new LinkedHashMap<>();
			ceilingMap.put("limitType", ceiling.getLimitType().name());
			List<Map<String, Object>> ceilingRules = new ArrayList<>();
			for (RuleCheck check : checks) {
				ceilingRules.add(ruleUsage(check));
			}
			ceilingMap.put("rules", ceilingRules);
			ceilings.add(ceilingMap);
		}
		limits.put("ceilings", ceilings);
		limits.put("capped", capped);
		return limits;
	}

	/**
	 * The {@code usageRestriction} of the model response, from the rule closest to
	 * its amount: {@code restrictedBy} ({@code token}, {@code cost} or
	 * {@code compute}), {@code currentValue} and {@code maxValue}.
	 *
	 * @return the restriction, or null when no rule applies
	 */
	public synchronized Map<String, Object> responseUsageRestriction() {
		if (evaluation == null) {
			return null;
		}
		RuleCheck closest = null;
		double closestShare = -1;
		for (RuleCheck check : evaluation.checksForCall()) {
			if (!check.applies || check.amount <= 0) {
				continue;
			}
			double share = (double) usedAfter(check) / check.amount;
			if (share > closestShare) {
				closest = check;
				closestShare = share;
			}
		}
		if (closest == null) {
			return null;
		}
		Map<String, Object> restriction = new LinkedHashMap<>();
		restriction.put(AbstractModelEngineResponse.USAGE_RESTRICTION_MODE, restrictedBy(closest.metric()));
		restriction.put(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE, usedAfter(closest));
		restriction.put(AbstractModelEngineResponse.USAGE_RESTRICTION_MAX_VALUE, closest.amount);
		return restriction;
	}

	private Map<String, Object> ruleUsage(RuleCheck check) {
		Map<String, Object> rule = new LinkedHashMap<>();
		rule.put("metric", check.metric().name());
		rule.put("amount", check.amount);
		rule.put("window", check.window().name());
		rule.put("windowLength", check.length() > 1 ? check.length() : null);
		rule.put("counts", check.rule.getCounts() == null ? null : check.rule.getCounts().name());
		rule.put("teamTotal", check.teamTotal);
		rule.put("used", usedAfter(check));
		rule.put("resetsAt", ModelUsageWindows.iso(check.resetsAt(evaluation.getNow())));
		return rule;
	}

	/**
	 * Works out a rule's used for the response: what the counters held when they
	 * were read, plus what this call used once it is billed.
	 *
	 * @return what the rule has counted, this call included once it is billed
	 */
	private long usedAfter(RuleCheck check) {
		long used = check.usedSoFar();
		if (billedReport == null || !check.applies) {
			return used;
		}
		switch (check.metric()) {
		case TOKENS:
			return used + billedReport.totalTokens();
		case COST:
			return used + (costMicros == null ? 0 : costMicros);
		case COMPUTE_TIME:
			return used + billedReport.getResponseTimeMillis();
		default:
			return used;
		}
	}

	private static String restrictedBy(Metric metric) {
		switch (metric) {
		case TOKENS:
			return "token";
		case COST:
			return "cost";
		default:
			return "compute";
		}
	}

	/**
	 * Tells whether a Python call's output is its client's refusal for a call that
	 * does not fit in its ceiling.
	 *
	 * @param output what a Python model call returned
	 * @return whether the client refused the call because it did not fit in its
	 *         {@code usage_ceiling}: an error whose {@code reason} is
	 *         {@link #DOES_NOT_FIT_REASON}
	 */
	public static boolean callDoesNotFit(Object output) {
		if (!(output instanceof Map)) {
			return false;
		}
		Object reason = ((Map<?, ?>) output).get(AskErrorModelEngineResponse.REASON);
		return reason != null && DOES_NOT_FIT_REASON.equals(reason.toString().trim());
	}

	/**
	 * Reads the message of a Python client's refusal for a call that does not fit,
	 * from its {@code message} or else its {@code response}.
	 *
	 * @param output what a Python model call returned
	 * @return the client's message for a call that did not fit, or a default one
	 */
	public static String doesNotFitMessage(Object output) {
		if (output instanceof Map) {
			Object message = ((Map<?, ?>) output).get("message");
			if (message == null) {
				message = ((Map<?, ?>) output).get("response");
			}
			if (message != null && !message.toString().isBlank()) {
				return message.toString();
			}
		}
		return "There is not enough left of your usage limit for this request.";
	}
}
