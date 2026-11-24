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

import java.util.Locale;
import java.util.Map;

import prerna.engine.impl.model.ModelRouterEngine.ReplaySettings;
import prerna.util.ValueUtils;

/**
 * Determines whether a candidate improves on the live router beyond measured noise.
 */
final class RouterGate {

	/** The one-sided 95% quantile of the normal distribution: a difference beyond this many standard errors is real, not noise. */
	static final double Z = 1.645;

	/** Mean and standard error of the per-task differences (candidate minus live) of one measure. */
	record Paired(double mean, double se) {

		static Paired of(double[] diffs) {
			int n = diffs.length;
			double mean = 0;
			for (double d : diffs) {
				mean += d / n;
			}
			double variance = 0;
			for (double d : diffs) {
				variance += (d - mean) * (d - mean) / Math.max(1, n - 1);
			}
			return new Paired(mean, Math.sqrt(variance / n));
		}

		/** The measure is lower than {@code limit} by more than noise. */
		boolean below(double limit) {
			return mean + Z * se < limit;
		}

		boolean above(double limit) {
			return mean - Z * se > limit;
		}
	}

	private RouterGate() {
	}

	/**
	 * Why a candidate must not replace the live version, or null when it may. It needs the configured number of paired tasks; its quality
	 * change must not be below {@code -allowedLoss} (the point estimate: not being able to prove a loss is no reason to accept it); and it
	 * must be cheaper by more than noise, or better by more than noise at a cost not higher by more than noise. Noise is the standard error
	 * of the per-task differences; it only ever works against the candidate, never excuses a quality loss.
	 */
	static String refusal(int n, Paired quality, Paired cost, double allowedLoss) {
		return refusal(n, quality, cost, allowedLoss, ReplaySettings.DEFAULT_VALIDATION_TASKS);
	}

	private static String refusal(int n, Paired quality, Paired cost, double allowedLoss, int minimum) {
		if (n < minimum) {
			return "needs " + (minimum - n) + " more tasks: only " + n + " were compared, at least " + minimum + " are needed";
		}
		if (quality.mean() < -allowedLoss) {
			return String.format(Locale.ROOT, "quality is %.0f points lower than the live version on %d tasks, the limit is %.0f", -100 * quality.mean(), n, 100 * allowedLoss);
		}
		boolean cheaper = cost.below(0);
		boolean betterWithoutHigherCost = quality.above(0) && !cost.above(0);
		if (cheaper || betterWithoutHigherCost) {
			return null;
		}
		return String.format(Locale.ROOT,
				"the candidate is neither cheaper nor better than the live version beyond noise: quality %+.3f (noise %.3f), cost %+.0f micros per task (noise %.0f), on %d tasks",
				quality.mean(), quality.se(), cost.mean(), cost.se(), n);
	}

	/**
	 * Why the evaluation may not be applied, or null when it may: complete, and the paired comparison finds the candidate not
	 * worse than the live version beyond noise and cheaper or better beyond noise.
	 */
	static String refusal(Map<String, Object> e, double allowedLoss) {
		return refusal(e, allowedLoss, ReplaySettings.DEFAULT_VALIDATION_TASKS);
	}

	static String refusal(Map<String, Object> e, double allowedLoss, int minimum) {
		if (Boolean.TRUE.equals(e.get("stopped_by_cap"))) {
			return "the evaluation stopped early (" + e.get("stop_reason") + ")";
		}
		if (!Boolean.TRUE.equals(e.get("complete"))) {
			return "the evaluation is incomplete (" + ValueUtils.toStringOrEmpty(e.get("stop_reason")) + ")";
		}
		if (!(e.get("quality_delta_vs_live") instanceof Number) || !(e.get("cost_delta_vs_live") instanceof Number)) {
			return "the evaluation has no complete comparison with the live version on the same tasks";
		}
		return refusal((int) number(e.get("n")), new Paired(number(e.get("quality_delta_vs_live")), number(e.get("quality_se"))),
				new Paired(number(e.get("cost_delta_vs_live")), number(e.get("cost_se"))), allowedLoss, minimum);
	}

	static double number(Object value) {
		return value instanceof Number n ? n.doubleValue() : 0.0;
	}
}
