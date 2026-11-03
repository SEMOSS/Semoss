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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import prerna.auth.access.ModelUsageLimitPath.Step;
import prerna.auth.access.ModelUsageLimitPath.StepLimitType;
import prerna.auth.access.ModelUsageLimitRule.Counts;
import prerna.auth.access.ModelUsageLimitRule.Metric;
import prerna.auth.access.ModelUsageLimitRule.Window;
import prerna.engine.impl.model.usage.ModelUsageEvaluation.RuleCheck;

/**
 * The words a refused call is answered with. Each message says which limit
 * refused the call and when it resets, and names only the model, the caller's
 * own limits and the teams the caller is in.
 */
final class ModelUsageFormat {

	private static final BigDecimal MICROS_PER_DOLLAR = BigDecimal.valueOf(1_000_000L);

	private ModelUsageFormat() {

	}

	/**
	 * Builds the refusal for a call a limit refused, with a message that names the
	 * limit, or every team the caller is in whose limit is used up, and when it
	 * resets.
	 *
	 * @param evaluation a refused evaluation
	 * @param modelName  the model's name
	 * @return the refusal to throw
	 */
	static ModelUsageLimitException refusal(ModelUsageEvaluation evaluation, String modelName) {
		Step step = evaluation.getRefusedBy();
		RuleCheck check = evaluation.getRefusedRule();
		String resetsAt = check == null ? null : ModelUsageWindows.iso(check.resetsAt(evaluation.getNow()));
		// a minute or hour rule is a rate limit, answered with Retry-After since
		// trying again soon can work
		boolean rateLimit = check != null && ModelUsageWindows.isSliding(check.window());
		String message;
		// a team refusal names every team the caller is in that has nothing left, and
		// the soonest reset among them
		if (step.getLimitType() == StepLimitType.TEAM) {
			List<String> teams = new ArrayList<>();
			for (Step team : evaluation.getTeamsReached()) {
				teams.add(team.getKey().subjectId());
			}
			if (teams.size() <= 1) {
				message = "Your team's limit on " + modelName + " is used up"
						+ (teams.isEmpty() ? "" : ": " + teams.get(0)) + ".";
			} else {
				message = "Your teams' limits on " + modelName + " are used up: " + String.join(", ", teams) + ".";
			}
			if (resetsAt != null) {
				message += (teams.size() <= 1 ? " It resets at " : " The soonest resets at ") + resetsAt + ".";
			}
		} else {
			message = sentence(step.getLimitType(), check, modelName);
			if (resetsAt != null) {
				message += " It resets at " + resetsAt + ".";
			}
		}
		return new ModelUsageLimitException(message, step.getLimitType(), rateLimit, resetsAt);
	}

	/**
	 * The refusal for a call that does not fit in what is left: the Python client
	 * found its prompt leaves too little for output, a batch's requests do not fit,
	 * or other calls kept taking what was left before it could reserve. It names
	 * the tightest token or cost rule.
	 *
	 * @param evaluation the evaluation the call was admitted on
	 * @param modelName  the model's name
	 * @param embeddings whether the call embeds
	 * @return the refusal for a call that does not fit in what is left
	 */
	static ModelUsageLimitException doesNotFit(ModelUsageEvaluation evaluation, String modelName, boolean embeddings) {
		RuleCheck check = evaluation.tightest();
		if (check == null) {
			return new ModelUsageLimitException("There is not enough left of your usage limit for this request.", null,
					false, null);
		}
		String resetsAt = ModelUsageWindows.iso(check.resetsAt(evaluation.getNow()));
		String message = "There is not enough left of " + source(check, modelName) + " for this request."
				+ (embeddings ? "" : " A shorter conversation or a smaller request may fit.") + " It resets at "
				+ resetsAt + ".";
		return new ModelUsageLimitException(message, check.step.getLimitType(),
				ModelUsageWindows.isSliding(check.window()), resetsAt);
	}

	private static String sentence(StepLimitType limitType, RuleCheck check, String modelName) {
		String amount = check == null ? "limit" : amount(check.rule.getMetric(), check.amount);
		String per = check == null ? "" : " " + per(check.window(), check.length());
		String during = check == null ? "for now" : during(check.window(), check.length());
		switch (limitType) {
		case MODEL_TOTAL:
			return modelName + " has reached its limit " + during + ".";
		case DIRECT:
			return "You have used your " + amount + per + " on " + modelName + ".";
		case PUBLIC:
			if (check != null && check.rule.getCounts() == Counts.SHARED) {
				return "Public use of " + modelName + " has reached its limit " + during + ".";
			}
			return "You have used your public allowance of " + modelName + " " + during + ".";
		case ALL_MODELS:
			return "You have used your " + amount + per + " across all models.";
		case PLATFORM:
			return "You have used the platform's " + amount + per + " across all models.";
		default:
			return "Your usage limit on " + modelName + " is used up.";
		}
	}

	private static String source(RuleCheck check, String modelName) {
		switch (check.step.getLimitType()) {
		case MODEL_TOTAL:
			return modelName + "'s total limit";
		case DIRECT:
			return "your limit on " + modelName;
		case TEAM:
			return check.teamTotal ? "the total of team " + check.step.getKey().subjectId()
					: "team " + check.step.getKey().subjectId() + "'s limit on " + modelName;
		case PUBLIC:
			return check.rule.getCounts() == Counts.SHARED ? "the public use limit on " + modelName
					: "your public allowance of " + modelName;
		case ALL_MODELS:
			return "your limit across all models";
		case PLATFORM:
			return "the platform's limit across all models";
		default:
			return "your usage limit";
		}
	}

	/**
	 * Writes an amount a rule counts in words people read, for the messages a
	 * refused call is answered with.
	 *
	 * @param metric what is counted
	 * @param amount tokens, millionths of a dollar or milliseconds
	 * @return the amount for people, such as {@code 100,000 tokens} or
	 *         {@code $20.00}
	 */
	static String amount(Metric metric, long amount) {
		switch (metric) {
		case TOKENS:
			return NumberFormat.getIntegerInstance(Locale.US).format(amount) + " tokens";
		case COST:
			return dollars(amount);
		case COMPUTE_TIME:
			return duration(amount) + " of compute time";
		default:
			return String.valueOf(amount);
		}
	}

	/**
	 * Writes an amount in millionths of a dollar as dollars people read.
	 *
	 * @param micros millionths of a dollar
	 * @return the dollars, with cents, or more places when an amount is smaller
	 *         than a cent
	 */
	static String dollars(long micros) {
		BigDecimal dollars = BigDecimal.valueOf(micros).divide(MICROS_PER_DOLLAR);
		BigDecimal cents = dollars.setScale(2, RoundingMode.HALF_UP);
		BigDecimal shown = cents.compareTo(dollars) == 0 || dollars.compareTo(BigDecimal.ONE) >= 0 ? cents
				: dollars.stripTrailingZeros();
		NumberFormat format = NumberFormat.getNumberInstance(Locale.US);
		format.setMinimumFractionDigits(Math.max(2, shown.scale()));
		format.setMaximumFractionDigits(Math.max(2, shown.scale()));
		return "$" + format.format(shown);
	}

	private static String duration(long millis) {
		if (millis % ModelUsageWindows.HOUR_MILLIS == 0) {
			long hours = millis / ModelUsageWindows.HOUR_MILLIS;
			return hours + (hours == 1 ? " hour" : " hours");
		}
		if (millis % ModelUsageWindows.MINUTE_MILLIS == 0) {
			long minutes = millis / ModelUsageWindows.MINUTE_MILLIS;
			return minutes + (minutes == 1 ? " minute" : " minutes");
		}
		if (millis % 1000 == 0) {
			long seconds = millis / 1000;
			return seconds + (seconds == 1 ? " second" : " seconds");
		}
		return millis + " milliseconds";
	}

	private static String per(Window window, int length) {
		switch (window) {
		case MINUTE:
			return length > 1 ? "every " + length + " minutes" : "a minute";
		case HOUR:
			return length > 1 ? "every " + length + " hours" : "an hour";
		case DAY:
			return "a day";
		case WEEK:
			return "a week";
		case MONTH:
			return "a month";
		default:
			return "a year";
		}
	}

	private static String during(Window window, int length) {
		switch (window) {
		case MINUTE:
			return length > 1 ? "for these " + length + " minutes" : "for this minute";
		case HOUR:
			return length > 1 ? "for these " + length + " hours" : "for this hour";
		case DAY:
			return "for today";
		case WEEK:
			return "for this week";
		case MONTH:
			return "for this month";
		default:
			return "for this year";
		}
	}
}
