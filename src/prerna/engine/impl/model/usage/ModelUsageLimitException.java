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

import prerna.auth.access.ModelUsageLimitPath.StepLimitType;

/**
 * Thrown when a usage limit refuses a model call. The message says which limit
 * and when it resets, and names only what the caller may see: the model, their
 * own limits and the teams they are in.
 *
 * <ul>
 * <li>{@link #isRateLimit()}: the rule is a minute or hour rule, so trying
 * again soon can work. The OpenAI, Anthropic and Ollama endpoints answer 429
 * with {@code Retry-After}.</li>
 * <li>Otherwise the rule is a day or longer rule. Those endpoints answer 429
 * with the error code {@link #ERROR_CODE} and no {@code Retry-After}.</li>
 * <li>{@link #isUnavailable()}: the limits could not be checked, such as when
 * Redis cannot be reached on a cluster. The call is refused with
 * {@link #UNAVAILABLE_MESSAGE}, which has the meaning of a 503.</li>
 * </ul>
 */
public class ModelUsageLimitException extends IllegalArgumentException {

	private static final long serialVersionUID = 1L;

	public static final String UNAVAILABLE_MESSAGE = "Usage limits cannot be checked right now";
	/** The error code a refused call is answered with */
	public static final String ERROR_CODE = "usage_limit_reached";

	/**
	 * The last refusal on each thread, so a pixel's caller can tell it from other
	 * errors
	 */
	private static final ThreadLocal<ModelUsageLimitException> LAST_ON_THREAD = new ThreadLocal<>();

	private final StepLimitType limitType;
	private final boolean rateLimit;
	private final String resetsAt;
	private final boolean unavailable;
	private final boolean callDoesNotFit;

	/**
	 * Makes the refusal of a call by a usage limit, which also becomes the last
	 * refusal on this thread.
	 *
	 * @param message   what to tell the caller
	 * @param limitType the limit that refused the call
	 * @param rateLimit whether the rule is a minute or hour rule
	 * @param resetsAt  when the limit has usage left again, as ISO 8601 in UTC, or
	 *                  null
	 */
	public ModelUsageLimitException(String message, StepLimitType limitType, boolean rateLimit, String resetsAt) {
		this(message, limitType, rateLimit, resetsAt, false, false);
	}

	private ModelUsageLimitException(String message, StepLimitType limitType, boolean rateLimit, String resetsAt,
			boolean unavailable, boolean callDoesNotFit) {
		super(message);
		this.limitType = limitType;
		this.rateLimit = rateLimit;
		this.resetsAt = resetsAt;
		this.unavailable = unavailable;
		this.callDoesNotFit = callDoesNotFit;
		// kept as it is made, since a pixel turns the refusal into an error message;
		// its caller matches that message to answer it as a refusal
		LAST_ON_THREAD.set(this);
	}

	/**
	 * Makes the refusal, with {@link #UNAVAILABLE_MESSAGE}, for a call a rule
	 * applies to when the counters cannot be reached.
	 *
	 * @return the refusal for limits that cannot be checked right now
	 */
	public static ModelUsageLimitException unavailable() {
		return new ModelUsageLimitException(UNAVAILABLE_MESSAGE, null, false, null, true, false);
	}

	/**
	 * Makes the refusal for a call a model client turned down because it did not
	 * fit in its {@code usage_ceiling}.
	 *
	 * @param message what the model client said
	 * @return the refusal a model client gives when the call does not fit in its
	 *         ceiling; the engine replaces it with one that names the limit
	 */
	public static ModelUsageLimitException doesNotFit(String message) {
		return new ModelUsageLimitException(message, null, false, null, false, true);
	}

	/**
	 * The type of limit that refused the call, such as the Direct Limit or a Team
	 * Limit.
	 *
	 * @return the limit that refused the call, or null when limits could not be
	 *         checked
	 */
	public StepLimitType getLimitType() {
		return limitType;
	}

	/**
	 * Whether trying again soon can work, so the endpoints answer with
	 * {@code Retry-After}.
	 *
	 * @return whether the rule is a minute or hour rule
	 */
	public boolean isRateLimit() {
		return rateLimit;
	}

	/**
	 * When a call can next get through, which the refusal's {@code resetsAt}
	 * carries.
	 *
	 * @return when the limit has usage left again, as ISO 8601 in UTC, or null
	 */
	public String getResetsAt() {
		return resetsAt;
	}

	/**
	 * Whether the refusal is one of {@link #unavailable()}, which has the meaning
	 * of a 503.
	 *
	 * @return whether the limits could not be checked, rather than having nothing
	 *         left
	 */
	public boolean isUnavailable() {
		return unavailable;
	}

	/**
	 * Whether the refusal came from {@link #doesNotFit(String)}, which the engine
	 * turns into one that names the limit.
	 *
	 * @return whether a model client refused the call because it did not fit in its
	 *         ceiling
	 */
	public boolean callDoesNotFit() {
		return callDoesNotFit;
	}

	/**
	 * Clears the last refusal on this thread, before running a model call whose
	 * refusal is wanted.
	 */
	public static void clearLastOnThread() {
		LAST_ON_THREAD.remove();
	}

	/**
	 * Takes the last refusal made on this thread, so a pixel's caller can tell a
	 * refusal from other errors.
	 *
	 * @return the last refusal on this thread since it was cleared, or null; it is
	 *         cleared again
	 */
	public static ModelUsageLimitException takeLastOnThread() {
		ModelUsageLimitException last = LAST_ON_THREAD.get();
		LAST_ON_THREAD.remove();
		return last;
	}
}
