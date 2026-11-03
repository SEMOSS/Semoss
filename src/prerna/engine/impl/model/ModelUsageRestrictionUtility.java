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
package prerna.engine.impl.model;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import com.google.gson.Gson;

import prerna.auth.User;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.responses.AbstractModelEngineResponse;
import prerna.engine.impl.model.usage.ModelUsageGuard;
import prerna.engine.impl.model.usage.ModelUsageReport;
import prerna.engine.impl.model.usage.ModelUsageRequest;
import prerna.engine.impl.model.usage.ModelUsageTicket;
import prerna.util.ValueUtils;

/**
 * Where the model engines enter usage limit enforcement: admit a call before it
 * runs, pass its ceiling to the Python call, and bill it when it ends. The
 * rules live in {@link ModelUsageGuard}.
 *
 * <ol>
 * <li>{@link #admit}: refuses the call or reserves for it, and returns its
 * ticket.</li>
 * <li>{@link #withUsageCeiling}: adds the ticket's usage left to the call's
 * parameters as {@code usage_ceiling}, for an engine whose Python client fits
 * the call in it; a router model passes its own on, and the lesser of the two
 * is sent.</li>
 * <li>{@link #bill} or {@link #billEmbeddings} when the call ends, or
 * {@code ticket.release()} when it fails, then the call's usage goes on the
 * response.</li>
 * </ol>
 */
public final class ModelUsageRestrictionUtility {

	private static final Gson GSON = new Gson();

	private static final String[] MAX_TOKENS_PARAMS = { AbstractModelEngine.MAX_TOKENS, "max_completion_tokens",
			"max_output_tokens", "max_new_tokens" };

	private ModelUsageRestrictionUtility() {

	}

	/**
	 * Admits a call under the usage limits that apply to the person calling,
	 * through {@link ModelUsageGuard}: it reserves what the call may use, or
	 * refuses it.
	 *
	 * @param user    the person calling, or null for a call SEMOSS makes itself
	 * @param engine  the model
	 * @param request what the call asks for
	 * @return the call's ticket
	 * @throws prerna.engine.impl.model.usage.ModelUsageLimitException when a limit
	 *                                                                 refuses the
	 *                                                                 call
	 */
	public static ModelUsageTicket admit(User user, IModelEngine engine, ModelUsageRequest request) {
		ModelUsageTicket ticket = ModelUsageGuard.admit(user, engine, request);
		return ticket == null ? ModelUsageTicket.none() : ticket;
	}

	/**
	 * Takes the {@code usage_ceiling} a router model passes on out of the
	 * parameters. It is taken out, rather than left in, so the engine sends one
	 * ceiling: the lesser of the one passed on and its own.
	 *
	 * @param parameters the call's parameters, or null
	 * @return the ceiling passed on, or null
	 */
	public static Object takeUsageCeiling(Map<String, Object> parameters) {
		return parameters == null ? null : parameters.remove(ModelUsageTicket.USAGE_CEILING);
	}

	/**
	 * Adds the call's ceiling to its parameters as {@code usage_ceiling}, for an
	 * engine whose Python client fits the call's output in it.
	 *
	 * @param parameters the call's parameters, or null
	 * @param ticket     the call's ticket
	 * @param passedOn   a ceiling a router model passed on, or null
	 * @return a copy of the parameters with {@code usage_ceiling}, the lesser of
	 *         the ticket's and the one passed on; the parameters themselves when
	 *         there is no ceiling
	 */
	public static Map<String, Object> withUsageCeiling(Map<String, Object> parameters, ModelUsageTicket ticket,
			Object passedOn) {
		Map<String, Object> ceiling = mergeCeilings(ticket == null ? null : ticket.pythonCeiling(),
				ceilingMap(passedOn));
		if (ceiling == null) {
			return parameters;
		}
		// a copy, so the ceiling of one call never stays on parameters a caller
		// reuses
		Map<String, Object> withCeiling = parameters == null ? new HashMap<>() : new HashMap<>(parameters);
		withCeiling.put(ModelUsageTicket.USAGE_CEILING, ceiling);
		return withCeiling;
	}

	/**
	 * Combines this model's ceiling with one a router model passed on, so a routed
	 * call fits within both. Each safety setting takes the stricter value.
	 *
	 * @param ours     this model's ceiling, or null
	 * @param passedOn a router's ceiling, or null
	 * @return the lesser of the two for tokens and for cost, with this model's
	 *         rates, or null when neither has a ceiling
	 */
	static Map<String, Object> mergeCeilings(Map<String, Object> ours, Map<String, Object> passedOn) {
		if (ours == null) {
			return passedOn;
		}
		if (passedOn == null) {
			return ours;
		}
		// the routed call must fit in what both the router and its own model have left,
		// so each
		// amount is the lesser and each safety setting the stricter
		Map<String, Object> merged = new LinkedHashMap<>(ours);
		merged.put("tokens", lesser(ours.get("tokens"), passedOn.get("tokens")));
		Object cost = lesser(ours.get("cost_micros"), passedOn.get("cost_micros"));
		merged.put("cost_micros", cost);
		// this model's rates price its own call; the router's are used only when
		// this model has no cost limit of its own
		if (cost != null && merged.get("rates") == null) {
			merged.put("rates", passedOn.get("rates"));
		}
		merged.put("min_output_tokens", greater(ours.get("min_output_tokens"), passedOn.get("min_output_tokens")));
		merged.put("margin", greater(ours.get("margin"), passedOn.get("margin")));
		return merged;
	}

	/**
	 * Reads the {@code usage_ceiling} a router model passed on into a map.
	 *
	 * @return the ceiling passed on, as a map, whether it came as a map or as JSON
	 *         text; null when there is none or it cannot be read
	 */
	@SuppressWarnings("unchecked")
	private static Map<String, Object> ceilingMap(Object ceiling) {
		if (ceiling instanceof Map) {
			return (Map<String, Object>) ceiling;
		}
		if (ceiling instanceof String && !((String) ceiling).isBlank()) {
			try {
				return GSON.fromJson((String) ceiling, Map.class);
			} catch (RuntimeException e) {
				return null;
			}
		}
		return null;
	}

	private static Object lesser(Object a, Object b) {
		if (!(a instanceof Number)) {
			return b instanceof Number ? ((Number) b).longValue() : null;
		}
		if (!(b instanceof Number)) {
			return ((Number) a).longValue();
		}
		return Math.min(((Number) a).longValue(), ((Number) b).longValue());
	}

	private static Object greater(Object a, Object b) {
		if (!(a instanceof Number)) {
			return b;
		}
		if (!(b instanceof Number)) {
			return a;
		}
		return ((Number) a).doubleValue() >= ((Number) b).doubleValue() ? a : b;
	}

	/**
	 * Bills a call that ended and puts its {@code usageRestriction} and
	 * {@code usageLimits} on the response.
	 *
	 * @param ticket     the call's ticket, or null
	 * @param response   the model's response, or null when it reported nothing
	 * @param inputTime  when the call started
	 * @param outputTime when it ended
	 * @return the call's cost in millionths of a dollar, or null when the model has
	 *         no known prices
	 */
	public static Long bill(ModelUsageTicket ticket, AbstractModelEngineResponse<?> response, ZonedDateTime inputTime,
			ZonedDateTime outputTime) {
		if (ticket == null) {
			return null;
		}
		Long cost = ticket.bill(ModelUsageReport.of(response, inputTime, outputTime));
		attach(ticket, response);
		return cost;
	}

	/**
	 * Bills an embeddings call that ended, priced per input token, and puts its
	 * usage on the response.
	 *
	 * @param ticket     the call's ticket, or null
	 * @param response   the model's response
	 * @param inputTime  when the call started
	 * @param outputTime when it ended
	 * @return the call's cost in millionths of a dollar, or null when the model has
	 *         no known prices
	 */
	public static Long billEmbeddings(ModelUsageTicket ticket, AbstractModelEngineResponse<?> response,
			ZonedDateTime inputTime, ZonedDateTime outputTime) {
		if (ticket == null) {
			return null;
		}
		Long cost = ticket.bill(ModelUsageReport.of(response, inputTime, outputTime).asEmbeddings());
		attach(ticket, response);
		return cost;
	}

	/**
	 * Puts a billed call's {@code usageRestriction} and {@code usageLimits} on its
	 * response, when the ticket has them.
	 *
	 * @param ticket   a billed ticket
	 * @param response the response to put its usage on, or null
	 */
	public static void attach(ModelUsageTicket ticket, AbstractModelEngineResponse<?> response) {
		if (ticket == null || response == null) {
			return;
		}
		Map<String, Object> restriction = ticket.responseUsageRestriction();
		if (restriction != null) {
			response.setUsageRestriction(restriction);
		}
		Map<String, Object> limits = ticket.responseUsageLimits(response.isUsageCapped());
		if (limits != null) {
			response.setUsageLimits(limits);
		}
	}

	/**
	 * Reads the most output a call asks for from its parameters, which bounds the
	 * estimate the call is reserved for.
	 *
	 * @param parameters      the call's parameters, or null
	 * @param engineMaxTokens the model's maximum output tokens, or 0 when unknown
	 * @return the most output the call asks for: the caller's maximum, under any of
	 *         the names the Python clients accept, or else the model's; null when
	 *         neither is known
	 */
	public static Long requestedMaxTokens(Map<String, Object> parameters, long engineMaxTokens) {
		if (parameters != null) {
			for (String key : MAX_TOKENS_PARAMS) {
				Long value = ValueUtils.toPositiveLongOrNull(parameters.get(key));
				if (value != null) {
					return value;
				}
			}
		}
		return engineMaxTokens > 0 ? engineMaxTokens : null;
	}
}
