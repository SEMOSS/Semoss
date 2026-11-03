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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * How model usage limits apply to one person on one model, in the order of "The
 * whole picture" in the model usage limits design: the ceilings, which every
 * call must fit in, then the sources a call can be billed to, from the top.
 *
 * <p>
 * The resolver fills in which limits are defined and which source is billed
 * when nothing is used up: {@link State#DEFINED}, {@link State#NOT_DEFINED},
 * {@link State#SKIPPED} with its {@link Reason}, {@link State#NOT_REACHED} for
 * a defined source after the billed one, and {@link State#CHECKED} for a
 * ceiling. Enforcement and the path view then set the call time states from
 * what is used: {@link State#BILLED}, {@link State#NEXT} for a team a call
 * moves on to when the billed team has nothing left, and {@link State#REACHED}
 * where a call is refused.
 * </p>
 */
public class ModelUsageLimitPath {

	/** Whether a step is a ceiling or a source the model call can be billed to */
	public enum Layer {
		CEILING, BILLED
	}

	/**
	 * Which limit a step checks: a ceiling, or one of the limits a model call can
	 * be billed to
	 */
	public enum StepLimitType {
		MODEL_TOTAL, DIRECT, TEAM, PUBLIC, ALL_MODELS, PLATFORM
	}

	/** Where a step stands */
	public enum State {
		/** A source with a limit defined, which a call is billed to first */
		DEFINED,
		/** A source with no limit defined, so the decision passes on */
		NOT_DEFINED,
		/** A source that cannot apply, with the reason */
		SKIPPED,
		/** A source with a limit defined, after the one that applies */
		NOT_REACHED,
		/** A ceiling with usage left */
		CHECKED,
		/** The source a call is billed to */
		BILLED,
		/** A team that calls move on to when the teams before it have nothing left */
		NEXT,
		/**
		 * A step with nothing left, which refuses the call or, for a team, passes it on
		 */
		REACHED
	}

	/** Why a step is skipped */
	public enum Reason {
		/** Public use applies only to a public model */
		NOT_PUBLIC,
		/** The person has no grant of their own on the model */
		NO_GRANT
	}

	/** One step */
	public static class Step {
		private final Layer layer;
		private final StepLimitType limitType;
		private final ModelUsageLimitKey key;
		private final ModelUsageLimit limit;
		private ModelUsageLimitKey teamTotalKey;
		private ModelUsageLimit teamTotal;
		private String accessSince;
		private State state;
		private Reason reason;

		/**
		 * Creates a step, in the state its limit gives it: defined, or no limit
		 * defined.
		 *
		 * @param layer     a ceiling, or a limit a call can be billed to
		 * @param limitType which limit the step checks
		 * @param key       where that limit is defined
		 * @param limit     the limit, or null when none is defined there
		 */
		public Step(Layer layer, StepLimitType limitType, ModelUsageLimitKey key, ModelUsageLimit limit) {
			this.layer = layer;
			this.limitType = limitType;
			this.key = key;
			this.limit = limit;
			this.state = limit == null ? State.NOT_DEFINED : State.DEFINED;
		}

		/**
		 * Attaches the team's Team Total to this team step, so the ceiling is checked
		 * along with the team's limit.
		 *
		 * @param key   the team's total across all models
		 * @param limit its limit, or null
		 * @return this step
		 */
		public Step withTeamTotal(ModelUsageLimitKey key, ModelUsageLimit limit) {
			this.teamTotalKey = key;
			this.teamTotal = limit;
			return this;
		}

		/**
		 * Records when the person got access to the model through this step's team, as
		 * shown in the path view.
		 *
		 * @param accessSince when the person got access to the model through this team,
		 *                    as an ISO date and time in UTC, or null
		 * @return this step
		 */
		public Step withAccessSince(String accessSince) {
			this.accessSince = accessSince;
			return this;
		}

		/**
		 * Marks the step as one that cannot apply, with the reason.
		 *
		 * @param reason why the step cannot apply
		 * @return this step
		 */
		public Step skipped(Reason reason) {
			this.state = State.SKIPPED;
			this.reason = reason;
			return this;
		}

		public Layer getLayer() {
			return layer;
		}

		/**
		 * Which limit the step checks, such as the Direct Limit or a Team Limit.
		 *
		 * @return the step's limit type
		 */
		public StepLimitType getLimitType() {
			return limitType;
		}

		public ModelUsageLimitKey getKey() {
			return key;
		}

		public ModelUsageLimit getLimit() {
			return limit;
		}

		public ModelUsageLimitKey getTeamTotalKey() {
			return teamTotalKey;
		}

		public ModelUsageLimit getTeamTotal() {
			return teamTotal;
		}

		public String getAccessSince() {
			return accessSince;
		}

		public State getState() {
			return state;
		}

		public void setState(State state) {
			this.state = state;
		}

		public Reason getReason() {
			return reason;
		}

		/**
		 * Whether the step's source has a limit, rather than No Limit Defined.
		 *
		 * @return whether the step has a limit defined, rules or Unlimited
		 */
		public boolean isDefined() {
			return limit != null;
		}

		/**
		 * Whether the step's limit is an explicit Unlimited, which ends the search for
		 * the source billed.
		 *
		 * @return whether the step is an explicit Unlimited
		 */
		public boolean isUnlimited() {
			return limit != null && limit.isUnlimited();
		}
	}

	private final String engineId;
	private final String userId;
	private final String userType;
	private final List<Step> ceilings = new ArrayList<>();
	private final List<Step> steps = new ArrayList<>();
	private StepLimitType billedLimitType;
	private final List<Step> billingOrder = new ArrayList<>();

	/**
	 * Creates an empty path for a person on a model, which the resolver fills in
	 * with the ceilings and the steps.
	 *
	 * @param engineId the model
	 * @param userId   the person's primary login id
	 * @param userType the person's primary login type
	 */
	public ModelUsageLimitPath(String engineId, String userId, String userType) {
		this.engineId = engineId;
		this.userId = userId;
		this.userType = userType;
	}

	public void addCeiling(Step step) {
		ceilings.add(step);
	}

	public void addStep(Step step) {
		steps.add(step);
	}

	/**
	 * Sets the source a call is billed to and the steps it can be billed to,
	 * replacing any order set before.
	 *
	 * @param limitType the type of limit a call is billed to, or null for no limit
	 * @param order     the steps a call can be billed to, in order: one step, or
	 *                  the teams in billing order
	 */
	public void setBilled(StepLimitType limitType, List<Step> order) {
		this.billedLimitType = limitType;
		this.billingOrder.clear();
		if (order != null) {
			this.billingOrder.addAll(order);
		}
	}

	public String getEngineId() {
		return engineId;
	}

	public String getUserId() {
		return userId;
	}

	public String getUserType() {
		return userType;
	}

	/**
	 * The ceiling steps on the path, as a list that cannot be changed.
	 *
	 * @return the ceilings, which every call must fit in
	 */
	public List<Step> getCeilings() {
		return Collections.unmodifiableList(ceilings);
	}

	/**
	 * The steps for the sources a call can be billed to, as a list that cannot be
	 * changed.
	 *
	 * @return the sources a call can be billed to, from the top: direct, each team
	 *         with a grant on the model, public use, the all-models default and the
	 *         platform default
	 */
	public List<Step> getSteps() {
		return Collections.unmodifiableList(steps);
	}

	/**
	 * The type of limit billed, which the resolver takes from the first source with
	 * a limit defined.
	 *
	 * @return the source a call is billed to, or null when no limit applies
	 */
	public StepLimitType getBilledLimitType() {
		return billedLimitType;
	}

	/**
	 * The steps a call can be billed to: the one source billed, or the teams with a
	 * limit defined, which a call moves through when one has none left.
	 *
	 * @return the steps a call can be billed to, in order; for teams, Unlimited
	 *         teams first and then by when the person got access through each
	 */
	public List<Step> getBillingOrder() {
		return Collections.unmodifiableList(billingOrder);
	}
}
