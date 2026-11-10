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
 * What one source allows on a model. A limit has three states, kept apart
 * everywhere a limit is passed around:
 * <ul>
 * <li>{@code null}: no limit defined. The source is skipped and the next source
 * in the order is tried
 * ({@link prerna.auth.utils.ModelUsageLimitResolver}).</li>
 * <li>{@link #unlimited()}: Unlimited, a decision that ends the search, so no
 * later source applies. The ceilings, a model's total and a team's total, still
 * do.</li>
 * <li>{@link #of(List)}: rules, which must all have usage left for a call to go
 * through.</li>
 * </ul>
 * An amount of 0 means no limit defined: saving a limit drops such rules, and a
 * limit left with no rules becomes {@code null}
 * ({@code SecurityModelUsageLimitUtils.normalize}).
 */
public class ModelUsageLimit {

	private boolean unlimited;
	private List<ModelUsageLimitRule> rules;

	public ModelUsageLimit() {

	}

	/**
	 * Creates a limit that is either an explicit Unlimited or a set of rules. The
	 * rules are copied, so a later change to the list given does not change the
	 * limit.
	 *
	 * @param unlimited whether this is an explicit Unlimited
	 * @param rules     the rules, none when unlimited
	 */
	public ModelUsageLimit(boolean unlimited, List<ModelUsageLimitRule> rules) {
		this.unlimited = unlimited;
		this.rules = rules == null ? new ArrayList<>() : new ArrayList<>(rules);
	}

	/**
	 * Creates an explicit Unlimited, which ends the search for the source billed;
	 * the ceilings still apply.
	 *
	 * @return an explicit Unlimited
	 */
	public static ModelUsageLimit unlimited() {
		return new ModelUsageLimit(true, null);
	}

	/**
	 * Creates a limit made of the given rules, every one of which a call must stay
	 * within.
	 *
	 * @param rules the rules
	 * @return a limit with these rules
	 */
	public static ModelUsageLimit of(List<ModelUsageLimitRule> rules) {
		return new ModelUsageLimit(false, rules);
	}

	/**
	 * If there is unlimited usage allowed on the model
	 * 
	 * @return
	 */
	public boolean isUnlimited() {
		return unlimited;
	}

	/**
	 * The rules of this limit, as a list that cannot be changed.
	 *
	 * @return the rules, never null and empty when unlimited
	 */
	public List<ModelUsageLimitRule> getRules() {
		return rules == null ? Collections.emptyList() : Collections.unmodifiableList(rules);
	}
}
