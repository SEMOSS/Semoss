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

/**
 * The access a subject has, or will have: a level, an optional end date, and
 * for a model engine an optional usage limit, {@link #getUsageLimit()}. In
 * JSON, a {@code usageLimit} of null is the same as leaving it out: no limit
 * defined.
 *
 * <p>
 * For a model's public use and its total the grant carries only
 * {@code usageLimit}, with no level or end date. Only the model's owners, and
 * admin mode, may change a usage limit; anyone else must leave it as it is.
 * </p>
 */
public class PermissionGrant {

	/** {@code OWNER}, {@code EDIT} or {@code READ_ONLY} */
	private String level;
	/**
	 * When the access ends, as an ISO date and time with a zone, or null for never
	 */
	private String endDate;
	/** A model engine's usage limit, or null for no limit defined */
	private ModelUsageLimit usageLimit;
	/**
	 * Whether the usage limit was given, null included, by a Java caller; not sent
	 * or read as JSON. It is transient so Gson skips it, which is why a batch read
	 * from JSON cannot tell a null {@code usageLimit} from one left out. With
	 * {@code keepingModelLimits()}, a change that gives neither keeps the current
	 * limit.
	 */
	private transient boolean usageLimitGiven;

	public PermissionGrant() {

	}

	/**
	 * Creates a grant with a level and an end date, and no usage limit given.
	 *
	 * @param level   {@code OWNER}, {@code EDIT} or {@code READ_ONLY}
	 * @param endDate when the access ends, as an ISO date and time with a zone, or
	 *                null for never
	 */
	public PermissionGrant(String level, String endDate) {
		this.level = level;
		this.endDate = endDate;
	}

	/**
	 * Sets a model engine's usage limit.
	 *
	 * @param usageLimit the limit, or null for no limit defined
	 * @return this grant
	 */
	public PermissionGrant withUsageLimit(ModelUsageLimit usageLimit) {
		this.usageLimit = usageLimit;
		this.usageLimitGiven = true;
		return this;
	}

	/**
	 * Whether a Java caller set the usage limit, through
	 * {@link #withUsageLimit(ModelUsageLimit)} or
	 * {@link #setUsageLimit(ModelUsageLimit)}; always false for a grant read from
	 * JSON.
	 *
	 * @return whether the usage limit was given, null included, so a change that
	 *         keeps the current limit when none is given sets this one
	 */
	public boolean isUsageLimitGiven() {
		return usageLimitGiven;
	}

	/**
	 * The usage limit the grant gives on a model engine.
	 *
	 * @return a model engine's usage limit, or null for no limit defined
	 */
	public ModelUsageLimit getUsageLimit() {
		return usageLimit;
	}

	public void setUsageLimit(ModelUsageLimit usageLimit) {
		this.usageLimit = usageLimit;
		this.usageLimitGiven = true;
	}

	public String getLevel() {
		return level;
	}

	public String getEndDate() {
		return endDate;
	}
}
