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
 * for a model engine optional usage limits.
 */
public class PermissionGrant {

	/** {@code OWNER}, {@code EDIT} or {@code READ_ONLY} */
	private String level;
	/**
	 * When the access ends, as an ISO date and time with a zone, or null for never
	 */
	private String endDate;
	private String usageRestriction;
	private String usageFrequency;
	private Integer maxTokens;
	private Double maxResponseTime;
	private Double maxCredits;

	public PermissionGrant() {

	}

	/**
	 * @param level   {@code OWNER}, {@code EDIT} or {@code READ_ONLY}
	 * @param endDate when the access ends, as an ISO date and time with a zone, or
	 *                null for never
	 */
	public PermissionGrant(String level, String endDate) {
		this.level = level;
		this.endDate = endDate;
	}

	/**
	 * Sets a model engine's usage limits.
	 *
	 * @param usageRestriction the usage restriction, or null
	 * @param usageFrequency   how often the limits reset, or null
	 * @param maxTokens        the token limit, or null for none
	 * @param maxResponseTime  the response time limit, or null for none
	 * @return this grant
	 */
	public PermissionGrant withUsageLimits(String usageRestriction, String usageFrequency, Number maxTokens,
			Number maxResponseTime) {
		return withUsageLimits(usageRestriction, usageFrequency, maxTokens, maxResponseTime, null);
	}

	/**
	 * Sets a model engine's usage limits, including a credit limit.
	 *
	 * @param usageRestriction the usage restriction, or null
	 * @param usageFrequency   how often the limits reset, or null
	 * @param maxTokens        the token limit, or null for none
	 * @param maxResponseTime  the response time limit, or null for none
	 * @param maxCredits       the credit limit, or null for none
	 * @return this grant
	 */
	public PermissionGrant withUsageLimits(String usageRestriction, String usageFrequency, Number maxTokens,
			Number maxResponseTime, Number maxCredits) {
		this.usageRestriction = usageRestriction;
		this.usageFrequency = usageFrequency;
		this.maxTokens = maxTokens == null ? null : maxTokens.intValue();
		this.maxResponseTime = maxResponseTime == null ? null : maxResponseTime.doubleValue();
		this.maxCredits = maxCredits == null ? null : maxCredits.doubleValue();
		return this;
	}

	public String getLevel() {
		return level;
	}

	public String getEndDate() {
		return endDate;
	}

	public String getUsageRestriction() {
		return usageRestriction;
	}

	public String getUsageFrequency() {
		return usageFrequency;
	}

	public Integer getMaxTokens() {
		return maxTokens;
	}

	public Double getMaxResponseTime() {
		return maxResponseTime;
	}

	public Double getMaxCredits() {
		return maxCredits;
	}

	public void setUsageRestriction(String usageRestriction) {
		this.usageRestriction = usageRestriction;
	}

	public void setUsageFrequency(String usageFrequency) {
		this.usageFrequency = usageFrequency;
	}

	public void setMaxTokens(Integer maxTokens) {
		this.maxTokens = maxTokens;
	}

	public void setMaxResponseTime(Double maxResponseTime) {
		this.maxResponseTime = maxResponseTime;
	}

	public void setMaxCredits(Double maxCredits) {
		this.maxCredits = maxCredits;
	}
}
