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
 * Which model usage limit: where one is defined, the key of one row of
 * {@code MODELUSAGELIMIT}. The factories, with the names the UI uses:
 * <ul>
 * <li>{@link #direct}: a person's grant on a model, the Direct Limit</li>
 * <li>{@link #team}: a team's grant on a model, the Team Limit</li>
 * <li>{@link #publicUse}: a model's public use, the Public Use Limit</li>
 * <li>{@link #allModels}: a person's limit across all models, the User's Total
 * Model Limit</li>
 * <li>{@link #platform}: the General Platform Limit, for everyone with no other
 * limit</li>
 * <li>{@link #modelTotal}: a model's total, for everyone together, a
 * ceiling</li>
 * <li>{@link #teamTotal}: a team's total across all models, a ceiling</li>
 * </ul>
 * A ceiling is checked on every call on top of the limit that applies, and is
 * never the limit a call is billed to ({@link #isCeiling()}).
 *
 * @param type        what type of limit it is, kept in {@code LIMITTYPE}
 * @param engineId    the model, or null for a limit across all models
 * @param subjectId   the user or team id, or null for public use, the platform
 *                    and a model's total
 * @param subjectType the person's login type, or the team's type. A person's
 *                    grant on a model keeps the login type too, though
 *                    {@code ENGINEPERMISSION} keeps only the user id; it is
 *                    null only for a grant's limit whose login type is not
 *                    known
 */
public record ModelUsageLimitKey(Type type, String engineId, String subjectId, String subjectType) {

	/** What type of limit a key names */
	public enum Type {
		/** A person's grant on a model, or their all-models default */
		USER,
		/** A team's grant on a model, or the team's total across all models */
		TEAM,
		/** A model's public use */
		PUBLIC,
		/** The platform default, for everyone with no other limit */
		PLATFORM,
		/** A model's total, for everyone together */
		MODEL
	}

	/**
	 * The key of the Direct Limit on a person's grant on a model.
	 *
	 * @param engineId the model
	 * @param userId   the user id
	 * @param userType the login type, or null when it is not known
	 * @return the limit on a person's grant on a model
	 */
	public static ModelUsageLimitKey direct(String engineId, String userId, String userType) {
		return new ModelUsageLimitKey(Type.USER, engineId, userId, userType);
	}

	/**
	 * The key of the User's Total Model Limit, a person's default across all
	 * models.
	 *
	 * @param userId   the user id
	 * @param userType the login type
	 * @return a person's all-models default
	 */
	public static ModelUsageLimitKey allModels(String userId, String userType) {
		return new ModelUsageLimitKey(Type.USER, null, userId, userType);
	}

	/**
	 * The key of the Team Limit on a team's grant on a model.
	 *
	 * @param engineId the model
	 * @param teamId   the team id
	 * @param teamType the team's type, such as {@code CUSTOM}
	 * @return the limit on a team's grant on a model
	 */
	public static ModelUsageLimitKey team(String engineId, String teamId, String teamType) {
		return new ModelUsageLimitKey(Type.TEAM, engineId, teamId, teamType);
	}

	/**
	 * The key of a Team Total, the ceiling on a team's use across all models.
	 *
	 * @param teamId   the team id
	 * @param teamType the team's type, such as {@code CUSTOM}
	 * @return a team's total across all models
	 */
	public static ModelUsageLimitKey teamTotal(String teamId, String teamType) {
		return new ModelUsageLimitKey(Type.TEAM, null, teamId, teamType);
	}

	/**
	 * The key of a model's Public Use Limit, which applies only when the model is
	 * public.
	 *
	 * @param engineId the model
	 * @return the model's public use
	 */
	public static ModelUsageLimitKey publicUse(String engineId) {
		return new ModelUsageLimitKey(Type.PUBLIC, engineId, null, null);
	}

	/**
	 * The key of a Model Total, the ceiling on everyone's use of a model together.
	 *
	 * @param engineId the model
	 * @return the model's total, for everyone together
	 */
	public static ModelUsageLimitKey modelTotal(String engineId) {
		return new ModelUsageLimitKey(Type.MODEL, engineId, null, null);
	}

	/**
	 * The key of the General Platform Limit, the platform default for everyone with
	 * no other limit.
	 *
	 * @return the platform default
	 */
	public static ModelUsageLimitKey platform() {
		return new ModelUsageLimitKey(Type.PLATFORM, null, null, null);
	}

	/**
	 * Whether the key names a Direct Limit, a person's limit on one model rather
	 * than their default across all models.
	 *
	 * @return whether the limit is on a person's grant on a model
	 */
	public boolean isDirect() {
		return type == Type.USER && engineId != null;
	}

	/**
	 * A copy of the key with no login type, so Direct Limits on the same grant
	 * compare equal whatever login type each is kept with.
	 *
	 * @return the same limit with no login type, as a grant's limit whose login
	 *         type is not known is kept
	 */
	public ModelUsageLimitKey withoutSubjectType() {
		return new ModelUsageLimitKey(type, engineId, subjectId, null);
	}

	/**
	 * Whether the key names a ceiling: a Model Total or a Team Total.
	 *
	 * @return whether the limit is a ceiling, checked on top of whatever a call is
	 *         billed to and never billed itself
	 */
	public boolean isCeiling() {
		return type == Type.MODEL || (type == Type.TEAM && engineId == null);
	}
}
