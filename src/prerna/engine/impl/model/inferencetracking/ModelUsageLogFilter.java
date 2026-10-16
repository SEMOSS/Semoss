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
package prerna.engine.impl.model.inferencetracking;

/**
 * Which {@code MESSAGE} rows a usage limit counter is filled from. A null value
 * leaves that column unfiltered.
 *
 * @param engineId          the model ({@code AGENT_ID}), or null for any model
 * @param userId            the person's primary login id, or null for anyone
 * @param userType          the person's primary login type; rows written before
 *                          {@code USER_TYPE} existed match any type
 * @param billedSourceType  the type of source the rows were billed to
 *                          ({@code BILLED_SOURCE_TYPE}), such as {@code TEAM}
 * @param billedSubjectId   the id of the source billed, such as the team id
 * @param billedSubjectType the type of the source billed, such as the team type
 * @param skipRouterRows    whether to leave out the rows a router model writes
 *                          for itself, so a routed call counts once across all
 *                          models
 */
public record ModelUsageLogFilter(String engineId, String userId, String userType, String billedSourceType,
		String billedSubjectId, String billedSubjectType, boolean skipRouterRows) {
}
