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
package prerna.reactor.agent.skill;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Disabled stub.
 *
 * <p>The previous bootstrap scanned {@code <BASE_FOLDER>/skills/} at startup and
 * upserted each subdirectory into a storage engine via {@code SKILL__} +
 * {@code SKILL_VERSION__}. That model is gone: skills are now Projects of type
 * {@code SKILL} (tagged {@code Skill_Project}), with content under
 * {@code <project>/version/assets/skill/} and versioning handled by the
 * project's git repo.
 *
 * <p>Migration of platform skills from the legacy install tree into the
 * new Skill-Project layout will be done manually (or via a one-off importer)
 * later; this class deliberately no-ops until then.
 */
public final class PlatformSkillBootstrap {

	private static final Logger logger = LogManager.getLogger(PlatformSkillBootstrap.class);

	private PlatformSkillBootstrap() {}

	public static void scan() {
		logger.info("PlatformSkillBootstrap: skill bootstrap is disabled. Platform skills are now Skill-Projects "
				+ "and must be migrated/seeded explicitly.");
	}
}
