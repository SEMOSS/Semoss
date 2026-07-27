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
package prerna.util;

import java.util.List;

import prerna.auth.utils.SecurityOwlCreator;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsOwlCreator;
import prerna.engine.impl.owl.AbstractOwlCreator;
import prerna.engine.impl.owl.AbstractOwlCreator.OwlColumn;
import prerna.engine.logging.AuditLogsDbOwlCreator;
import prerna.masterdatabase.utility.LocalMasterOwlCreator;
import prerna.notifications.NotificationOwlCreator;
import prerna.prompt.PromptOwlCreator;
import prerna.reactor.scheduler.SchedulerOwlCreator;
import prerna.theme.ThemeOwlCreator;
import prerna.usertracking.UserTrackingOwlCreator;
import prerna.util.sql.AbstractSqlQueryUtil;

/**
 * Resolves the declared OWL schema (table/column/datatype) for the system
 * default databases. This lives in {@code prerna.auth.utils} because that
 * package is on the {@link SystemEngineRegistry} access allowlist for every
 * system engine - callers outside the allowlist (e.g. reactors) delegate here
 * rather than touching the registry directly.
 */
public class SystemDatabaseSchemaUtils {

	private SystemDatabaseSchemaUtils() {

	}

	/**
	 * @param databaseId the engine id to test
	 * @return true if the id is one of the system default databases (the engines
	 *         backed by an OWL creator)
	 */
	public static boolean isSystemDatabase(String databaseId) {
		return databaseId != null && SystemDefaultEngines.getDatabasesWithGeneratedOwl().contains(databaseId);
	}

	/**
	 * Returns the declared schema - one {@link OwlColumn} (table, column, datatype)
	 * per column - for a system default database, sourced from that engine's OWL
	 * creator.
	 *
	 * @param databaseId one of the {@link SystemDefaultEngines} ids
	 * @return the flattened table/column/datatype schema
	 * @throws IllegalArgumentException if the id is not a system default database
	 */
	public static List<OwlColumn> getSystemDatabaseSchema(String databaseId) {
		if (!isSystemDatabase(databaseId)) {
			throw new IllegalArgumentException("'" + databaseId + "' is not a system default database. Valid ids are: "
					+ SystemDefaultEngines.getDatabasesWithGeneratedOwl());
		}

		IRDBMSEngine engine = SystemEngineRegistry.getSystemEngine(databaseId);
		AbstractSqlQueryUtil queryUtil = engine.getQueryUtil();

		AbstractOwlCreator owlCreator = switch (databaseId) {
		case Constants.SECURITY_DB -> new SecurityOwlCreator(queryUtil);
		case Constants.LOCAL_MASTER_DB -> new LocalMasterOwlCreator(queryUtil);
		case Constants.SCHEDULER_DB -> new SchedulerOwlCreator();
		case Constants.THEMING_DB -> new ThemeOwlCreator(queryUtil);
		case Constants.USER_TRACKING_DB -> new UserTrackingOwlCreator(queryUtil);
		case Constants.PROMPT_DB -> new PromptOwlCreator(queryUtil);
		case Constants.NOTIFICATION_DB -> new NotificationOwlCreator(queryUtil);
		case Constants.AUDIT_LOGS_DB -> new AuditLogsDbOwlCreator(queryUtil);
		case Constants.MODEL_INFERENCE_LOGS_DB -> new ModelInferenceLogsOwlCreator(queryUtil);
		default ->
			throw new IllegalArgumentException("No OWL creator is mapped for system database '" + databaseId + "'");
		};

		return owlCreator.getSchemaColumns();
	}
}
