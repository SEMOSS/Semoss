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
package prerna.engine.impl.migration;

import java.sql.Timestamp;

/**
 * Plain data holder for a single row of SEMOSS_MIGRATIONS (Security DB) — one
 * version of a versioned migration definition. Content is immutable once
 * created; a "new version" is always a new row, never an edit of an existing
 * one (mirrors PROMPT's VERSION/IS_LATEST convention).
 */
public class MigrationDefinition {

	private final String migrationId;
	private final String engineId;
	private final int version;
	private final String scriptName;
	private final String sqlContent;
	private final boolean isLatest;
	private final String createdBy;
	private final Timestamp createdOn;
	private final String notes;

	public MigrationDefinition(String migrationId, String engineId, int version, String scriptName,
			String sqlContent, boolean isLatest, String createdBy, Timestamp createdOn, String notes) {
		this.migrationId = migrationId;
		this.engineId = engineId;
		this.version = version;
		this.scriptName = scriptName;
		this.sqlContent = sqlContent;
		this.isLatest = isLatest;
		this.createdBy = createdBy;
		this.createdOn = createdOn;
		this.notes = notes;
	}

	public String getMigrationId() {
		return migrationId;
	}

	public String getEngineId() {
		return engineId;
	}

	public int getVersion() {
		return version;
	}

	public String getScriptName() {
		return scriptName;
	}

	public String getSqlContent() {
		return sqlContent;
	}

	public boolean isLatest() {
		return isLatest;
	}

	public String getCreatedBy() {
		return createdBy;
	}

	public Timestamp getCreatedOn() {
		return createdOn;
	}

	public String getNotes() {
		return notes;
	}

}
