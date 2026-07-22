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
 * Plain data holder for a single row of SEMOSS_SCHEMA_HISTORY (Security DB) —
 * the run outcome for one specific (migrationId, version) from
 * SEMOSS_MIGRATIONS.
 */
public class MigrationRecord {

	private final String engineId;
	private final String migrationId;
	private final int version;
	private final String description;
	private final String scriptName;
	private final String checksum;
	private final String appliedBy;
	private final Timestamp appliedOn;
	private final long executionTimeMs;
	private final boolean success;

	public MigrationRecord(String engineId, String migrationId, int version, String description, String scriptName,
			String checksum, String appliedBy, Timestamp appliedOn, long executionTimeMs, boolean success) {
		this.engineId = engineId;
		this.migrationId = migrationId;
		this.version = version;
		this.description = description;
		this.scriptName = scriptName;
		this.checksum = checksum;
		this.appliedBy = appliedBy;
		this.appliedOn = appliedOn;
		this.executionTimeMs = executionTimeMs;
		this.success = success;
	}

	public String getEngineId() {
		return engineId;
	}

	public String getMigrationId() {
		return migrationId;
	}

	public int getVersion() {
		return version;
	}

	public String getDescription() {
		return description;
	}

	/**
	 * Alias for {@link #getDescription()} — on a failed run, this column is
	 * repurposed to carry the failure reason (see {@code SchemaMigrationRunner}).
	 */
	public String getErrorMessage() {
		return description;
	}

	public String getScriptName() {
		return scriptName;
	}

	public String getChecksum() {
		return checksum;
	}

	public String getAppliedBy() {
		return appliedBy;
	}

	public Timestamp getAppliedOn() {
		return appliedOn;
	}

	public long getExecutionTimeMs() {
		return executionTimeMs;
	}

	public boolean isSuccess() {
		return success;
	}

}
