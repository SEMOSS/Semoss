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
package prerna.engine.impl.rdbms.migration;

import java.sql.Timestamp;

/**
 * The derived, display-ready status of one migration version -- merges a
 * {@link MigrationFile} discovered on disk with its most recent
 * {@link MigrationHistoryRecord} row, the same way Flyway's {@code info}
 * command reports migration state. Produced only by
 * {@link MigrationStatusUtils#getStatus}.
 */
public class MigrationStatus {

	/** Mirrors the subset of Flyway's {@code info} states relevant to this design (no undo/repeatable support). */
	public enum State {
		/** File exists, never run. */
		PENDING,
		/** File exists, last run succeeded, checksum unchanged. */
		SUCCESS,
		/** File exists, last run failed. */
		FAILED,
		/** History row exists but the file is no longer present on disk. */
		MISSING,
		/** File exists and previously succeeded, but its content has changed since. */
		OUTDATED
	}

	private final String version;
	private final String description;
	private final String fileName;
	private final State state;
	private final String appliedBy;
	private final Timestamp appliedOn;
	private final long executionTimeMs;
	private final String errorMessage;

	public MigrationStatus(String version, String description, String fileName, State state, String appliedBy,
			Timestamp appliedOn, long executionTimeMs, String errorMessage) {
		this.version = version;
		this.description = description;
		this.fileName = fileName;
		this.state = state;
		this.appliedBy = appliedBy;
		this.appliedOn = appliedOn;
		this.executionTimeMs = executionTimeMs;
		this.errorMessage = errorMessage;
	}

	public String getVersion() {
		return version;
	}

	public String getDescription() {
		return description;
	}

	/** {@code null} for a {@link State#MISSING} row -- there is no file to name. */
	public String getFileName() {
		return fileName;
	}

	public State getState() {
		return state;
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

	/** Failure reason for {@link State#FAILED}, or an explanatory note for {@link State#OUTDATED}; otherwise {@code null}. */
	public String getErrorMessage() {
		return errorMessage;
	}

}
