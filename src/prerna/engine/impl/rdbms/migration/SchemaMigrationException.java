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

/**
 * Thrown when a pending migration cannot be safely run -- a failed SQL
 * migration, an out-of-order version, a checksum mismatch against an
 * already-applied version, or a folder/history read failure. Always thrown
 * from inside {@code IRDBMSEngine.open(Properties)}, so by design it
 * propagates out of {@code open()} and the engine is never registered
 * (confirmed in {@code Utility.loadEngine} -- a thrown {@code open()}
 * exception nulls out the engine before it reaches {@code DIHelper}), rather
 * than leaving a partially-migrated engine usable.
 */
public class SchemaMigrationException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	/** The version string of the migration that directly caused this failure, or {@code null} for infrastructure failures (folder creation, lock timeout, etc.). */
	private final String failedVersion;

	public SchemaMigrationException(String message) {
		super(message);
		this.failedVersion = null;
	}

	public SchemaMigrationException(String message, String failedVersion) {
		super(message);
		this.failedVersion = failedVersion;
	}

	public SchemaMigrationException(String message, Throwable cause) {
		super(message, cause);
		this.failedVersion = null;
	}

	public String getFailedVersion() {
		return failedVersion;
	}

}
