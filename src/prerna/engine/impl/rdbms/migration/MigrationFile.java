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
 * Plain data holder for a single {@code V<version>__<description>.sql} file
 * discovered under an engine's {@code assets/.migrations} folder. This is only
 * ever produced by {@link MigrationFileUtils#scanPendingMigrations} — files
 * are UI-created only, never hand-edited, so this is intentionally not a
 * mutable builder.
 */
public class MigrationFile {

	private final String version;
	private final String description;
	private final String fileName;
	private final String sqlContent;

	public MigrationFile(String version, String description, String fileName, String sqlContent) {
		this.version = version;
		this.description = description;
		this.fileName = fileName;
		this.sqlContent = sqlContent;
	}

	public String getVersion() {
		return version;
	}

	public String getDescription() {
		return description;
	}

	public String getFileName() {
		return fileName;
	}

	public String getSqlContent() {
		return sqlContent;
	}

}
