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
package prerna.engine.impl.vector;

import java.util.Map;

/**
 * The outcome of embedding one file.
 *
 * The component names match the field names of the class this replaced, so the
 * JSON handed back to callers is unchanged.
 *
 * @param fileName        the file this status describes
 * @param status          "SUCCESS", "PARTIAL" or "FAILED"
 * @param insertedRecords rows successfully embedded
 * @param failedRecords   rows that could not be embedded
 * @param totalRecords    rows attempted
 * @param error           error detail from buildEmbeddingError, null when there
 *                        is nothing to report
 */
public record FileEmbeddingStatus(String fileName, String status, long insertedRecords, long failedRecords,
		long totalRecords, Map<String, Object> error) {

	/**
	 * For the common case of a status with no error attached.
	 */
	public FileEmbeddingStatus(String fileName, String status, long insertedRecords, long failedRecords,
			long totalRecords) {
		this(fileName, status, insertedRecords, failedRecords, totalRecords, null);
	}

	/**
	 * Copy of this status with the error attached. Replaces the old setError, since
	 * a record cannot be mutated after construction.
	 *
	 * @param error error detail to attach
	 * @return a new status, this one is left alone
	 */
	public FileEmbeddingStatus withError(Map<String, Object> error) {
		return new FileEmbeddingStatus(fileName, status, insertedRecords, failedRecords, totalRecords, error);
	}
}
