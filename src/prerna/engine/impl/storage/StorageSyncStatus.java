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
package prerna.engine.impl.storage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The outcome of syncing a local folder to storage.
 *
 * This exists because a sync that half worked currently looks identical to one
 * that fully worked: the per file failures are logged and swallowed, and the
 * caller is told true either way. Returning this instead lets the caller see
 * that some files did not make it.
 *
 * Follows the same shape as FileEmbeddingStatus, so the JSON a caller gets back
 * reads the same way.
 *
 * The status is always meaningful. The three file lists are best effort and how
 * full they are depends on the engine: the native S3, Azure and GCS engines
 * walk files themselves so they can name them, while engines that delegate the
 * whole transfer in one call cannot enumerate what moved, so they report
 * SUCCESS with empty lists. Treat an empty uploadedFiles as "this engine does
 * not report names", not as "nothing was uploaded".
 *
 * @param storagePath   the storage folder that was synced to
 * @param status        SUCCESS, PARTIAL or FAILED, see of(...)
 * @param uploadedFiles keys written during this sync
 * @param skippedFiles  keys already in storage and unchanged, so not rewritten
 * @param failedFiles   keys that could not be written
 */
public record StorageSyncStatus(String storagePath, String status, List<String> uploadedFiles,
		List<String> skippedFiles, List<String> failedFiles) {

	public static final String SUCCESS = "SUCCESS";
	public static final String PARTIAL = "PARTIAL";
	public static final String FAILED = "FAILED";

	/**
	 * Defensive copies, and null lists become empty ones, so callers can hand over
	 * the lists they were accumulating and still treat this as a value.
	 */
	public StorageSyncStatus(String storagePath, String status, List<String> uploadedFiles, List<String> skippedFiles,
			List<String> failedFiles) {
		this.storagePath = storagePath;
		this.status = status;
		this.uploadedFiles = copyOf(uploadedFiles);
		this.skippedFiles = copyOf(skippedFiles);
		this.failedFiles = copyOf(failedFiles);
	}

	/**
	 * Builds a status, working out SUCCESS / PARTIAL / FAILED from what actually
	 * happened rather than making every caller decide.
	 *
	 * Nothing failed is SUCCESS, even when everything was skipped. Something failed
	 * but something else got through is PARTIAL. Everything failed is FAILED.
	 *
	 * @param storagePath   the storage folder that was synced to
	 * @param uploadedFiles keys written during this sync
	 * @param skippedFiles  keys already in storage and unchanged
	 * @param failedFiles   keys that could not be written
	 * @return the status
	 */
	public static StorageSyncStatus of(String storagePath, List<String> uploadedFiles, List<String> skippedFiles,
			List<String> failedFiles) {
		boolean anyFailed = failedFiles != null && !failedFiles.isEmpty();
		boolean anyMadeIt = (uploadedFiles != null && !uploadedFiles.isEmpty())
				|| (skippedFiles != null && !skippedFiles.isEmpty());

		String status = SUCCESS;
		if (anyFailed) {
			status = anyMadeIt ? PARTIAL : FAILED;
		}
		return new StorageSyncStatus(storagePath, status, uploadedFiles, skippedFiles, failedFiles);
	}

	/**
	 * @return true only when nothing failed
	 */
	public boolean isSuccess() {
		return SUCCESS.equals(this.status);
	}

	private static List<String> copyOf(List<String> values) {
		if (values == null || values.isEmpty()) {
			return Collections.emptyList();
		}
		return Collections.unmodifiableList(new ArrayList<>(values));
	}
}
