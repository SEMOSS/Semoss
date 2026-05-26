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

public class FileEmbeddingStatus {
	private String fileName;
    private String status;
    private long insertedRecords;
    private long failedRecords;
    private long totalRecords;
	private Map<String, Object> error;

    public FileEmbeddingStatus() {}

    public FileEmbeddingStatus(String fileName, String status, long insertedRecords, long failedRecords, long totalRecords) {
    	this.fileName = fileName;
        this.status = status;
        this.insertedRecords = insertedRecords;
        this.failedRecords = failedRecords;
        this.totalRecords = totalRecords;
    }

	public FileEmbeddingStatus(String fileName, String status, long insertedRecords, long failedRecords,
				long totalRecords, Map<String, Object> error) {
		this.fileName = fileName;
		this.status = status;
		this.insertedRecords = insertedRecords;
		this.failedRecords = failedRecords;
		this.totalRecords = totalRecords;
		this.error = error;
	}
    
    public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public long getInsertedRecords() {
		return insertedRecords;
	}

	public void setInsertedRecords(long insertedRecords) {
		this.insertedRecords = insertedRecords;
	}

	public long getFailedRecords() {
		return failedRecords;
	}

	public void setFailedRecords(long failedRecords) {
		this.failedRecords = failedRecords;
	}

	public long getTotalRecords() {
		return totalRecords;
	}

	public void setTotalRecords(long totalRecords) {
		this.totalRecords = totalRecords;
	}

	public Map<String, Object> getError() {
		return error;
	}

	public void setError(Map<String, Object> error) {
		this.error = error;
	}

   
}
