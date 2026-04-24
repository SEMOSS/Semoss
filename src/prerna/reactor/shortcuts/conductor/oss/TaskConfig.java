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
package prerna.reactor.shortcuts.conductor.oss;

import java.sql.Timestamp;

public class TaskConfig {
	public Long id;

	// FK - workflow_definition.id
	public Long workflowId;

	// Task details
	public String taskName; // e.g., file_extract_task
	public String taskReferenceName; // e.g., action1

	// Pixel logic
	public String pixel;

	// Execution config
	public Integer retryCount;
	public Integer retryDelaySeconds;
	public Integer timeoutSeconds;
	public Boolean async;

	// Audit fields
	public Timestamp createdAt;
	public Timestamp updatedAt;

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public Long getWorkflowId() {
		return workflowId;
	}

	public void setWorkflowId(Long workflowId) {
		this.workflowId = workflowId;
	}

	public String getTaskName() {
		return taskName;
	}

	public void setTaskName(String taskName) {
		this.taskName = taskName;
	}

	public String getTaskReferenceName() {
		return taskReferenceName;
	}

	public void setTaskReferenceName(String taskReferenceName) {
		this.taskReferenceName = taskReferenceName;
	}

	public String getPixel() {
		return pixel;
	}

	public void setPixel(String pixel) {
		this.pixel = pixel;
	}

	public Integer getRetryCount() {
		return retryCount;
	}

	public void setRetryCount(Integer retryCount) {
		this.retryCount = retryCount;
	}

	public Integer getRetryDelaySeconds() {
		return retryDelaySeconds;
	}

	public void setRetryDelaySeconds(Integer retryDelaySeconds) {
		this.retryDelaySeconds = retryDelaySeconds;
	}

	public Integer getTimeoutSeconds() {
		return timeoutSeconds;
	}

	public void setTimeoutSeconds(Integer timeoutSeconds) {
		this.timeoutSeconds = timeoutSeconds;
	}

	public Boolean getAsync() {
		return async;
	}

	public void setAsync(Boolean async) {
		this.async = async;
	}

	public Timestamp getCreatedAt() {
		return createdAt;
	}

	public void setCreatedAt(Timestamp createdAt) {
		this.createdAt = createdAt;
	}

	public Timestamp getUpdatedAt() {
		return updatedAt;
	}

	public void setUpdatedAt(Timestamp updatedAt) {
		this.updatedAt = updatedAt;
	}

}
