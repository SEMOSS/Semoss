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
package prerna.reactor.shortcuts.temporal;

public class WorkflowEntity {
	public Long id;
	public String workflowKey;
	public String workflowTemplateKey;
	public String workflowName;
	public String description;
	public Integer version;
	public String status;
	public String triggerType;
	public String watchDirectory;
	public String filePattern;
	public Boolean recursiveWatch;
	public String cronExpression;
	public String apiEndpoint;
	public String temporalScheduleId;
	public String workflowJson;

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getWorkflowKey() {
		return workflowKey;
	}

	public void setWorkflowKey(String workflowKey) {
		this.workflowKey = workflowKey;
	}

	public String getWorkflowTemplateKey() {
		return workflowTemplateKey;
	}

	public void setWorkflowTemplateKey(String workflowTemplateKey) {
		this.workflowTemplateKey = workflowTemplateKey;
	}

	public String getWorkflowName() {
		return workflowName;
	}

	public void setWorkflowName(String workflowName) {
		this.workflowName = workflowName;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getTriggerType() {
		return triggerType;
	}

	public void setTriggerType(String triggerType) {
		this.triggerType = triggerType;
	}

	public String getWatchDirectory() {
		return watchDirectory;
	}

	public void setWatchDirectory(String watchDirectory) {
		this.watchDirectory = watchDirectory;
	}

	public String getFilePattern() {
		return filePattern;
	}

	public void setFilePattern(String filePattern) {
		this.filePattern = filePattern;
	}

	public boolean isRecursiveWatch() {
		return recursiveWatch;
	}

	public void setRecursiveWatch(boolean recursiveWatch) {
		this.recursiveWatch = recursiveWatch;
	}

	public String getCronExpression() {
		return cronExpression;
	}

	public void setCronExpression(String cronExpression) {
		this.cronExpression = cronExpression;
	}

	public String getTemporalScheduleId() {
		return temporalScheduleId;
	}

	public void setTemporalScheduleId(String temporalScheduleId) {
		this.temporalScheduleId = temporalScheduleId;
	}

	public String getWorkflowJson() {
		return workflowJson;
	}

	public void setWorkflowJson(String workflowJson) {
		this.workflowJson = workflowJson;
	}

}
