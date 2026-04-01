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

import java.util.List;

public class WorkflowConfig {
	public String workflowKey;
	public String workflowName;
	public String workflowTemplateKey;
	public Integer version;
	public Long tenantId;
	public String status;
	public Trigger trigger;
	public ExecutionConfig execution;
	public List<Action> actions;
	public List<List<String>> flow;
	public ErrorHandling errorHandling;

	public String getWorkflowKey() {
		return workflowKey;
	}

	public void setWorkflowKey(String workflowKey) {
		this.workflowKey = workflowKey;
	}

	public String getWorkflowName() {
		return workflowName;
	}

	public void setWorkflowName(String workflowName) {
		this.workflowName = workflowName;
	}

	public String getWorkflowTemplateKey() {
		return workflowTemplateKey;
	}

	public void setWorkflowTemplateKey(String workflowTemplateKey) {
		this.workflowTemplateKey = workflowTemplateKey;
	}

	public Integer getVersion() {
		return version;
	}

	public void setVersion(Integer version) {
		this.version = version;
	}

	public Long getTenantId() {
		return tenantId;
	}

	public void setTenantId(Long tenantId) {
		this.tenantId = tenantId;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public Trigger getTrigger() {
		return trigger;
	}

	public void setTrigger(Trigger trigger) {
		this.trigger = trigger;
	}

	public ExecutionConfig getExecution() {
		return execution;
	}

	public void setExecution(ExecutionConfig execution) {
		this.execution = execution;
	}

	public List<Action> getActions() {
		return actions;
	}

	public void setActions(List<Action> actions) {
		this.actions = actions;
	}

	public List<List<String>> getFlow() {
		return flow;
	}

	public void setFlow(List<List<String>> flow) {
		this.flow = flow;
	}

	public ErrorHandling getErrorHandling() {
		return errorHandling;
	}

	public void setErrorHandling(ErrorHandling errorHandling) {
		this.errorHandling = errorHandling;
	}

}
