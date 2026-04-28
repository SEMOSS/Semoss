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
package prerna.rpa.jobrunr.jobs;

import org.jobrunr.jobs.lambdas.JobRequest;
import org.jobrunr.jobs.lambdas.JobRequestHandler;

public class JobRunrPixelExecutionJobRequest implements JobRequest {

	private String pixelScript;
	private String pixelParameters;
	private String userAccess;
	private String execId;
	private String jobId;
	private String jobGroup;
	private String jobName;

	public JobRunrPixelExecutionJobRequest() {
		this(null, null, null, null, null, null, null);
	}

	/**
	 * Create a new Pixel execution job request
	 * 
	 * @param pixelScript     The Pixel script/code to execute
	 * @param pixelParameters Parameters for the Pixel script
	 * @param userAccess      Encrypted user access token
	 * @param execId          Execution ID for tracking
	 * @param jobId           Job identifier
	 * @param jobGroup        Job group identifier
	 */
	public JobRunrPixelExecutionJobRequest(String pixelScript, String pixelParameters, String userAccess, String execId,
			String jobId, String jobGroup, String jobName) {
		this.pixelScript = pixelScript;
		this.pixelParameters = pixelParameters;
		this.userAccess = userAccess;
		this.execId = execId;
		this.jobId = jobId;
		this.jobGroup = jobGroup;
		this.jobName = jobName;
	}

	@Override
	public Class<? extends JobRequestHandler> getJobRequestHandler() {
		return JobRunrPixelExecutionJobHandler.class;
	}

	// Getters and Setters for JSON serialization

	public String getPixelScript() {
		return pixelScript;
	}

	public void setPixelScript(String pixelScript) {
		this.pixelScript = pixelScript;
	}

	public String getPixelParameters() {
		return pixelParameters;
	}

	public void setPixelParameters(String pixelParameters) {
		this.pixelParameters = pixelParameters;
	}

	public String getUserAccess() {
		return userAccess;
	}

	public void setUserAccess(String userAccess) {
		this.userAccess = userAccess;
	}

	public String getExecId() {
		return execId;
	}

	public void setExecId(String execId) {
		this.execId = execId;
	}

	public String getJobId() {
		return jobId;
	}

	public void setJobId(String jobId) {
		this.jobId = jobId;
	}

	public String getJobGroup() {
		return jobGroup;
	}

	public String getJobName() {
		return jobName;
	}

	public void setJobGroup(String jobGroup) {
		this.jobGroup = jobGroup;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	@Override
	public String toString() {
		return "JobRunrPixelExecutionJobRequest{" + "pixelScript='"
				+ (pixelScript != null ? pixelScript.substring(0, Math.min(50, pixelScript.length())) + "..." : "null")
				+ '\'' + ", execId='" + execId + '\'' + ", jobId='" + jobId + '\'' + ", jobGroup='" + jobGroup + '\''
				+ '}';
	}
}
