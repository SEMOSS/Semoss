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
package prerna.util.jobrunr.model;

import java.time.Instant;

/**
 * job metadata stored in the database.
 * Contains all information needed to recreate a job after pause/resume.
 * Provides enhanced tracking for execution, retries, and status.
 */
public class JobMetadata {
    
    private String jobId;
    private String jobName;
    private String jobGroup;
    private String cronExpression;
    private String timezone;
    private String jobRequestClass;
    private String jobRequestData;
    private JobStatus status;
    private boolean paused = false;
    private boolean isRunning = false;  // Execution guard
    private String userId;
    private int priority = 5;  // 1-10: Lower number = higher priority
    private int maxRetries = 3;
    private int retryCount = 0;
    private String lastExecutionStatus = "PENDING";  // PENDING, SUCCESS, FAILED
    private String errorMessage;
    private Instant lastErrorAt;
    private long executionCount = 0;
    private Instant lastExecutionAt;
    private String tags;
    private Instant createdAt;
    private Instant updatedAt;
    
    public JobMetadata() {
        this.status = JobStatus.ACTIVE;
        this.createdAt = Instant.now();
        this.updatedAt = Instant.now();
    }
    
    public JobMetadata(String jobId, String jobName, String jobGroup, 
                      String cronExpression, String timezone,
                      String jobRequestClass, String jobRequestData,
                      String userId) {
        this();
        this.jobId = jobId;
        this.jobName = jobName;
        this.jobGroup = jobGroup;
        this.cronExpression = cronExpression;
        this.timezone = timezone;
        this.jobRequestClass = jobRequestClass;
        this.jobRequestData = jobRequestData;
        this.userId = userId;
    }
    
    // Getters and Setters
    
    public String getJobId() {
        return jobId;
    }
    
    public void setJobId(String jobId) {
        this.jobId = jobId;
    }
    
    public String getJobName() {
        return jobName;
    }
    
    public void setJobName(String jobName) {
        this.jobName = jobName;
    }
    
    public String getJobGroup() {
        return jobGroup;
    }
    
    public void setJobGroup(String jobGroup) {
        this.jobGroup = jobGroup;
    }
    
    public String getCronExpression() {
        return cronExpression;
    }
    
    public void setCronExpression(String cronExpression) {
        this.cronExpression = cronExpression;
    }
    
    public String getTimezone() {
        return timezone;
    }
    
    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }
    
    public String getJobRequestClass() {
        return jobRequestClass;
    }
    
    public void setJobRequestClass(String jobRequestClass) {
        this.jobRequestClass = jobRequestClass;
    }
    
    public String getJobRequestData() {
        return jobRequestData;
    }
    
    public void setJobRequestData(String jobRequestData) {
        this.jobRequestData = jobRequestData;
    }
    
    public JobStatus getStatus() {
        return status;
    }
    
    public void setStatus(JobStatus status) {
        this.status = status;
        this.paused = (status == JobStatus.PAUSED);
        this.updatedAt = Instant.now();
    }
    
    public String getUserId() {
        return userId;
    }
    
    public void setUserId(String userId) {
        this.userId = userId;
    }
    
    public Instant getCreatedAt() {
        return createdAt;
    }
    
    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }
    
    public Instant getUpdatedAt() {
        return updatedAt;
    }
    
    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }
    
    public boolean isPaused() {
        return paused;
    }
    
    public void setPaused(boolean paused) {
        this.paused = paused;
        this.updatedAt = Instant.now();
    }
    
    public int getPriority() {
        return priority;
    }
    
    public void setPriority(int priority) {
        this.priority = priority;
    }
    
    public int getMaxRetries() {
        return maxRetries;
    }
    
    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }
    
    public long getExecutionCount() {
        return executionCount;
    }
    
    public void setExecutionCount(long executionCount) {
        this.executionCount = executionCount;
    }
    
    public Instant getLastExecutionAt() {
        return lastExecutionAt;
    }
    
    public void setLastExecutionAt(Instant lastExecutionAt) {
        this.lastExecutionAt = lastExecutionAt;
    }
    
    public String getTags() {
        return tags;
    }
    
    public void setTags(String tags) {
        this.tags = tags;
    }
    
    public boolean isRunning() {
        return isRunning;
    }
    
    public void setRunning(boolean running) {
        isRunning = running;
        this.updatedAt = Instant.now();
    }
    
    public int getRetryCount() {
        return retryCount;
    }
    
    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
        this.updatedAt = Instant.now();
    }
    
    public void incrementRetryCount() {
        this.retryCount++;
        this.updatedAt = Instant.now();
    }
    
    public String getLastExecutionStatus() {
        return lastExecutionStatus;
    }
    
    public void setLastExecutionStatus(String lastExecutionStatus) {
        this.lastExecutionStatus = lastExecutionStatus;
        this.updatedAt = Instant.now();
    }
    
    public String getErrorMessage() {
        return errorMessage;
    }
    
    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
        this.updatedAt = Instant.now();
    }
    
    public Instant getLastErrorAt() {
        return lastErrorAt;
    }
    
    public void setLastErrorAt(Instant lastErrorAt) {
        this.lastErrorAt = lastErrorAt;
    }
    
    @Override
    public String toString() {
        return "JobMetadata{" +
                "jobId='" + jobId + '\'' +
                ", jobName='" + jobName + '\'' +
                ", jobGroup='" + jobGroup + '\'' +
                ", cronExpression='" + cronExpression + '\'' +
                ", status=" + status +
                '}';
    }
}
