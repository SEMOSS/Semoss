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
package prerna.logging;

import java.sql.Timestamp;

public class LogActivityDto {

	private String requestId;

	private java.sql.Timestamp startTime;
	private java.sql.Timestamp endTime;
	private java.sql.Timestamp logTimestamp;

	private String request;
	private String response;
	private int tokens;

	private long latency;
	private boolean status;
	private String engineName;
	private String engineType;
	private String methodName;
	private String userId;
	private String sessionId;
	private String spanId;

	public LogActivityDto() {

	}

	public LogActivityDto(String requestId, java.sql.Timestamp startTime, java.sql.Timestamp endTime, String request,
			String response, int tokens, long latency, boolean status, String engineName, String engineType,
			String methodName, String userId, String sessionId, String spanId, java.sql.Timestamp logTimestamp) {
		this.requestId = requestId;
		this.startTime = startTime;
		this.endTime = endTime;
		this.request = request;
		this.response = response;
		this.tokens = tokens;
		this.latency = latency;
		this.status = status;
		this.engineName = engineName;
		this.engineType = engineType;
		this.methodName = methodName;
		this.userId = userId;
		this.sessionId = sessionId;
		this.spanId = spanId;
		this.logTimestamp = logTimestamp;
	}

	public String getRequestId() {
		return requestId;
	}

	public void setRequestId(String requestId) {
		this.requestId = requestId;
	}

	public Timestamp getStartTime() {
		return startTime;
	}

	public void setStartTime(Timestamp startTime) {
		this.startTime = startTime;
	}

	public Timestamp getEndTime() {
		return endTime;
	}

	public void setEndTime(Timestamp endTime) {
		this.endTime = endTime;
	}

	public String getRequest() {
		return request;
	}

	public void setRequest(String request) {
		this.request = request;
	}

	public String getResponse() {
		return response;
	}

	public void setResponse(String response) {
		this.response = response;
	}

	public int getTokens() {
		return tokens;
	}

	public void setTokens(int tokens) {
		this.tokens = tokens;
	}

	public long getLatency() {
		return latency;
	}

	public void setLatency(long latency) {
		this.latency = latency;
	}

	public boolean isStatus() {
		return status;
	}

	public void setStatus(boolean status) {
		this.status = status;
	}

	public String getEngineName() {
		return engineName;
	}

	public void setEngineName(String engineName) {
		this.engineName = engineName;
	}

	public String getEngineType() {
		return engineType;
	}

	public void setEngineType(String engineType) {
		this.engineType = engineType;
	}

	public String getMethodName() {
		return methodName;
	}

	public void setMethodName(String methodName) {
		this.methodName = methodName;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getSessionId() {
		return sessionId;
	}

	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}

	public String getSpanId() {
		return spanId;
	}

	public void setSpanId(String spanId) {
		this.spanId = spanId;
	}

	public java.sql.Timestamp getLogTimestamp() {
		return logTimestamp;
	}

	public void setLogTimestamp(java.sql.Timestamp logTimestamp) {
		this.logTimestamp = logTimestamp;
	}

}
