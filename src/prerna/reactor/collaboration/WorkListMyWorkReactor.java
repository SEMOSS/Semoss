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
package prerna.reactor.collaboration;

import java.util.LinkedHashMap;
import java.util.Map;

import prerna.collaboration.WorkTaskUtils;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// WorkListMyWork(status=["open"], topicId=["..."], includeSuggested=[false]);
public class WorkListMyWorkReactor extends AbstractWorkTaskReactor {
	private static final String STATUS = "status";
	private static final String TOPIC_ID = "topicId";
	private static final String INCLUDE_SUGGESTED = "includeSuggested";

	public WorkListMyWorkReactor() {
		this.keysToGet = new String[] { STATUS, TOPIC_ID, INCLUDE_SUGGESTED };
		this.keyRequired = new int[] { 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		Map<String, Object> filter = new LinkedHashMap<>();
		filter.put("status", getString(STATUS));
		filter.put("topicId", getString(TOPIC_ID));
		filter.put("includeSuggested", Boolean.TRUE.equals(getBoolean(INCLUDE_SUGGESTED)));
		return mapResult(WorkTaskUtils.listMyWork(getUser(), filter));
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description(
				"The owner's to-do list: their open work items (outcomes they are driving), each with its open steps, plus loose steps that belong to no work item. "
						+ "Use it when the owner asks what is on their plate, for their to-do list, or what they owe someone. "
						+ "It does not list mail waiting on a reply; use SearchMail for that")
				.key(STATUS, "open (default), done, or all").key(TOPIC_ID, "Only work items under this topic")
				.key(INCLUDE_SUGGESTED, key -> key.type(Boolean.class).description(
						"Also list the steps Brain pulled from mail that no work item holds yet; they are often noise"));
	}
}
