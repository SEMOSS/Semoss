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
package prerna.engine.impl.model.inferencetracking.workers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.impl.model.inferencetracking.MemoryUtils;
import prerna.om.Insight;

/**
 * Backfills a memory's EVENT_TYPE by calling a reasoning model in the
 * background, mirroring
 * {@link prerna.engine.impl.model.workers.ModelEngineInferenceLogsWorker}'s
 * fire-and-forget pattern for logging. The memory row is inserted synchronously
 * with a default "memory" type so AddMemoryReactor can return immediately; this
 * worker only runs when a reasoning engine was supplied (classification is
 * opt-in) and updates the row afterward.
 *
 * <p>
 * Embedding-based duplicate detection stays on the synchronous path (in
 * AddMemoryReactor) since the caller's return value - either a new memory id or
 * an existing duplicate's id - depends on it.
 */
public class MemoryClassificationWorker implements Runnable {

	private static final Logger classLogger = LogManager.getLogger(MemoryClassificationWorker.class);

	private final String memoryId;
	private final String reasoningEngineId;
	private final Insight insight;
	private final String content;

	public MemoryClassificationWorker(String memoryId, String reasoningEngineId, Insight insight, String content) {
		this.memoryId = memoryId;
		this.reasoningEngineId = reasoningEngineId;
		this.insight = insight;
		this.content = content;
	}

	@Override
	public void run() {
		try {
			String eventType = MemoryUtils.classifyEventType(reasoningEngineId, insight, content);
			// "memory" is already the default the row was inserted with; skip the
			// redundant write when the model agreed with the default.
			if (!"memory".equals(eventType)) {
				MemoryUtils.updateEventType(memoryId, eventType);
			}
		} catch (Exception e) {
			classLogger.warn("Async memory classification failed for memory '{}'; leaving default type.", memoryId, e);
		}
	}

}
