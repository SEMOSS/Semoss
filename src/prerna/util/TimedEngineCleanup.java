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
package prerna.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IEngine;

public final class TimedEngineCleanup {

	private static final Logger classLogger = LogManager.getLogger(TimedEngineCleanup.class);

	private static volatile TimedEngineCleanup singleton = null;

	private final Map<String, IEngine> internalMap = new HashMap<>();
	private final Map<String, Timer> timers = new HashMap<>();

	private TimedEngineCleanup() {

	}

	public static TimedEngineCleanup getInstance() {
		if (singleton != null) {
			return singleton;
		}

		if (singleton == null) {
			synchronized (TimedEngineCleanup.class) {
				if (singleton != null) {
					return singleton;
				}

				singleton = new TimedEngineCleanup();
			}
		}

		return singleton;
	}

	/**
	 * 
	 * @param key
	 * @param engine
	 * @param timeoutMillis
	 */
	public synchronized void put(IEngine engine, long timeoutMillis) {
		String engineId = engine.getEngineId();
		internalMap.put(engineId, engine);

		Timer timer = new Timer(true);
		timer.schedule(new TimerTask() {
			@Override
			public void run() {
				onRemove(engineId);
				timer.cancel();
			}
		}, timeoutMillis);

		if (this.timers.containsKey(engineId)) {
			this.timers.remove(engineId).cancel();
		}
		this.timers.put(engineId, timer);
	}

	/**
	 * Get the timer set for this engine
	 * 
	 * @param engine
	 * @return
	 */
	public Timer getEngineTimer(IEngine engine) {
		return this.timers.get(engine.getEngineId());
	}

	/**
	 * 
	 * @param key
	 */
	protected synchronized void onRemove(String engineId) {
		// check that the engine is still loaded
		IEngine engine = (IEngine) DIHelper.getInstance().getEngineProperty(engineId);
		if (engine == null) {
			classLogger.info("Engine {} has already been removed", engineId);
			return;
		}

		// now try to actually remove from disk
		try {
			classLogger.info("Deleting engine {} from disk without removing any cloud backup or metadata", engineId);
			engine.delete();
		} catch (IOException e) {
			classLogger.error(
					"Failed to delete engine {} from disk during timed cleanup. Cloud backups and metadata were not modified.",
					engineId, e);
		}
	}

}
