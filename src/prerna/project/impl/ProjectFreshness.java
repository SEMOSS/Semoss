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
package prerna.project.impl;

import java.time.LocalDateTime;

import prerna.date.SemossDate;

/**
 * The one comparison that decides whether a container's local timestamp for
 * something it derived from a project is behind the cluster timestamp for that
 * project.
 *
 * <p>
 * The cluster timestamp lives in the security db, which every container shares,
 * and records when a container last reported changing the content. The local
 * timestamp is held in memory by the container and records when that container
 * last derived something from that content. Two things in a project work this
 * way: the portal a container publishes into public_home, and the custom
 * reactor classes a container compiles. Both comparisons live here so they
 * cannot drift apart.
 *
 * <p>
 * Package private on purpose: this is how the classes in this package agree to
 * read a cluster timestamp against a local one, not an api for callers outside
 * it.
 */
final class ProjectFreshness {

	private ProjectFreshness() {

	}

	/**
	 * Whether the cluster timestamp is newer than this container's local timestamp,
	 * which means the container has to refresh what it derived.
	 *
	 * Both timestamps are UTC wall clocks. The cluster timestamp is stored and read
	 * back with no offset attached to it, and the local timestamp comes from
	 * {@link prerna.util.Utility#getCurrentZonedDateTimeUTC()}.
	 *
	 * A timestamp that is absent on either side reports as not behind. A missing
	 * cluster timestamp means no container has reported a change, and a missing
	 * local timestamp means this container has not derived anything yet; neither
	 * says the container is behind, and callers handle those cases with what else
	 * they know.
	 *
	 * @param clusterTimestamp UTC wall clock the cluster last recorded for the
	 *                         content, may be null
	 * @param localTimestamp   UTC wall clock of this container's last refresh, may
	 *                         be null
	 * @return true when the cluster timestamp is newer than the local timestamp,
	 *         false when it is not or when either is absent
	 */
	static boolean isBehind(LocalDateTime clusterTimestamp, SemossDate localTimestamp) {
		if (clusterTimestamp == null || localTimestamp == null) {
			return false;
		}
		LocalDateTime local = localTimestamp.getLocalDateTime();
		if (local == null) {
			return false;
		}
		return clusterTimestamp.isAfter(local);
	}

}
