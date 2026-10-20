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
package prerna.engine.impl.model.usage;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Where usage limit counters live. A counter is one source and one rule's
 * window bucket, such as the cost shared by team Data on model M for 2026-10-12
 * UTC, and holds what is used and what calls in flight have reserved.
 *
 * <p>
 * {@link LocalModelUsageCounterStore} keeps them in memory for a single node.
 * {@link RedisModelUsageCounterStore} keeps them in Redis and is used when
 * SEMOSS runs in a cluster, since counters on each node would allow each node
 * the whole limit. Every method throws {@link ModelUsageStoreException} when
 * the store cannot be reached.
 * </p>
 *
 * <p>
 * Reservations expire, so a call that never settles, such as one lost with its
 * server, stops holding usage once its expiry passes. Counters expire a while
 * after their window bucket ends.
 * </p>
 */
public interface IModelUsageCounterStore {

	/**
	 * Reads what several counters hold now: what is used, what calls in flight have
	 * reserved, and whether each has been filled from the inference logs.
	 *
	 * @param keys the counters
	 * @return what each holds; a counter that does not exist is
	 *         {@link ModelUsageCount#EMPTY}
	 */
	Map<String, ModelUsageCount> get(Collection<String> keys);

	/**
	 * Reserves on several counters at once: either every hold is reserved or none
	 * is. Checking that each hold fits and reserving it are one atomic step, so two
	 * calls, on one server or on two, cannot both take the last of what is left.
	 *
	 * @param holds  what to reserve on each counter, with the limit it must stay
	 *               within
	 * @param expiry how long the reservation holds, at most, such as the call's
	 *               timeout
	 * @return the reservation, or null when any counter would go over its limit
	 */
	ModelUsageReservation reserve(List<ModelUsageHold> holds, Duration expiry);

	/**
	 * Replaces a reservation with what was used, in one step.
	 *
	 * @param reservation the reservation, or null to only add the charges
	 * @param released    what to release on each counter, or null to release
	 *                    everything the reservation still holds
	 * @param charges     what was used, on each counter it counts toward
	 */
	void settle(ModelUsageReservation reservation, Map<String, Long> released, List<ModelUsageCharge> charges);

	/**
	 * Fills a counter from the inference logs, once. A counter that has already
	 * been filled is left as it is. Otherwise what it has used becomes the larger
	 * of what it counted itself and the amount, since calls it counted may already
	 * be in the logs.
	 *
	 * @param key             the counter
	 * @param amount          what the logs say was used in the counter's window
	 * @param expiresAtMillis when the counter can be dropped, in epoch milliseconds
	 */
	void seed(String key, long amount, long expiresAtMillis);

	/**
	 * Marks something as done, once and atomically: the first call with a key makes
	 * the mark and returns true, and every other call returns false until the mark
	 * expires. A batch result is marked when it is billed and when it is logged, so
	 * neither happens twice, whichever server reads the results.
	 *
	 * @param key the mark
	 * @param ttl how long the mark is kept
	 * @return whether this call made the mark
	 */
	boolean markOnce(String key, Duration ttl);
}
