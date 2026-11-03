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

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * What a call holds on its counters while it runs. Each counter keeps its
 * reservations in buckets named by the minute they expire in, so a call that is
 * lost, with its node or otherwise, stops holding anything once its reservation
 * expires.
 *
 * <p>
 * What is still held is tracked here too, so releases taken from it, such as
 * the parts of a batch billed as they come in, never give back more than was
 * reserved.
 * </p>
 */
public final class ModelUsageReservation {

	private final long expiryMinute;
	private final Map<String, Long> remaining = new LinkedHashMap<>();

	/**
	 * Makes the reservation of what a call holds on each counter. An amount of 0 or
	 * less is left out.
	 *
	 * @param expiryMinute the epoch minute the reservation expires in; it counts
	 *                     while the current minute is before it
	 * @param amounts      what is held on each counter
	 */
	public ModelUsageReservation(long expiryMinute, Map<String, Long> amounts) {
		this.expiryMinute = expiryMinute;
		if (amounts != null) {
			for (Map.Entry<String, Long> entry : amounts.entrySet()) {
				if (entry.getValue() != null && entry.getValue() > 0) {
					this.remaining.merge(entry.getKey(), entry.getValue(), Long::sum);
				}
			}
		}
	}

	/**
	 * The epoch minute the reservation expires in, which names the bucket each
	 * counter keeps it in.
	 *
	 * @return the epoch minute the reservation expires in
	 */
	public long getExpiryMinute() {
		return expiryMinute;
	}

	/**
	 * Whether the reservation holds nothing on any counter, so there is nothing to
	 * give back.
	 *
	 * @return whether nothing is held any more
	 */
	public synchronized boolean isEmpty() {
		return remaining.isEmpty();
	}

	/**
	 * Takes part of what is held on one counter, for a store to release.
	 *
	 * @param key    the counter
	 * @param amount the most to take
	 * @return what was taken, never more than is held
	 */
	public synchronized long take(String key, long amount) {
		Long held = remaining.get(key);
		if (held == null || amount <= 0) {
			return 0;
		}
		long taken = Math.min(held, amount);
		if (held - taken <= 0) {
			remaining.remove(key);
		} else {
			remaining.put(key, held - taken);
		}
		return taken;
	}

	/**
	 * Takes everything still held, for a store to release.
	 *
	 * @return what was held on each counter
	 */
	public synchronized Map<String, Long> takeAll() {
		Map<String, Long> all = new LinkedHashMap<>(remaining);
		remaining.clear();
		return all;
	}
}
