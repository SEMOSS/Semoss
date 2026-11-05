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
package prerna.engine.impl.usage;

import java.util.HashSet;
import java.util.List;

/**
 * One counter's part of a reservation.
 *
 * @param key             the counter
 * @param amount          what to reserve on it
 * @param limit           the rule's amount the counter must stay within, or
 *                        null when the counter is only billed and not checked
 * @param carried         what else counts toward the limit, such as the share
 *                        of a sliding window's previous bucket
 * @param expiresAtMillis when the counter can be dropped, in epoch milliseconds
 */
public record UsageHold(String key, long amount, Long limit, long carried, long expiresAtMillis) {

	static void requireUniqueKeys(List<UsageHold> holds) {
		var keys = new HashSet<String>();
		for (var hold : holds) {
			if (hold.key() == null || hold.key().isBlank() || hold.amount() < 0 || hold.carried() < 0
					|| (hold.limit() != null && hold.limit() < 0) || !keys.add(hold.key()))
				throw new IllegalArgumentException(
						"Usage consumption needs valid non-negative quantities and distinct counter keys");
		}
	}

	/**
	 * The check {@link LocalUsageCounterStore} makes; the reserve script of
	 * {@link RedisUsageCounterStore} makes the same one in Redis.
	 *
	 * @param count what the counter holds now
	 * @return whether reserving the amount keeps the counter within its limit
	 */
	public boolean fits(UsageCount count) {
		if (limit == null) {
			return true;
		}
		long total = count == null ? 0 : count.total();
		return total <= limit && carried <= limit - total && amount <= limit - total - carried;
	}
}
