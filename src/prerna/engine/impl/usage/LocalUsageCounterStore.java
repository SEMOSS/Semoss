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

import java.time.Clock;
import java.time.Duration;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;

/**
 * Usage limit counters in memory, for a single node. Each counter expires a
 * while after its window bucket ends, and the reservations it holds expire in
 * the minute their call's timeout ends. Marks are kept in memory too, so they
 * do not outlive a restart.
 *
 * <p>
 * Every method is synchronized on the store, so checking several counters and
 * reserving on them is one step for every call on the server, as the Lua
 * scripts make it for {@link RedisUsageCounterStore}.
 * </p>
 */
public class LocalUsageCounterStore implements IUsageCounterStore {

	private static final long MAXIMUM_COUNTERS = 500_000L;
	private static final long MAXIMUM_MARKS = 1_000_000L;

	private final Clock clock;
	private final Cache<String, Counter> counters;
	/** Each mark, with when it expires in epoch milliseconds */
	private final Cache<String, Long> marks;

	public LocalUsageCounterStore() {
		this(Clock.systemUTC());
	}

	/**
	 * Makes an empty store whose counters, reservations and marks expire by the
	 * given clock.
	 *
	 * @param clock the clock reservations and counters expire by
	 */
	public LocalUsageCounterStore(Clock clock) {
		this.clock = clock;
		this.counters = Caffeine
				.newBuilder().maximumSize(MAXIMUM_COUNTERS).expireAfter(Expiry.<String, Counter>creating((key,
						counter) -> Duration.ofMillis(Math.max(1, counter.expiresAtMillis - this.clock.millis()))))
				.build();
		this.marks = Caffeine.newBuilder().maximumSize(MAXIMUM_MARKS).expireAfter(Expiry.<String, Long>creating(
				(key, expiresAtMillis) -> Duration.ofMillis(Math.max(1, expiresAtMillis - this.clock.millis()))))
				.build();
	}

	@Override
	public synchronized Map<String, UsageCount> get(Collection<String> keys) {
		long nowMillis = clock.millis();
		long nowMinute = Math.floorDiv(nowMillis, UsageWindows.MINUTE_MILLIS);
		Map<String, UsageCount> counts = new LinkedHashMap<>();
		for (String key : keys) {
			Counter counter = live(key, nowMillis);
			counts.put(key, counter == null ? UsageCount.EMPTY : counter.count(nowMinute));
		}
		return counts;
	}

	@Override
	public synchronized UsageReservation reserve(List<UsageHold> holds, Duration expiry) {
		long nowMillis = clock.millis();
		long nowMinute = Math.floorDiv(nowMillis, UsageWindows.MINUTE_MILLIS);
		// every hold is checked before any is reserved, so it is all or none
		for (UsageHold hold : holds) {
			Counter counter = live(hold.key(), nowMillis);
			if (!hold.fits(counter == null ? UsageCount.EMPTY : counter.count(nowMinute))) {
				return null;
			}
		}
		long expiryMinute = expiryMinute(nowMinute, expiry);
		Map<String, Long> amounts = new LinkedHashMap<>();
		for (UsageHold hold : holds) {
			if (hold.amount() <= 0) {
				continue;
			}
			Counter counter = liveOrCreate(hold.key(), hold.expiresAtMillis(), nowMillis);
			counter.reserved.merge(expiryMinute, hold.amount(), Long::sum);
			amounts.merge(hold.key(), hold.amount(), Long::sum);
		}
		return new UsageReservation(expiryMinute, amounts);
	}

	@Override
	public synchronized boolean consume(List<UsageHold> holds) {
		UsageHold.requireUniqueKeys(holds);
		long now = clock.millis();
		long minute = Math.floorDiv(now, UsageWindows.MINUTE_MILLIS);
		for (var hold : holds) {
			Counter counter = live(hold.key(), now);
			if (!hold.fits(counter == null ? UsageCount.EMPTY : counter.count(minute)))
				return false;
		}
		for (var hold : holds)
			if (hold.amount() > 0)
				liveOrCreate(hold.key(), hold.expiresAtMillis(), now).used += hold.amount();
		return true;
	}

	@Override
	public synchronized void settle(UsageReservation reservation, Map<String, Long> released,
			List<UsageCharge> charges) {
		long nowMillis = clock.millis();
		long nowMinute = Math.floorDiv(nowMillis, UsageWindows.MINUTE_MILLIS);
		if (reservation != null) {
			Map<String, Long> taken = takeFrom(reservation, released);
			for (Map.Entry<String, Long> entry : taken.entrySet()) {
				Counter counter = live(entry.getKey(), nowMillis);
				// a reservation that has expired no longer counts, so there is nothing
				// to take back from it
				if (counter == null || reservation.getExpiryMinute() <= nowMinute) {
					continue;
				}
				Long held = counter.reserved.get(reservation.getExpiryMinute());
				if (held == null) {
					continue;
				}
				long left = held - entry.getValue();
				if (left > 0) {
					counter.reserved.put(reservation.getExpiryMinute(), left);
				} else {
					counter.reserved.remove(reservation.getExpiryMinute());
				}
			}
		}
		if (charges != null) {
			for (UsageCharge charge : charges) {
				if (charge.amount() <= 0) {
					continue;
				}
				liveOrCreate(charge.key(), charge.expiresAtMillis(), nowMillis).used += charge.amount();
			}
		}
	}

	@Override
	public synchronized void seed(String key, long amount, long expiresAtMillis) {
		Counter counter = liveOrCreate(key, expiresAtMillis, clock.millis());
		if (!counter.seeded) {
			counter.used = Math.max(counter.used, amount);
			counter.seeded = true;
		}
	}

	@Override
	public synchronized boolean markOnce(String key, Duration ttl) {
		long nowMillis = clock.millis();
		Long expiresAtMillis = marks.getIfPresent(key);
		if (expiresAtMillis != null && expiresAtMillis > nowMillis) {
			return false;
		}
		marks.put(key, nowMillis + Math.max(1, ttl == null ? 0 : ttl.toMillis()));
		return true;
	}

	/**
	 * Takes what a settle gives back out of a reservation, so no more is given back
	 * than the reservation still holds. Both counter stores use it.
	 *
	 * @param reservation the reservation
	 * @param released    what to take from each counter, or null for everything
	 * @return what was taken from each counter
	 */
	public static Map<String, Long> takeFrom(UsageReservation reservation, Map<String, Long> released) {
		if (released == null) {
			return reservation.takeAll();
		}
		Map<String, Long> taken = new LinkedHashMap<>();
		for (Map.Entry<String, Long> entry : released.entrySet()) {
			long amount = reservation.take(entry.getKey(), entry.getValue() == null ? 0 : entry.getValue());
			if (amount > 0) {
				taken.put(entry.getKey(), amount);
			}
		}
		return taken;
	}

	/**
	 * Works out the epoch minute a reservation made now expires in, so it holds for
	 * at least its expiry. Both counter stores use it.
	 *
	 * @param nowMinute the current epoch minute
	 * @param expiry    how long a reservation holds
	 * @return the epoch minute a reservation made now expires in
	 */
	public static long expiryMinute(long nowMinute, Duration expiry) {
		long minutes = expiry == null ? 1
				: Math.max(1, (expiry.toMillis() + UsageWindows.MINUTE_MILLIS - 1) / UsageWindows.MINUTE_MILLIS);
		// the expiry rounded up to whole minutes, and one more since the current
		// minute is partly gone, so a reservation holds for at least its expiry
		return nowMinute + minutes + 1;
	}

	/**
	 * Reads a counter, dropping it once it has expired by the store's clock.
	 *
	 * @return the counter, or null when it has expired by the store's clock, which
	 *         the cache's own expiry does not follow
	 */
	private Counter live(String key, long nowMillis) {
		Counter counter = counters.getIfPresent(key);
		if (counter != null && counter.expiresAtMillis <= nowMillis) {
			counters.invalidate(key);
			return null;
		}
		return counter;
	}

	private Counter liveOrCreate(String key, long expiresAtMillis, long nowMillis) {
		Counter counter = live(key, nowMillis);
		if (counter == null) {
			counter = new Counter(expiresAtMillis);
			counters.put(key, counter);
		}
		return counter;
	}

	/** One counter */
	private static final class Counter {
		private final long expiresAtMillis;
		private long used;
		private boolean seeded;
		/** what is reserved, by the epoch minute it expires in */
		private final TreeMap<Long, Long> reserved = new TreeMap<>();

		private Counter(long expiresAtMillis) {
			this.expiresAtMillis = expiresAtMillis;
		}

		/**
		 * Reads what the counter has used and what its reservations hold now.
		 *
		 * @return what the counter holds, dropping the reservations that have expired,
		 *         since their calls are gone or never settled
		 */
		private UsageCount count(long nowMinute) {
			long held = 0;
			Iterator<Map.Entry<Long, Long>> entries = reserved.entrySet().iterator();
			while (entries.hasNext()) {
				Map.Entry<Long, Long> entry = entries.next();
				if (entry.getKey() <= nowMinute) {
					entries.remove();
				} else {
					held += entry.getValue();
				}
			}
			return new UsageCount(used, held, seeded);
		}
	}
}
