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
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import prerna.engine.impl.usage.UsagePlan.Target;
import prerna.engine.impl.usage.UsageRule.Metric;

/**
 * Shared admission and incremental accounting for engines with measurable work.
 */
public final class UsageLimiter {
	public interface History {
		long used(String counterKey);

		void record(Map<String, UsageCharge> charges);
	}

	private final IUsageCounterStore store;
	private final Clock clock;
	private final History history;

	public UsageLimiter(IUsageCounterStore store, Clock clock, History history) {
		this.store = store;
		this.clock = clock;
		this.history = history;
	}

	public Ticket admit(UsagePlan plan) {
		try {
			check(plan.ceilings());
			UsagePlan.Candidate selected = null;
			UsageLimitException last = null;
			for (var candidate : plan.candidates()) {
				try {
					check(candidate.targets());
					selected = candidate;
					break;
				} catch (UsageLimitException refused) {
					if (refused.isUnavailable())
						throw refused;
					last = refused;
				}
			}
			if (selected == null && last != null)
				throw last;
			List<Target> applied = new ArrayList<>(plan.ceilings());
			if (selected != null)
				applied.addAll(selected.targets());
			Map<String, Target> enforced = unique(applied);
			Map<String, Target> charged = new LinkedHashMap<>(enforced);
			charged.putAll(unique(plan.tracking()));
			return new Ticket(List.copyOf(enforced.values()), List.copyOf(charged.values()),
					selected == null ? null : selected.key());
		} catch (UsageStoreException e) {
			throw UsageLimitException.unavailable(e);
		}
	}

	private static Map<String, Target> unique(List<Target> targets) {
		Map<String, Target> result = new LinkedHashMap<>();
		for (Target t : targets) {
			String key = t.scope() + "|" + t.rule().metric() + "|" + t.rule().window() + "|" + t.rule().length();
			Target old = result.get(key);
			if (old == null || t.rule().amount() < old.rule().amount())
				result.put(key, t);
		}
		return result;
	}

	private void check(List<Target> targets) {
		for (Target target : targets) {
			Reading read = read(target, clock.instant());
			if (read.current.total() >= target.rule().amount()
					|| read.carried >= target.rule().amount() - read.current.total())
				throw refusal(target, read);
		}
	}

	private record Reading(String key, UsageCount current, long previous, long carried, Instant at, long expires) {
	}

	private Reading read(Target target, Instant at) {
		var rule = target.rule();
		Instant bucket = UsageWindows.bucketStart(rule.window(), rule.length(), at);
		String key = key(target, bucket);
		String previousKey = key(target, UsageWindows.previousBucketStart(rule.window(), rule.length(), at));
		List<String> keys = UsageWindows.isSliding(rule.window()) ? List.of(key, previousKey) : List.of(key);
		Map<String, UsageCount> values = store.get(keys);
		boolean seeded = false;
		for (String id : keys) {
			if (!values.getOrDefault(id, UsageCount.EMPTY).seeded()) {
				Instant start = id.equals(key) ? bucket
						: UsageWindows.previousBucketStart(rule.window(), rule.length(), at);
				store.seed(id, history.used(id), UsageWindows.counterExpiresAt(rule.window(), rule.length(), start));
				seeded = true;
			}
		}
		if (seeded)
			values = store.get(keys);
		long previous = values.getOrDefault(previousKey, UsageCount.EMPTY).total();
		return new Reading(key, values.getOrDefault(key, UsageCount.EMPTY), previous,
				UsageWindows.carried(rule.window(), rule.length(), at, previous), at,
				UsageWindows.counterExpiresAt(rule.window(), rule.length(), bucket));
	}

	private static String key(Target target, Instant start) {
		var r = target.rule();
		return "eul|" + UsageLimitKey
				.digest(List.of(target.scope(), r.metric(), r.window(), r.length(), start.toEpochMilli()));
	}

	private static UsageLimitException refusal(Target target, Reading read) {
		var r = target.rule();
		return new UsageLimitException(r.metric(), UsageWindows.resetsAt(r.window(), r.length(), read.at, read.previous,
				read.current.total(), r.amount()));
	}

	/**
	 * One execution; incremental consumption is atomic across all its relevant
	 * counters.
	 */
	public final class Ticket implements AutoCloseable {
		private final List<Target> enforced;
		private final List<Target> charged;
		private final UsageLimitKey billedTo;
		private final Map<String, UsageCharge> pending = new LinkedHashMap<>();
		private final ReentrantLock lock = new ReentrantLock();
		private boolean closed;

		private Ticket(List<Target> enforced, List<Target> charged, UsageLimitKey billedTo) {
			this.enforced = enforced;
			this.charged = charged;
			this.billedTo = billedTo;
		}

		public UsageLimitKey billedTo() {
			return billedTo;
		}

		public void consume(Metric metric, long amount) {
			lock.lock();
			try {
				consumeLocked(metric, amount);
			} finally {
				lock.unlock();
			}
		}

		private void consumeLocked(Metric metric, long amount) {
			if (closed)
				throw new IllegalStateException("Usage ticket is closed");
			if (amount <= 0)
				return;
			try {
				for (int attempt = 0; attempt < 3; attempt++) {
					Instant at = clock.instant();
					var limits = unique(enforced);
					List<UsageHold> holds = new ArrayList<>();
					UsageLimitException refusal = null;
					for (Target target : charged) {
						if (target.rule().metric() != metric)
							continue;
						Reading reading = read(target, at);
						Target bound = limits.get(target.scope() + "|" + metric + "|" + target.rule().window() + "|"
								+ target.rule().length());
						Long limit = bound == null ? null : bound.rule().amount();
						if (limit != null
								&& !new UsageHold(reading.key, amount, limit, reading.carried, reading.expires)
										.fits(reading.current))
							refusal = refusal(bound, reading);
						holds.add(new UsageHold(reading.key, amount, limit, reading.carried, reading.expires));
					}
					if (refusal != null)
						throw refusal;
					if (holds.isEmpty())
						return;
					if (!store.consume(holds))
						continue;
					List<UsageCharge> charges = holds.stream()
							.map(h -> new UsageCharge(h.key(), h.amount(), h.expiresAtMillis())).toList();
					remember(charges);
					return;
				}
				// Report a newly exhausted rule as a quota refusal, allowing an adapter
				// to reconsider another billing source after a reservation race.
				check(enforced);
				throw UsageLimitException.unavailable(new IllegalStateException("Concurrent usage changed repeatedly"));
			} catch (UsageStoreException e) {
				throw UsageLimitException.unavailable(e);
			}
		}

		/** For a metric known only after work, such as elapsed execution time. */
		public void record(Metric metric, long amount) {
			lock.lock();
			try {
				recordLocked(metric, amount);
			} finally {
				lock.unlock();
			}
		}

		private void recordLocked(Metric metric, long amount) {
			if (closed || amount <= 0)
				return;
			try {
				List<UsageCharge> charges = new ArrayList<>();
				Instant at = clock.instant();
				for (Target target : charged)
					if (target.rule().metric() == metric) {
						Reading read = read(target, at);
						charges.add(new UsageCharge(read.key, amount, read.expires));
					}
				store.settle(null, null, charges);
				remember(charges);
			} catch (UsageStoreException e) {
				throw UsageLimitException.unavailable(e);
			}
		}

		private void remember(List<UsageCharge> charges) {
			for (UsageCharge charge : charges)
				pending.merge(charge.key(), charge, (a, b) -> new UsageCharge(a.key(),
						Math.addExact(a.amount(), b.amount()), Math.max(a.expiresAtMillis(), b.expiresAtMillis())));
		}

		@Override
		public void close() {
			lock.lock();
			try {
				closeLocked();
			} finally {
				lock.unlock();
			}
		}

		private void closeLocked() {
			if (closed)
				return;
			closed = true;
			if (!pending.isEmpty())
				history.record(Map.copyOf(pending));
		}
	}
}
