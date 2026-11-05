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
import java.util.Arrays;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import prerna.auth.User;
import prerna.auth.utils.EngineUsagePolicyResolver;
import prerna.auth.utils.SecurityEngineUsageLimitUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.redis.RedisConnectionConfig;

/**
 * Shared admission, plan caching and store selection for new catalog adapters.
 */
public final class EngineUsageGuard {
	private static final Cache<String, Cached> PLANS = Caffeine.newBuilder().maximumSize(50_000)
			.expireAfterWrite(Duration.ofSeconds(30)).build();

	private record Cached(long version, UsagePlan plan) {
	}

	private static volatile UsageLimiter limiter;

	private EngineUsageGuard() {
	}

	/**
	 * Returns null for system calls or an execution with no applicable counter
	 * rules.
	 */
	public static UsageLimiter.Ticket admit(User user, IEngine engine, UsageRule.Metric admissionMetric,
			long admissionAmount) {
		if (user == null || engine == null || engine.getEngineId() == null || engine.getEngineId().isBlank())
			return null;
		if (admissionMetric == null || admissionAmount <= 0)
			throw new IllegalArgumentException("Admission needs a metric and positive known usage");
		try {
			var primary = user.isAnonymous() ? null : User.getPrimaryUserIdAndTypePair(user);
			String key = UsageLimitKey.digest(Arrays.asList(engine.getCatalogType(), engine.getEngineId(), primary));
			long version = SecurityEngineUsageLimitUtils.version();
			Cached cached = PLANS.getIfPresent(key);
			UsagePlan plan;
			if (cached == null || cached.version != version) {
				plan = EngineUsagePolicyResolver.resolve(user, engine.getCatalogType(), engine.getEngineId());
				PLANS.put(key, new Cached(version, plan));
			} else
				plan = cached.plan;
			if (plan.ceilings().isEmpty() && plan.tracking().isEmpty()
					&& (plan.candidates().isEmpty() || plan.candidates().get(0).targets().isEmpty()))
				return null;
			for (int attempt = 0;; attempt++) {
				UsageLimiter.Ticket ticket = limiter().admit(plan);
				try {
					ticket.consume(admissionMetric, admissionAmount);
					return ticket;
				} catch (RuntimeException e) {
					try {
						ticket.close();
					} catch (RuntimeException close) {
						e.addSuppressed(close);
						throw e;
					}
					if (e instanceof UsageLimitException refusal && !refusal.isUnavailable() && attempt < 2)
						continue;
					throw e;
				}
			}
		} catch (UsageStoreException e) {
			throw UsageLimitException.unavailable(e);
		}
	}

	private static UsageLimiter limiter() {
		UsageLimiter current = limiter;
		if (current == null)
			synchronized (EngineUsageGuard.class) {
				current = limiter;
				if (current == null) {
					// Unlike the legacy model fallback, new cluster limits fail closed without
					// Redis.
					if (ClusterUtil.IS_CLUSTER && RedisConnectionConfig.fromDIHelper() == null)
						throw UsageLimitException.unavailable(
								new IllegalStateException("Shared usage counters require Redis on a cluster"));
					current = new UsageLimiter(
							ClusterUtil.IS_CLUSTER ? new RedisUsageCounterStore() : new LocalUsageCounterStore(),
							Clock.systemUTC(), SecurityEngineUsageLimitUtils.history());
					limiter = current;
				}
			}
		return current;
	}
}
