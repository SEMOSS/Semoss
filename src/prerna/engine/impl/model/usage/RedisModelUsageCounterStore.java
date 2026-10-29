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

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.redis.RedisConnectionFactory;
import redis.clients.jedis.UnifiedJedis;

/**
 * Usage limit counters in Redis, shared by every node of a cluster.
 *
 * <p>
 * Each counter is a hash: {@code u} is what is used, {@code s} is set once the
 * counter has been filled from the inference logs, and each {@code r<minute>}
 * field holds the reservations that expire in that epoch minute. Lua scripts
 * check and change several counters at once. Every key carries the same hash
 * tag, so they share one slot and a script can touch them together on Redis
 * Cluster too. A counter expires a while after its window bucket ends. A mark
 * is a plain key set with {@code SET NX PX}, so only one server makes it.
 * </p>
 *
 * <p>
 * Redis runs each script whole, with no other command in between, so checking
 * that a call fits and reserving it is atomic across every server of the
 * cluster. Expired {@code r<minute>} fields are not counted, and the reserve
 * script deletes them as it goes, so a call lost with its server stops holding
 * usage once its minute passes.
 * </p>
 */
public class RedisModelUsageCounterStore implements IModelUsageCounterStore {

	private static final Logger classLogger = LogManager.getLogger(RedisModelUsageCounterStore.class);

	/** The hash tag keeps every counter in one slot on Redis Cluster */
	private static final String KEY_PREFIX = "{semoss-model-usage}|";
	private static final String MARK_PREFIX = "mark|";

	/**
	 * Reads each counter: what is used, what unexpired reservations hold, and
	 * whether it is seeded; three numbers for each key, in order. ARGV[1] is the
	 * current epoch minute.
	 */
	private static final String GET_SCRIPT = """
			local now = tonumber(ARGV[1])
			local out = {}
			for i, key in ipairs(KEYS) do
			  local fields = redis.call('HGETALL', key)
			  local used, reserved, seeded = 0, 0, 0
			  for j = 1, #fields, 2 do
			    local f = fields[j]
			    local v = tonumber(fields[j + 1]) or 0
			    if f == 'u' then
			      used = v
			    elseif f == 's' then
			      seeded = 1
			    elseif string.sub(f, 1, 1) == 'r' then
			      local m = tonumber(string.sub(f, 2))
			      if m and m > now then
			        reserved = reserved + v
			      end
			    end
			  end
			  out[#out + 1] = used
			  out[#out + 1] = reserved
			  out[#out + 1] = seeded
			end
			return out
			""";

	/**
	 * Reserves on every counter or on none. The first pass checks each hold: what
	 * is used and reserved, plus the carried share of a sliding window's previous
	 * bucket, plus the amount, must stay within the limit, and a limit of -1 is a
	 * counter the call is only billed on. Only when every hold fits does the second
	 * pass add the amounts to the {@code r<expiry minute>} field and keep the key
	 * for at least as long. ARGV is the current minute, the expiry minute, then
	 * amount, limit, carried and expire-at seconds for each key. Returns 1 when
	 * reserved and 0 when a hold does not fit.
	 */
	private static final String RESERVE_SCRIPT = """
			local now = tonumber(ARGV[1])
			local field = 'r' .. ARGV[2]
			local function total(key)
			  local fields = redis.call('HGETALL', key)
			  local sum = 0
			  for j = 1, #fields, 2 do
			    local f = fields[j]
			    if f == 'u' then
			      sum = sum + (tonumber(fields[j + 1]) or 0)
			    elseif string.sub(f, 1, 1) == 'r' then
			      local m = tonumber(string.sub(f, 2))
			      if m and m > now then
			        sum = sum + (tonumber(fields[j + 1]) or 0)
			      else
			        redis.call('HDEL', key, f)
			      end
			    end
			  end
			  return sum
			end
			for i = 1, #KEYS do
			  local base = 2 + (i - 1) * 4
			  local amount = tonumber(ARGV[base + 1])
			  local limit = tonumber(ARGV[base + 2])
			  local carried = tonumber(ARGV[base + 3])
			  if limit >= 0 and total(KEYS[i]) + carried + amount > limit then
			    return 0
			  end
			end
			for i = 1, #KEYS do
			  local base = 2 + (i - 1) * 4
			  if tonumber(ARGV[base + 1]) > 0 then
			    redis.call('HINCRBY', KEYS[i], field, ARGV[base + 1])
			    redis.call('EXPIREAT', KEYS[i], ARGV[base + 4])
			  end
			end
			return 1
			""";

	/**
	 * Replaces a reservation with what was used, in one step. KEYS are the counters
	 * released, first, then the counters charged. Each release adds a negative
	 * amount to the reservation's field, while the reservation has not expired, and
	 * deletes the field once it reaches 0; each charge adds to {@code u} and moves
	 * the key's expiry. ARGV is the current minute, the expiry minute, how many
	 * counters are released, their negative amounts, then amount and expire-at
	 * seconds for each charge.
	 */
	private static final String SETTLE_SCRIPT = """
			local now = tonumber(ARGV[1])
			local expiry = tonumber(ARGV[2])
			local released = tonumber(ARGV[3])
			local field = 'r' .. ARGV[2]
			for i = 1, released do
			  if expiry > now and redis.call('HEXISTS', KEYS[i], field) == 1 then
			    local left = redis.call('HINCRBY', KEYS[i], field, ARGV[3 + i])
			    if left <= 0 then
			      redis.call('HDEL', KEYS[i], field)
			    end
			  end
			end
			local base = 3 + released
			for i = released + 1, #KEYS do
			  local k = base + (i - released - 1) * 2
			  redis.call('HINCRBY', KEYS[i], 'u', ARGV[k + 1])
			  redis.call('EXPIREAT', KEYS[i], ARGV[k + 2])
			end
			return 1
			""";

	/**
	 * Fills a counter from the logs once: unless {@code s} is set, raises {@code u}
	 * to the logs' amount when that is larger and sets {@code s}, so a second
	 * server's seed of the same counter does nothing.
	 */
	private static final String SEED_SCRIPT = """
			if redis.call('HEXISTS', KEYS[1], 's') == 0 then
			  local used = tonumber(redis.call('HGET', KEYS[1], 'u') or '0') or 0
			  if tonumber(ARGV[1]) > used then
			    redis.call('HSET', KEYS[1], 'u', ARGV[1])
			  end
			  redis.call('HSET', KEYS[1], 's', '1')
			  redis.call('EXPIREAT', KEYS[1], ARGV[2])
			end
			return 1
			""";

	/**
	 * Makes a mark with {@code SET NX PX}: 1 for the one call that made it, 0 for
	 * every other
	 */
	private static final String MARK_SCRIPT = """
			if redis.call('SET', KEYS[1], '1', 'NX', 'PX', ARGV[1]) then
			  return 1
			end
			return 0
			""";

	private final Supplier<UnifiedJedis> client;
	private final Clock clock;

	public RedisModelUsageCounterStore() {
		this(RedisConnectionFactory::getClient, Clock.systemUTC());
	}

	/**
	 * Makes a store that keeps its counters in the shared Redis, with reservations
	 * that expire by the given clock.
	 *
	 * @param client where the shared Redis client comes from
	 * @param clock  the clock reservations expire by
	 */
	public RedisModelUsageCounterStore(Supplier<UnifiedJedis> client, Clock clock) {
		this.client = client;
		this.clock = clock;
	}

	@Override
	public Map<String, ModelUsageCount> get(Collection<String> keys) {
		Map<String, ModelUsageCount> counts = new LinkedHashMap<>();
		if (keys == null || keys.isEmpty()) {
			return counts;
		}
		List<String> ordered = new ArrayList<>(keys);
		List<String> redisKeys = new ArrayList<>();
		for (String key : ordered) {
			redisKeys.add(KEY_PREFIX + key);
		}
		Object result = eval(GET_SCRIPT, redisKeys, List.of(String.valueOf(nowMinute())));
		List<?> values = result instanceof List ? (List<?>) result : List.of();
		for (int i = 0; i < ordered.size(); i++) {
			long used = longAt(values, i * 3);
			long reserved = longAt(values, i * 3 + 1);
			boolean seeded = longAt(values, i * 3 + 2) == 1;
			counts.put(ordered.get(i), new ModelUsageCount(used, reserved, seeded));
		}
		return counts;
	}

	@Override
	public ModelUsageReservation reserve(List<ModelUsageHold> holds, Duration expiry) {
		long nowMinute = nowMinute();
		long expiryMinute = LocalModelUsageCounterStore.expiryMinute(nowMinute, expiry);
		List<String> keys = new ArrayList<>();
		List<String> args = new ArrayList<>();
		args.add(String.valueOf(nowMinute));
		args.add(String.valueOf(expiryMinute));
		Map<String, Long> amounts = new LinkedHashMap<>();
		for (ModelUsageHold hold : holds) {
			keys.add(KEY_PREFIX + hold.key());
			args.add(String.valueOf(Math.max(0, hold.amount())));
			// -1 tells the script the counter is only billed on, with no limit to check
			args.add(String.valueOf(hold.limit() == null ? -1 : hold.limit()));
			args.add(String.valueOf(Math.max(0, hold.carried())));
			args.add(String.valueOf(expireAtSeconds(hold.expiresAtMillis(), expiryMinute)));
			if (hold.amount() > 0) {
				amounts.merge(hold.key(), hold.amount(), Long::sum);
			}
		}
		if (keys.isEmpty()) {
			return new ModelUsageReservation(expiryMinute, amounts);
		}
		Object result = eval(RESERVE_SCRIPT, keys, args);
		if (!(result instanceof Number) || ((Number) result).longValue() != 1) {
			return null;
		}
		return new ModelUsageReservation(expiryMinute, amounts);
	}

	@Override
	public void settle(ModelUsageReservation reservation, Map<String, Long> released, List<ModelUsageCharge> charges) {
		List<String> keys = new ArrayList<>();
		List<String> args = new ArrayList<>();
		args.add(String.valueOf(nowMinute()));
		args.add(String.valueOf(reservation == null ? 0 : reservation.getExpiryMinute()));
		// what is taken off the reservation here is what the script gives back, so
		// the parts of a batch together never release more than was reserved
		Map<String, Long> taken = reservation == null ? Map.of()
				: LocalModelUsageCounterStore.takeFrom(reservation, released);
		args.add(String.valueOf(taken.size()));
		for (Map.Entry<String, Long> entry : taken.entrySet()) {
			keys.add(KEY_PREFIX + entry.getKey());
			args.add(String.valueOf(-entry.getValue()));
		}
		if (charges != null) {
			for (ModelUsageCharge charge : charges) {
				if (charge.amount() <= 0) {
					continue;
				}
				keys.add(KEY_PREFIX + charge.key());
				args.add(String.valueOf(charge.amount()));
				args.add(String.valueOf(Math.floorDiv(charge.expiresAtMillis(), 1000L)));
			}
		}
		if (keys.isEmpty()) {
			return;
		}
		eval(SETTLE_SCRIPT, keys, args);
	}

	@Override
	public void seed(String key, long amount, long expiresAtMillis) {
		eval(SEED_SCRIPT, List.of(KEY_PREFIX + key),
				List.of(String.valueOf(Math.max(0, amount)), String.valueOf(Math.floorDiv(expiresAtMillis, 1000L))));
	}

	@Override
	public boolean markOnce(String key, Duration ttl) {
		long millis = Math.max(1, ttl == null ? 0 : ttl.toMillis());
		Object result = eval(MARK_SCRIPT, List.of(KEY_PREFIX + MARK_PREFIX + key), List.of(String.valueOf(millis)));
		return result instanceof Number && ((Number) result).longValue() == 1;
	}

	/**
	 * Runs a script. Any failure, such as Redis being down, becomes a
	 * {@link ModelUsageStoreException}, on which the guard refuses the calls a rule
	 * applies to as unavailable and lets the others run uncounted.
	 */
	private Object eval(String script, List<String> keys, List<String> args) {
		try {
			return client.get().eval(script, keys, args);
		} catch (RuntimeException e) {
			classLogger.warn("Model usage counters in Redis cannot be reached: " + e.getMessage());
			throw new ModelUsageStoreException("Model usage counters in Redis cannot be reached", e);
		}
	}

	private long nowMinute() {
		return Math.floorDiv(clock.millis(), ModelUsageWindows.MINUTE_MILLIS);
	}

	/**
	 * A counter that holds a reservation lives at least until the reservation
	 * expires.
	 */
	private static long expireAtSeconds(long counterExpiresAtMillis, long expiryMinute) {
		long reservationEndsMillis = (expiryMinute + 1) * ModelUsageWindows.MINUTE_MILLIS;
		return Math.floorDiv(Math.max(counterExpiresAtMillis, reservationEndsMillis), 1000L);
	}

	private static long longAt(List<?> values, int index) {
		if (index >= values.size()) {
			return 0;
		}
		Object value = values.get(index);
		if (value instanceof Number) {
			return ((Number) value).longValue();
		}
		if (value instanceof byte[]) {
			value = new String((byte[]) value, java.nio.charset.StandardCharsets.UTF_8);
		}
		try {
			return value == null ? 0 : Long.parseLong(value.toString());
		} catch (NumberFormatException e) {
			return 0;
		}
	}
}
