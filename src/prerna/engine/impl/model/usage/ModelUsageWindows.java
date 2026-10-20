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

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;

import prerna.auth.access.ModelUsageLimitRule.Window;

/**
 * The time math of usage limit windows, all in UTC.
 *
 * <ul>
 * <li>Day, week (Sunday to Saturday), month and year are fixed calendar
 * windows. A rule's count is the count of the bucket that holds now, and it
 * resets when the bucket ends.</li>
 * <li>Minute and hour slide, using two buckets of the window's length, which
 * can be several minutes or hours, such as every 10 minutes. The count is the
 * current bucket plus the previous bucket weighted by how much of it is still
 * inside the window that ends now. Buckets line up with the epoch, so a
 * 10-minute window's buckets start on the hour and every 10 minutes after.</li>
 * </ul>
 *
 * <p>
 * Every method that takes a length treats it as 1 for a fixed window, and the
 * methods without one are for a length of 1.
 * </p>
 *
 * <p>
 * A bucket is named by its window, length and start ({@link #bucketId}), so
 * every server of a cluster puts a moment in the same counter.
 * {@link #resetsAt} is the {@code resetsAt} a refusal and the response's usage
 * limits carry.
 * </p>
 */
public final class ModelUsageWindows {

	public static final long MINUTE_MILLIS = 60_000L;
	public static final long HOUR_MILLIS = 3_600_000L;
	public static final long DAY_MILLIS = 86_400_000L;

	private ModelUsageWindows() {

	}

	/**
	 * Whether a rule on the window counts over two sliding buckets, rather than
	 * resetting when a calendar bucket ends.
	 *
	 * @param window the window
	 * @return whether the window slides (minute and hour) rather than being a fixed
	 *         calendar window
	 */
	public static boolean isSliding(Window window) {
		return window == Window.MINUTE || window == Window.HOUR;
	}

	/**
	 * The length in milliseconds of a sliding window of length 1, a minute or an
	 * hour. Throws for a fixed window, whose length varies.
	 *
	 * @param window a sliding window
	 * @return its length in milliseconds, for a length of 1
	 */
	public static long length(Window window) {
		return length(window, 1);
	}

	/**
	 * The length in milliseconds of a sliding window of several minutes or hours,
	 * such as 10 minutes; a length under 1 counts as 1. Throws for a fixed window,
	 * whose length varies.
	 *
	 * @param window a sliding window
	 * @param length how many minutes or hours it is
	 * @return its length in milliseconds
	 */
	public static long length(Window window, int length) {
		long count = Math.max(1, length);
		switch (window) {
		case MINUTE:
			return MINUTE_MILLIS * count;
		case HOUR:
			return HOUR_MILLIS * count;
		default:
			throw new IllegalArgumentException("Only minute and hour windows have a fixed length");
		}
	}

	/**
	 * Finds where the bucket that holds a moment starts, for a window of length 1.
	 *
	 * @param window the window
	 * @param at     a moment
	 * @return the start of the bucket that holds the moment
	 */
	public static Instant bucketStart(Window window, Instant at) {
		return bucketStart(window, 1, at);
	}

	/**
	 * Finds where the bucket that holds a moment starts, in UTC: midnight of the
	 * day for a day, of the Sunday on or before it for a week, and of the first day
	 * of the month or year for a month or a year. A sliding window's buckets start
	 * a whole number of bucket lengths after the epoch.
	 *
	 * @param window the window
	 * @param length how many minutes or hours a sliding window is
	 * @param at     a moment
	 * @return the start of the bucket that holds the moment
	 */
	public static Instant bucketStart(Window window, int length, Instant at) {
		LocalDate date = at.atZone(ZoneOffset.UTC).toLocalDate();
		switch (window) {
		case MINUTE:
		case HOUR:
			// a whole number of buckets since the epoch, so buckets line up the same
			// way on every server whatever the moment they are read at
			long bucketMillis = length(window, length);
			return Instant.ofEpochMilli(Math.floorDiv(at.toEpochMilli(), bucketMillis) * bucketMillis);
		case DAY:
			return date.atStartOfDay(ZoneOffset.UTC).toInstant();
		case WEEK:
			// a week runs Sunday to Saturday, UTC
			return date.with(TemporalAdjusters.previousOrSame(DayOfWeek.SUNDAY)).atStartOfDay(ZoneOffset.UTC)
					.toInstant();
		case MONTH:
			return date.withDayOfMonth(1).atStartOfDay(ZoneOffset.UTC).toInstant();
		case YEAR:
			return date.withDayOfYear(1).atStartOfDay(ZoneOffset.UTC).toInstant();
		default:
			throw new IllegalArgumentException("Unknown window " + window);
		}
	}

	/**
	 * Finds where the bucket that holds a moment ends, for a window of length 1.
	 *
	 * @param window the window
	 * @param at     a moment
	 * @return the end of the bucket that holds the moment, which is also the start
	 *         of the next one
	 */
	public static Instant bucketEnd(Window window, Instant at) {
		return bucketEnd(window, 1, at);
	}

	/**
	 * Finds where the bucket that holds a moment ends: one bucket length after its
	 * start for a sliding window, and a day, week, month or year after it for a
	 * fixed one.
	 *
	 * @param window the window
	 * @param length how many minutes or hours a sliding window is
	 * @param at     a moment
	 * @return the end of the bucket that holds the moment, which is also the start
	 *         of the next one
	 */
	public static Instant bucketEnd(Window window, int length, Instant at) {
		Instant start = bucketStart(window, length, at);
		ZonedDateTime utcStart = start.atZone(ZoneOffset.UTC);
		switch (window) {
		case MINUTE:
		case HOUR:
			return start.plusMillis(length(window, length));
		case DAY:
			return utcStart.plusDays(1).toInstant();
		case WEEK:
			return utcStart.plusWeeks(1).toInstant();
		case MONTH:
			return utcStart.plusMonths(1).toInstant();
		case YEAR:
			return utcStart.plusYears(1).toInstant();
		default:
			throw new IllegalArgumentException("Unknown window " + window);
		}
	}

	/**
	 * Finds where the bucket before the current one starts, whose count a sliding
	 * window carries in part, for a window of length 1.
	 *
	 * @param window the window
	 * @param at     a moment
	 * @return the start of the bucket before the one that holds the moment
	 */
	public static Instant previousBucketStart(Window window, Instant at) {
		return previousBucketStart(window, 1, at);
	}

	/**
	 * Finds where the bucket before the current one starts, whose count a sliding
	 * window carries in part into the window that ends at the moment.
	 *
	 * @param window the window
	 * @param length how many minutes or hours a sliding window is
	 * @param at     a moment
	 * @return the start of the bucket before the one that holds the moment
	 */
	public static Instant previousBucketStart(Window window, int length, Instant at) {
		return bucketStart(window, length, bucketStart(window, length, at).minusMillis(1));
	}

	/**
	 * Works out how much a sliding window weighs its previous bucket at a moment,
	 * for a window of length 1.
	 *
	 * @param window the window
	 * @param at     a moment
	 * @return for a sliding window, how much of the previous bucket is still inside
	 *         the window that ends at the moment, from 0 to 1; 0 for a fixed window
	 */
	public static double previousWeight(Window window, Instant at) {
		return previousWeight(window, 1, at);
	}

	/**
	 * Works out how much a sliding window weighs its previous bucket at a moment:
	 * the share of the current bucket's length that is still to come.
	 *
	 * @param window the window
	 * @param length how many minutes or hours a sliding window is
	 * @param at     a moment
	 * @return for a sliding window, how much of the previous bucket is still inside
	 *         the window that ends at the moment, from 0 to 1; 0 for a fixed window
	 */
	public static double previousWeight(Window window, int length, Instant at) {
		if (!isSliding(window)) {
			return 0;
		}
		// the window that ends now started one bucket length ago, so it still holds
		// the part of the previous bucket that the current one has not reached yet
		long bucketMillis = length(window, length);
		long left = bucketEnd(window, length, at).toEpochMilli() - at.toEpochMilli();
		return Math.max(0, Math.min(1, (double) left / bucketMillis));
	}

	/**
	 * Works out how much of the previous bucket's count still counts toward a rule
	 * at a moment, for a window of length 1.
	 *
	 * @param window   the window
	 * @param at       a moment
	 * @param previous the previous bucket's count
	 * @return the share of the previous bucket that still counts, rounded up; 0 for
	 *         a fixed window
	 */
	public static long carried(Window window, Instant at, long previous) {
		return carried(window, 1, at, previous);
	}

	/**
	 * Works out how much of the previous bucket's count still counts toward a rule
	 * at a moment: the count weighed by {@link #previousWeight}, rounded up so the
	 * estimate errs toward refusing.
	 *
	 * @param window   the window
	 * @param length   how many minutes or hours a sliding window is
	 * @param at       a moment
	 * @param previous the previous bucket's count
	 * @return the share of the previous bucket that still counts, rounded up; 0 for
	 *         a fixed window
	 */
	public static long carried(Window window, int length, Instant at, long previous) {
		if (previous <= 0) {
			return 0;
		}
		// rounded up, so the estimate errs toward refusing rather than letting a
		// window go over
		return (long) Math.ceil(previous * previousWeight(window, length, at));
	}

	/**
	 * When a rule counts again from what it can take. For a fixed window that is
	 * the end of the bucket. For a sliding window it is an estimate: with usage
	 * left, the time by which what is counted now has slid out; with nothing left,
	 * the time by which enough of it has slid out to leave usage again.
	 *
	 * @param window   the window
	 * @param at       now
	 * @param previous the previous bucket's count, for a sliding window
	 * @param current  the current bucket's count
	 * @param amount   the rule's amount
	 * @return the moment, to the second
	 */
	public static Instant resetsAt(Window window, Instant at, long previous, long current, long amount) {
		return resetsAt(window, 1, at, previous, current, amount);
	}

	/**
	 * {@link #resetsAt(Window, Instant, long, long, long)} for a sliding window of
	 * several minutes or hours.
	 *
	 * @param window   the window
	 * @param length   how many minutes or hours a sliding window is
	 * @param at       now
	 * @param previous the previous bucket's count, for a sliding window
	 * @param current  the current bucket's count
	 * @param amount   the rule's amount
	 * @return the moment, to the second
	 */
	public static Instant resetsAt(Window window, int length, Instant at, long previous, long current, long amount) {
		Instant end = bucketEnd(window, length, at);
		if (!isSliding(window)) {
			return end;
		}
		long bucketMillis = length(window, length);
		long effective = current + carried(window, length, at, previous);
		Instant result;
		if (effective < amount) {
			// what is counted now is gone once the current bucket has slid out
			result = end.plusMillis(bucketMillis);
		} else if (current >= amount) {
			// the current bucket becomes the previous one, and has to slide out until
			// its weighted share is under the amount
			double fraction = current <= 0 ? 0 : 1 - ((double) amount / current);
			result = end.plusMillis((long) Math.ceil(bucketMillis * Math.max(0, fraction)));
		} else if (previous > 0) {
			// enough of the previous bucket has to slide out during this one
			double millisLeft = (double) (amount - current) * bucketMillis / previous;
			result = end.minusMillis((long) Math.floor(millisLeft));
			if (result.isBefore(at)) {
				result = at;
			}
		} else {
			result = end;
		}
		return ceilToSecond(result);
	}

	/**
	 * Formats a moment, such as a rule's {@code resetsAt}, to the second; a null
	 * moment gives null.
	 *
	 * @param instant a moment
	 * @return the moment as ISO 8601 in UTC, such as {@code 2026-10-13T00:00:00Z}
	 */
	public static String iso(Instant instant) {
		return instant == null ? null : instant.truncatedTo(ChronoUnit.SECONDS).toString();
	}

	/**
	 * Works out when the store can drop a bucket's counter, for a window of length
	 * 1.
	 *
	 * @param window      the window
	 * @param bucketStart the start of a bucket
	 * @return when a counter for the bucket can be dropped: a day after a fixed
	 *         bucket ends, and a window and a minute after a sliding bucket ends,
	 *         since it is then the previous bucket for one more window
	 */
	public static long counterExpiresAt(Window window, Instant bucketStart) {
		return counterExpiresAt(window, 1, bucketStart);
	}

	/**
	 * Works out when the store can drop a bucket's counter, in epoch milliseconds,
	 * keeping it while a rule can still read it.
	 *
	 * @param window      the window
	 * @param length      how many minutes or hours a sliding window is
	 * @param bucketStart the start of a bucket
	 * @return when a counter for the bucket can be dropped: a day after a fixed
	 *         bucket ends, and a window and a minute after a sliding bucket ends,
	 *         since it is then the previous bucket for one more window
	 */
	public static long counterExpiresAt(Window window, int length, Instant bucketStart) {
		Instant end = bucketEnd(window, length, bucketStart);
		if (isSliding(window)) {
			return end.toEpochMilli() + length(window, length) + MINUTE_MILLIS;
		}
		return end.toEpochMilli() + DAY_MILLIS;
	}

	/**
	 * Names a bucket in its counter keys, for a window of length 1.
	 *
	 * @param window      the window
	 * @param bucketStart the start of a bucket
	 * @return the part of a counter key that names the bucket
	 */
	public static String bucketId(Window window, Instant bucketStart) {
		return bucketId(window, 1, bucketStart);
	}

	/**
	 * Names a bucket in its counter keys by its window, its length when a sliding
	 * window is longer than 1, and its start in epoch seconds, so every server of a
	 * cluster names it the same way.
	 *
	 * @param window      the window
	 * @param length      how many minutes or hours a sliding window is
	 * @param bucketStart the start of a bucket
	 * @return the part of a counter key that names the bucket, such as
	 *         {@code MINUTE10|1760313600} for a 10-minute window
	 */
	public static String bucketId(Window window, int length, Instant bucketStart) {
		String name = isSliding(window) && length > 1 ? window.name() + length : window.name();
		return name + "|" + bucketStart.getEpochSecond();
	}

	private static Instant ceilToSecond(Instant instant) {
		Instant truncated = instant.truncatedTo(ChronoUnit.SECONDS);
		return truncated.equals(instant) ? instant : truncated.plusSeconds(1);
	}
}
