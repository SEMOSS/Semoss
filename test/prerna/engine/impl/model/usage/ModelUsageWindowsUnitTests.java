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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;

import org.junit.jupiter.api.Test;

import prerna.auth.access.ModelUsageLimitRule.Window;

public class ModelUsageWindowsUnitTests {

	private static final Instant MONDAY_AFTERNOON = Instant.parse("2026-10-12T14:30:15Z");

	@Test
	void dayAndLongerAreFixedUtcCalendarWindows() {
		assertEquals(Instant.parse("2026-10-12T00:00:00Z"),
				ModelUsageWindows.bucketStart(Window.DAY, MONDAY_AFTERNOON));
		assertEquals(Instant.parse("2026-10-13T00:00:00Z"), ModelUsageWindows.bucketEnd(Window.DAY, MONDAY_AFTERNOON));
		assertEquals(Instant.parse("2026-10-01T00:00:00Z"),
				ModelUsageWindows.bucketStart(Window.MONTH, MONDAY_AFTERNOON));
		assertEquals(Instant.parse("2026-11-01T00:00:00Z"),
				ModelUsageWindows.bucketEnd(Window.MONTH, MONDAY_AFTERNOON));
		assertEquals(Instant.parse("2026-01-01T00:00:00Z"),
				ModelUsageWindows.bucketStart(Window.YEAR, MONDAY_AFTERNOON));
		assertEquals(Instant.parse("2027-01-01T00:00:00Z"), ModelUsageWindows.bucketEnd(Window.YEAR, MONDAY_AFTERNOON));
		assertFalse(ModelUsageWindows.isSliding(Window.DAY));
		assertEquals(0, ModelUsageWindows.previousWeight(Window.DAY, MONDAY_AFTERNOON));
	}

	@Test
	void weeksRunSundayToSaturday() {
		assertEquals(Instant.parse("2026-10-11T00:00:00Z"),
				ModelUsageWindows.bucketStart(Window.WEEK, MONDAY_AFTERNOON));
		assertEquals(Instant.parse("2026-10-18T00:00:00Z"), ModelUsageWindows.bucketEnd(Window.WEEK, MONDAY_AFTERNOON));
		Instant sunday = Instant.parse("2026-10-11T00:00:00Z");
		assertEquals(sunday, ModelUsageWindows.bucketStart(Window.WEEK, sunday));
		Instant saturdayNight = Instant.parse("2026-10-17T23:59:59Z");
		assertEquals(sunday, ModelUsageWindows.bucketStart(Window.WEEK, saturdayNight));
	}

	@Test
	void minuteAndHourSlideWithTwoBuckets() {
		assertTrue(ModelUsageWindows.isSliding(Window.MINUTE));
		assertEquals(Instant.parse("2026-10-12T14:30:00Z"),
				ModelUsageWindows.bucketStart(Window.MINUTE, MONDAY_AFTERNOON));
		assertEquals(Instant.parse("2026-10-12T14:29:00Z"),
				ModelUsageWindows.previousBucketStart(Window.MINUTE, MONDAY_AFTERNOON));
		// 15 seconds into the minute, three quarters of the previous minute is still
		// inside the window that ends now
		assertEquals(0.75, ModelUsageWindows.previousWeight(Window.MINUTE, MONDAY_AFTERNOON), 1e-9);
		assertEquals(75, ModelUsageWindows.carried(Window.MINUTE, MONDAY_AFTERNOON, 100));
		assertEquals(76, ModelUsageWindows.carried(Window.MINUTE, MONDAY_AFTERNOON, 101));
		assertEquals(0, ModelUsageWindows.carried(Window.MINUTE, MONDAY_AFTERNOON, 0));

		assertEquals(Instant.parse("2026-10-12T14:00:00Z"),
				ModelUsageWindows.bucketStart(Window.HOUR, MONDAY_AFTERNOON));
		double hourWeight = (double) (29 * 60 + 45) / 3600;
		assertEquals(hourWeight, ModelUsageWindows.previousWeight(Window.HOUR, MONDAY_AFTERNOON), 1e-9);
	}

	@Test
	void minuteAndHourWindowsCanBeSeveralMinutesOrHoursLong() {
		// a 10-minute window's buckets start on the hour and every 10 minutes after
		Instant bucket = Instant.parse("2026-10-12T14:30:00Z");
		assertEquals(bucket, ModelUsageWindows.bucketStart(Window.MINUTE, 10, MONDAY_AFTERNOON));
		assertEquals(Instant.parse("2026-10-12T14:40:00Z"),
				ModelUsageWindows.bucketEnd(Window.MINUTE, 10, MONDAY_AFTERNOON));
		assertEquals(Instant.parse("2026-10-12T14:20:00Z"),
				ModelUsageWindows.previousBucketStart(Window.MINUTE, 10, MONDAY_AFTERNOON));
		assertEquals(600_000L, ModelUsageWindows.length(Window.MINUTE, 10));
		// 15 seconds into the 10 minutes, all but 15 seconds of the previous 10 still
		// counts
		assertEquals((600.0 - 15) / 600, ModelUsageWindows.previousWeight(Window.MINUTE, 10, MONDAY_AFTERNOON), 1e-9);
		assertEquals(Instant.parse("2026-10-12T14:40:00Z"),
				ModelUsageWindows.resetsAt(Window.MINUTE, 10, MONDAY_AFTERNOON, 0, 1000, 1000));

		// a counter of a longer window has its own key, and a window of one keeps the
		// plain name
		assertEquals("MINUTE10|" + bucket.getEpochSecond(), ModelUsageWindows.bucketId(Window.MINUTE, 10, bucket));
		assertEquals("MINUTE|" + bucket.getEpochSecond(), ModelUsageWindows.bucketId(Window.MINUTE, 1, bucket));

		// a 6-hour window's buckets start at midnight, 6, noon and 18 UTC
		assertEquals(Instant.parse("2026-10-12T12:00:00Z"),
				ModelUsageWindows.bucketStart(Window.HOUR, 6, MONDAY_AFTERNOON));
		assertEquals(Instant.parse("2026-10-12T18:00:00Z"),
				ModelUsageWindows.bucketEnd(Window.HOUR, 6, MONDAY_AFTERNOON));

		// a fixed window ignores a length
		assertEquals(ModelUsageWindows.bucketStart(Window.DAY, MONDAY_AFTERNOON),
				ModelUsageWindows.bucketStart(Window.DAY, 3, MONDAY_AFTERNOON));
		assertEquals(ModelUsageWindows.bucketEnd(Window.DAY, MONDAY_AFTERNOON),
				ModelUsageWindows.bucketEnd(Window.DAY, 3, MONDAY_AFTERNOON));
	}

	@Test
	void fixedWindowsResetWhenTheirBucketEnds() {
		assertEquals(Instant.parse("2026-10-13T00:00:00Z"),
				ModelUsageWindows.resetsAt(Window.DAY, MONDAY_AFTERNOON, 0, 5000, 1000));
		assertEquals("2026-10-13T00:00:00Z",
				ModelUsageWindows.iso(ModelUsageWindows.resetsAt(Window.DAY, MONDAY_AFTERNOON, 0, 5000, 1000)));
	}

	@Test
	void slidingWindowsResetWhenEnoughHasSlidOut() {
		// the current minute alone is over: half of it has to slide out of the next one
		assertEquals(Instant.parse("2026-10-12T14:31:30Z"),
				ModelUsageWindows.resetsAt(Window.MINUTE, MONDAY_AFTERNOON, 0, 2000, 1000));
		// the previous minute's share puts it over: there is usage left again once 3/8
		// of
		// the previous minute is left in the window, 22.5 seconds before this minute
		// ends
		assertEquals(Instant.parse("2026-10-12T14:30:38Z"),
				ModelUsageWindows.resetsAt(Window.MINUTE, MONDAY_AFTERNOON, 800, 400, 700));
		// with usage left, what is counted now is gone once the current minute has slid
		// out
		assertEquals(Instant.parse("2026-10-12T14:32:00Z"),
				ModelUsageWindows.resetsAt(Window.MINUTE, MONDAY_AFTERNOON, 100, 100, 1000));
	}

	@Test
	void countersOutliveTheirWindow() {
		Instant day = Instant.parse("2026-10-12T00:00:00Z");
		assertEquals(Instant.parse("2026-10-14T00:00:00Z").toEpochMilli(),
				ModelUsageWindows.counterExpiresAt(Window.DAY, day));
		Instant minute = Instant.parse("2026-10-12T14:30:00Z");
		// a minute bucket is the previous bucket for the minute after it
		assertEquals(Instant.parse("2026-10-12T14:33:00Z").toEpochMilli(),
				ModelUsageWindows.counterExpiresAt(Window.MINUTE, minute));
		assertEquals("DAY|" + day.getEpochSecond(), ModelUsageWindows.bucketId(Window.DAY, day));
	}
}
