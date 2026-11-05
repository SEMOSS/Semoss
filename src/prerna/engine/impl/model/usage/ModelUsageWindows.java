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

import java.time.Instant;
import prerna.auth.access.ModelUsageLimitRule.Window;
import prerna.engine.impl.usage.UsageWindows;

/** Model API facade for the shared UTC window calculations. */
public final class ModelUsageWindows {
	public static final long MINUTE_MILLIS = UsageWindows.MINUTE_MILLIS;
	public static final long HOUR_MILLIS = UsageWindows.HOUR_MILLIS;
	public static final long DAY_MILLIS = UsageWindows.DAY_MILLIS;

	private ModelUsageWindows() {
	}

	private static UsageWindows.Window windowOf(Window window) {
		return window == null ? null : UsageWindows.Window.valueOf(window.name());
	}

	public static boolean isSliding(Window window) {
		return UsageWindows.isSliding(windowOf(window));
	}

	public static long length(Window window) {
		return UsageWindows.length(windowOf(window));
	}

	public static long length(Window window, int length) {
		return UsageWindows.length(windowOf(window), length);
	}

	public static Instant bucketStart(Window window, Instant at) {
		return UsageWindows.bucketStart(windowOf(window), at);
	}

	public static Instant bucketStart(Window window, int length, Instant at) {
		return UsageWindows.bucketStart(windowOf(window), length, at);
	}

	public static Instant bucketEnd(Window window, Instant at) {
		return UsageWindows.bucketEnd(windowOf(window), at);
	}

	public static Instant bucketEnd(Window window, int length, Instant at) {
		return UsageWindows.bucketEnd(windowOf(window), length, at);
	}

	public static Instant previousBucketStart(Window window, Instant at) {
		return UsageWindows.previousBucketStart(windowOf(window), at);
	}

	public static Instant previousBucketStart(Window window, int length, Instant at) {
		return UsageWindows.previousBucketStart(windowOf(window), length, at);
	}

	public static double previousWeight(Window window, Instant at) {
		return UsageWindows.previousWeight(windowOf(window), at);
	}

	public static double previousWeight(Window window, int length, Instant at) {
		return UsageWindows.previousWeight(windowOf(window), length, at);
	}

	public static long carried(Window window, Instant at, long previous) {
		return UsageWindows.carried(windowOf(window), at, previous);
	}

	public static long carried(Window window, int length, Instant at, long previous) {
		return UsageWindows.carried(windowOf(window), length, at, previous);
	}

	public static Instant resetsAt(Window window, Instant at, long previous, long current, long amount) {
		return UsageWindows.resetsAt(windowOf(window), at, previous, current, amount);
	}

	public static Instant resetsAt(Window window, int length, Instant at, long previous, long current, long amount) {
		return UsageWindows.resetsAt(windowOf(window), length, at, previous, current, amount);
	}

	public static String iso(Instant instant) {
		return UsageWindows.iso(instant);
	}

	public static long counterExpiresAt(Window window, Instant bucketStart) {
		return UsageWindows.counterExpiresAt(windowOf(window), bucketStart);
	}

	public static long counterExpiresAt(Window window, int length, Instant bucketStart) {
		return UsageWindows.counterExpiresAt(windowOf(window), length, bucketStart);
	}

	public static String bucketId(Window window, Instant bucketStart) {
		return UsageWindows.bucketId(windowOf(window), bucketStart);
	}

	public static String bucketId(Window window, int length, Instant bucketStart) {
		return UsageWindows.bucketId(windowOf(window), length, bucketStart);
	}
}
