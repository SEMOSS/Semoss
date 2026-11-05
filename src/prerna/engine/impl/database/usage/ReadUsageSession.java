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
package prerna.engine.impl.database.usage;

import java.util.concurrent.locks.ReentrantLock;

import prerna.engine.impl.usage.UsageLimiter;
import prerna.engine.impl.usage.UsageRule.Metric;

/**
 * Counts successful cursor advances; elapsed time excludes caller idle time.
 */
final class ReadUsageSession implements AutoCloseable {
	private final UsageLimiter.Ticket ticket;
	private final ReentrantLock lock = new ReentrantLock();
	private long elapsedNanos;
	private boolean closed;

	ReadUsageSession(UsageLimiter.Ticket ticket) {
		this.ticket = ticket;
	}

	void elapsed(long nanos) {
		lock.lock();
		try {
			if (!closed)
				elapsedNanos += Math.max(0, nanos);
		} finally {
			lock.unlock();
		}
	}

	void row() {
		lock.lock();
		try {
			if (closed)
				throw new IllegalStateException("Read usage session is closed");
			if (ticket != null)
				ticket.consume(Metric.ROWS, 1);
		} finally {
			lock.unlock();
		}
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
		if (ticket == null)
			return;
		RuntimeException failure = null;
		try {
			ticket.record(Metric.COMPUTE_TIME, (elapsedNanos + 999_999L) / 1_000_000L);
		} catch (RuntimeException e) {
			failure = e;
		}
		try {
			ticket.close();
		} catch (RuntimeException e) {
			if (failure == null)
				failure = e;
			else
				failure.addSuppressed(e);
		}
		if (failure != null)
			throw failure;
	}
}
