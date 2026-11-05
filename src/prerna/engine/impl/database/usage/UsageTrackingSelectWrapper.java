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

import java.io.IOException;
import java.util.Objects;

import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdbms.MultiRDBMSNativeEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.om.ThreadStore;

/**
 * Non-JDBC read lifecycle. Deferred wrappers acquire no ticket until execution.
 */
public final class UsageTrackingSelectWrapper implements IRawSelectWrapper {
	@FunctionalInterface
	public interface Factory {
		IRawSelectWrapper execute() throws Exception;
	}

	private final IDatabaseEngine engine;
	private final Factory factory;
	private IRawSelectWrapper delegate;
	private ReadUsageSession session;
	private boolean closed;
	private boolean limited;

	private UsageTrackingSelectWrapper(IDatabaseEngine engine, Factory factory) {
		this.engine = engine;
		this.factory = factory;
	}

	public static IRawSelectWrapper create(IDatabaseEngine engine, Factory factory, boolean deferred) throws Exception {
		if (engine instanceof RDBMSNativeEngine || engine instanceof MultiRDBMSNativeEngine
				|| !DatabaseUsageGuard.external(engine) || ThreadStore.getUser() == null)
			return factory.execute();
		var wrapper = new UsageTrackingSelectWrapper(engine, factory);
		if (!deferred) {
			wrapper.execute();
			// No policy means the existing wrapper and bulk-flush behavior stay unchanged.
			if (!wrapper.limited)
				return wrapper.delegate;
		}
		return wrapper;
	}

	@Override
	public void execute() throws Exception {
		close();
		closed = false;
		var ticket = DatabaseUsageGuard.admit(engine);
		limited = ticket != null;
		session = new ReadUsageSession(ticket);
		long start = System.nanoTime();
		try {
			delegate = factory.execute();
		} catch (Exception | Error e) {
			session.elapsed(System.nanoTime() - start);
			try {
				close();
			} catch (Exception close) {
				e.addSuppressed(close);
			}
			throw e;
		} finally {
			session.elapsed(System.nanoTime() - start);
		}
	}

	private IRawSelectWrapper ready() {
		if (closed)
			throw new IllegalStateException("Read wrapper is closed");
		if (delegate == null)
			try {
				execute();
			} catch (RuntimeException e) {
				throw e;
			} catch (Exception e) {
				throw new IllegalStateException("Unable to execute deferred read", e);
			}
		return delegate;
	}

	@Override
	public boolean hasNext() {
		var raw = ready();
		long start = System.nanoTime();
		try {
			try {
				return raw.hasNext();
			} finally {
				session.elapsed(System.nanoTime() - start);
			}
		} catch (RuntimeException e) {
			fail(e);
			throw e;
		}
	}

	@Override
	public IHeadersDataRow next() {
		var raw = ready();
		long start = System.nanoTime();
		try {
			IHeadersDataRow row;
			try {
				row = raw.next();
			} finally {
				session.elapsed(System.nanoTime() - start);
			}
			session.row();
			return row;
		} catch (RuntimeException e) {
			fail(e);
			throw e;
		}
	}

	private void fail(RuntimeException failure) {
		try {
			close();
		} catch (Exception e) {
			failure.addSuppressed(e);
		}
	}

	@Override
	public void close() throws IOException {
		if (closed)
			return;
		closed = true;
		Throwable failure = null;
		try {
			if (delegate != null)
				delegate.close();
		} catch (IOException | RuntimeException | Error e) {
			failure = e;
			throw e;
		} finally {
			try {
				if (session != null)
					session.close();
			} catch (RuntimeException | Error e) {
				if (failure != null)
					failure.addSuppressed(e);
				else
					throw e;
			} finally {
				delegate = null;
			}
		}
	}

	@Override
	public void reset() throws Exception {
		var raw = ready();
		try {
			if (session != null)
				session.close();
			var ticket = DatabaseUsageGuard.admit(engine);
			limited = ticket != null;
			session = new ReadUsageSession(ticket);
			long start = System.nanoTime();
			try {
				raw.reset();
			} finally {
				session.elapsed(System.nanoTime() - start);
			}
		} catch (Exception e) {
			try {
				close();
			} catch (Exception close) {
				e.addSuppressed(close);
			}
			throw e;
		}
	}

	@Override
	public String[] getHeaders() {
		return ready().getHeaders();
	}

	@Override
	public SemossDataType[] getTypes() {
		return ready().getTypes();
	}

	@Override
	public long getNumRows() throws Exception {
		return ready().getNumRows();
	}

	@Override
	public long getNumRecords() throws Exception {
		return ready().getNumRecords();
	}

	// Bulk flush must not bypass incremental row accounting.
	@Override
	public boolean flushable() {
		return delegate != null && !limited && delegate.flushable();
	}

	@Override
	public String flush() {
		var raw = ready();
		if (limited)
			throw new UnsupportedOperationException("Limited reads must be iterated");
		return raw.flush();
	}

	@Override
	public String getQuery() {
		return delegate == null ? null : delegate.getQuery();
	}

	@Override
	public IDatabaseEngine getEngine() {
		return engine;
	}

	@Override
	public void setEngine(IDatabaseEngine engine) {
		if (this.engine != engine)
			throw new IllegalStateException("A read must retain its engine");
	}

	@Override
	public void setQuery(String query) {
		// The factory retains the original query structure, including graph/R setup.
		// Never execute that old query as a side effect of changing a deferred one.
		if (delegate == null || !Objects.equals(delegate.getQuery(), query))
			throw new IllegalStateException("Create a new read wrapper to change its query");
	}
}
