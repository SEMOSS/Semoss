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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IEngineWrapper;
import prerna.engine.impl.rdbms.MultiRDBMSNativeEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.om.ThreadStore;

/**
 * Keeps legacy SELECT/CONSTRUCT iterators on the same read-accounting boundary.
 */
public final class UsageTrackingLegacyWrapper {
	private UsageTrackingLegacyWrapper() {
	}

	public static <T extends IEngineWrapper> T create(IDatabaseEngine engine, Class<T> api, Supplier<T> factory) {
		if (!api.isInterface() || !Iterator.class.isAssignableFrom(api))
			throw new IllegalArgumentException("A read iterator interface is required");
		if (engine instanceof RDBMSNativeEngine || engine instanceof MultiRDBMSNativeEngine
				|| !DatabaseUsageGuard.external(engine) || ThreadStore.getUser() == null)
			return factory.get();
		var ticket = DatabaseUsageGuard.admit(engine);
		if (ticket == null)
			return factory.get();
		var session = new ReadUsageSession(ticket);
		long start = System.nanoTime();
		T delegate = null;
		try {
			delegate = Objects.requireNonNull(factory.get(), "read wrapper");
			session.elapsed(System.nanoTime() - start);
			return api.cast(Proxy.newProxyInstance(UsageTrackingLegacyWrapper.class.getClassLoader(),
					new Class<?>[] { api }, new Handler(engine, delegate, session)));
		} catch (RuntimeException | Error failure) {
			session.elapsed(System.nanoTime() - start);
			if (delegate != null)
				try {
					delegate.close();
				} catch (Exception close) {
					failure.addSuppressed(close);
				}
			try {
				session.close();
			} catch (RuntimeException close) {
				failure.addSuppressed(close);
			}
			throw failure;
		}
	}

	private static Object invoke(IEngineWrapper target, Method method, Object[] args) throws Throwable {
		try {
			return method.invoke(target, args);
		} catch (InvocationTargetException e) {
			throw e.getCause();
		}
	}

	private static final class Handler implements InvocationHandler {
		private final IDatabaseEngine engine;
		private final IEngineWrapper delegate;
		private final ReentrantLock lock = new ReentrantLock();
		private ReadUsageSession session;
		private boolean closed;

		Handler(IDatabaseEngine engine, IEngineWrapper delegate, ReadUsageSession session) {
			this.engine = engine;
			this.delegate = delegate;
			this.session = session;
		}

		@Override
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			String name = method.getName();
			if (method.getDeclaringClass() == Object.class)
				return switch (name) {
				case "equals" -> proxy == args[0];
				case "hashCode" -> System.identityHashCode(proxy);
				default -> "MeteredLegacyRead";
				};
			if (name.equals("forEachRemaining")) {
				forEach(proxy, args[0]);
				return null;
			}
			lock.lock();
			try {
				if (name.equals("getEngine"))
					return engine;
				if (name.equals("setEngine")) {
					if (args[0] != engine)
						throw new IllegalStateException("A read must retain its database");
					return null;
				}
				if (name.equals("close")) {
					close();
					return null;
				}
				if (name.equals("execute")) {
					execute();
					return null;
				}
				if (name.equals("hasNext") || name.equals("next")) {
					if (closed) {
						if (name.equals("hasNext"))
							return false;
						throw new NoSuchElementException("Read is closed");
					}
					long start = System.nanoTime();
					try {
						Object value;
						try {
							value = UsageTrackingLegacyWrapper.invoke(delegate, method, args);
						} finally {
							session.elapsed(System.nanoTime() - start);
						}
						if (name.equals("next"))
							session.row();
						else if (Boolean.FALSE.equals(value))
							session.close();
						return value;
					} catch (Throwable failure) {
						cleanup(failure);
						throw failure;
					}
				}
				return UsageTrackingLegacyWrapper.invoke(delegate, method, args);
			} finally {
				lock.unlock();
			}
		}

		@SuppressWarnings("unchecked")
		private void forEach(Object proxy, Object action) {
			Consumer<Object> consumer = (Consumer<Object>) Objects.requireNonNull(action);
			Iterator<?> iterator = (Iterator<?>) proxy;
			while (iterator.hasNext())
				consumer.accept(iterator.next());
		}

		private void execute() throws Throwable {
			close();
			session = new ReadUsageSession(DatabaseUsageGuard.admit(engine));
			closed = false;
			long start = System.nanoTime();
			try {
				delegate.execute();
			} catch (Throwable failure) {
				session.elapsed(System.nanoTime() - start);
				cleanup(failure);
				throw failure;
			} finally {
				session.elapsed(System.nanoTime() - start);
			}
		}

		private void cleanup(Throwable failure) {
			try {
				close();
			} catch (Throwable close) {
				failure.addSuppressed(close);
			}
		}

		private void close() throws Exception {
			if (closed)
				return;
			closed = true;
			Throwable failure = null;
			try {
				delegate.close();
			} catch (Exception | Error e) {
				failure = e;
				throw e;
			} finally {
				try {
					session.close();
				} catch (RuntimeException | Error e) {
					if (failure != null)
						failure.addSuppressed(e);
					else
						throw e;
				}
			}
		}
	}
}
