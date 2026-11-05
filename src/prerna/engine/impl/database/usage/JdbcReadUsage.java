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
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.Select;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.usage.UsageLimiter;
import prerna.om.ThreadStore;
import prerna.query.parsers.SqlScriptSplitter;

/**
 * JDBC read boundary, shared by raw, prepared, deferred, count and direct JDBC
 * reads.
 */
public final class JdbcReadUsage {
	private static final ThreadLocal<IDatabaseEngine> ENGINE_CONTEXT = new ThreadLocal<>();

	private JdbcReadUsage() {
	}

	@FunctionalInterface
	public interface SqlWork<T> {
		T run() throws SQLException;
	}

	/**
	 * Attribute a contextual database's JDBC work to its public catalog identity.
	 */
	public static <T> T withEngine(IDatabaseEngine engine, SqlWork<T> work) throws SQLException {
		IDatabaseEngine previous = ENGINE_CONTEXT.get();
		ENGINE_CONTEXT.set(engine);
		try {
			return work.run();
		} finally {
			if (previous == null)
				ENGINE_CONTEXT.remove();
			else
				ENGINE_CONTEXT.set(previous);
		}
	}

	public static Connection wrap(IDatabaseEngine engine, Connection connection) {
		IDatabaseEngine effective = ENGINE_CONTEXT.get() == null ? engine : ENGINE_CONTEXT.get();
		if (!DatabaseUsageGuard.external(effective) || ThreadStore.getUser() == null)
			return connection;
		return wrap(connection, () -> DatabaseUsageGuard.admit(effective));
	}

	static Connection wrap(Connection connection, Supplier<UsageLimiter.Ticket> admission) {
		if (Proxy.isProxyClass(connection.getClass())
				&& Proxy.getInvocationHandler(connection) instanceof ConnectionHandler)
			return connection;
		return (Connection) Proxy.newProxyInstance(JdbcReadUsage.class.getClassLoader(),
				new Class<?>[] { Connection.class }, new ConnectionHandler(connection, admission));
	}

	/**
	 * Keep legacy non-pooled transaction synchronization on the physical handle.
	 */
	public static Object transactionIdentity(Connection connection) {
		if (connection != null && Proxy.isProxyClass(connection.getClass())
				&& Proxy.getInvocationHandler(connection) instanceof ConnectionHandler handler)
			return transactionIdentity(handler.raw);
		return connection;
	}

	private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
		try {
			return method.invoke(target, args);
		} catch (InvocationTargetException e) {
			throw e.getCause();
		}
	}

	private static boolean objectMethod(Object proxy, Object target, Method method, Object[] args) {
		return method.getDeclaringClass() == Object.class;
	}

	private static Object objectValue(Object proxy, Object target, Method method, Object[] args) {
		return switch (method.getName()) {
		case "equals" -> proxy == args[0];
		case "hashCode" -> System.identityHashCode(proxy);
		default -> "UsageTracked[" + target.getClass().getSimpleName() + "]";
		};
	}

	private static final class ConnectionHandler implements InvocationHandler {
		private final Connection raw;
		private final Supplier<UsageLimiter.Ticket> admission;
		private final List<StatementHandler> statements = new CopyOnWriteArrayList<>();

		ConnectionHandler(Connection raw, Supplier<UsageLimiter.Ticket> admission) {
			this.raw = raw;
			this.admission = admission;
		}

		@Override
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			String name = method.getName();
			if (objectMethod(proxy, raw, method, args))
				return objectValue(proxy, raw, method, args);
			if (name.equals("close") || name.equals("abort")) {
				Throwable failure = null;
				try {
					return JdbcReadUsage.invoke(raw, method, args);
				} catch (Throwable e) {
					failure = e;
					throw e;
				} finally {
					RuntimeException settlementFailure = null;
					for (StatementHandler statement : List.copyOf(statements)) {
						try {
							statement.finish();
						} catch (RuntimeException e) {
							if (settlementFailure == null)
								settlementFailure = e;
							else
								settlementFailure.addSuppressed(e);
						}
					}
					statements.clear();
					if (settlementFailure != null) {
						if (failure != null)
							failure.addSuppressed(settlementFailure);
						else
							throw settlementFailure;
					}
				}
			}
			Object result = JdbcReadUsage.invoke(raw, method, args);
			if (result instanceof Statement statement && (name.equals("createStatement")
					|| name.equals("prepareStatement") || name.equals("prepareCall"))) {
				String sql = args != null && args.length > 0 && args[0] instanceof String ? (String) args[0] : null;
				var handler = new StatementHandler(this, (Connection) proxy, statement, sql);
				statements.add(handler);
				Class<?> api = statement instanceof CallableStatement ? CallableStatement.class
						: statement instanceof PreparedStatement ? PreparedStatement.class : Statement.class;
				return Proxy.newProxyInstance(JdbcReadUsage.class.getClassLoader(), new Class<?>[] { api }, handler);
			}
			return result;
		}
	}

	private static final class StatementHandler implements InvocationHandler {
		private final ConnectionHandler owner;
		private final Connection connection;
		private final Statement raw;
		private final String preparedSql;
		private final Map<ResultSet, Object> results = Collections.synchronizedMap(new IdentityHashMap<>());
		private volatile ReadUsageSession session;

		StatementHandler(ConnectionHandler owner, Connection connection, Statement raw, String sql) {
			this.owner = owner;
			this.connection = connection;
			this.raw = raw;
			preparedSql = sql;
		}

		void finish() {
			ReadUsageSession current = session;
			session = null;
			try {
				if (current != null)
					current.close();
			} finally {
				results.clear();
			}
		}

		private void closeResultsBeforeExecution() throws SQLException {
			SQLException failure = null;
			for (ResultSet rows : List.copyOf(results.keySet())) {
				try {
					rows.close();
				} catch (SQLException e) {
					if (failure == null)
						failure = e;
					else
						failure.addSuppressed(e);
				}
			}
			try {
				finish();
			} catch (RuntimeException e) {
				if (failure != null)
					failure.addSuppressed(e);
				else
					throw e;
			}
			if (failure != null)
				throw failure;
		}

		@Override
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			String name = method.getName();
			if (objectMethod(proxy, raw, method, args))
				return objectValue(proxy, raw, method, args);
			if (name.equals("getConnection"))
				return connection;
			if (name.equals("close")) {
				Throwable failure = null;
				try {
					return JdbcReadUsage.invoke(raw, method, args);
				} catch (Throwable e) {
					failure = e;
					throw e;
				} finally {
					try {
						finish();
					} catch (RuntimeException e) {
						if (failure != null)
							failure.addSuppressed(e);
						else
							throw e;
					} finally {
						owner.statements.remove(this);
					}
				}
			}
			boolean execution = name.equals("executeQuery") || name.equals("execute") || name.equals("executeUpdate")
					|| name.equals("executeLargeUpdate") || name.equals("executeBatch")
					|| name.equals("executeLargeBatch");
			if (execution) {
				closeResultsBeforeExecution();
				String sql = args != null && args.length > 0 && args[0] instanceof String ? (String) args[0]
						: preparedSql;
				boolean read = name.equals("executeQuery") || (name.equals("execute") && isRead(sql));
				if (read) {
					UsageLimiter.Ticket ticket = owner.admission.get();
					if (ticket != null)
						session = new ReadUsageSession(ticket);
				}
			}
			long start = System.nanoTime();
			ReadUsageSession active = session;
			try {
				Object result = JdbcReadUsage.invoke(raw, method, args);
				if (result instanceof ResultSet rows && session != null
						&& (name.equals("executeQuery") || name.equals("getResultSet"))) {
					return result(rows, (Statement) proxy, session);
				}
				if (execution && Boolean.FALSE.equals(result))
					finish();
				if (name.equals("getMoreResults") && Boolean.FALSE.equals(result) && raw.getUpdateCount() == -1)
					finish();
				return result;
			} catch (Throwable failure) {
				if (active != null)
					active.elapsed(System.nanoTime() - start);
				try {
					finish();
				} catch (RuntimeException e) {
					failure.addSuppressed(e);
				}
				throw failure;
			} finally {
				if (active != null)
					active.elapsed(System.nanoTime() - start);
			}
		}

		private Object result(ResultSet rows, Statement statement, ReadUsageSession active) {
			return results.computeIfAbsent(rows, key -> Proxy.newProxyInstance(JdbcReadUsage.class.getClassLoader(),
					new Class<?>[] { ResultSet.class }, (proxy, method, args) -> {
						String name = method.getName();
						if (objectMethod(proxy, rows, method, args))
							return objectValue(proxy, rows, method, args);
						if (name.equals("getStatement"))
							return statement;
						boolean move = name.equals("next") || name.equals("previous") || name.equals("first")
								|| name.equals("last") || name.equals("absolute") || name.equals("relative");
						long start = System.nanoTime();
						Throwable failure = null;
						try {
							Object result;
							try {
								result = JdbcReadUsage.invoke(rows, method, args);
							} finally {
								if (move)
									active.elapsed(System.nanoTime() - start);
							}
							if (move && Boolean.TRUE.equals(result))
								active.row();
							return result;
						} catch (Throwable e) {
							failure = e;
							try {
								rows.close();
							} catch (Throwable close) {
								e.addSuppressed(close);
							}
							try {
								finish();
							} catch (RuntimeException close) {
								e.addSuppressed(close);
							}
							throw e;
						} finally {
							if (name.equals("close"))
								try {
									active.close();
									if (session == active)
										session = null;
								} catch (RuntimeException e) {
									if (failure != null)
										failure.addSuppressed(e);
									else
										throw e;
								}
						}
					}));
		}
	}

	private static boolean isRead(String sql) {
		if (sql == null)
			return false;
		try {
			// Avoid grammar-parsing ordinary writes and batches on this read-only path.
			var statements = SqlScriptSplitter.split(sql);
			if (statements.size() != 1 || statements.get(0).leadingWords().isEmpty())
				return false;
			String first = statements.get(0).leadingWords().get(0);
			if (!Set.of("SELECT", "WITH", "VALUES", "TABLE").contains(first))
				return false;
			var parsed = CCJSqlParserUtil.parse(sql);
			if (!(parsed instanceof Select select))
				return false;
			return readSelect(select);
		} catch (Exception unsupportedDialect) {
			return false;
		}
	}

	private static boolean readSelect(Select select) {
		if (select == null)
			return false;
		if (select.getWithItemsList() != null)
			for (var with : select.getWithItemsList())
				if (!readSelect(with.getSelect()))
					return false;
		if (select instanceof net.sf.jsqlparser.statement.select.ParenthesedSelect nested)
			return readSelect(nested.getSelect());
		if (select instanceof net.sf.jsqlparser.statement.select.PlainSelect plain)
			return plain.getIntoTables() == null || plain.getIntoTables().isEmpty();
		if (select instanceof net.sf.jsqlparser.statement.select.SetOperationList set)
			return set.getSelects().stream().allMatch(JdbcReadUsage::readSelect);
		return select instanceof net.sf.jsqlparser.statement.select.Values;
	}
}
