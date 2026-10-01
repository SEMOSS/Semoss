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
package prerna.util;

import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;
import com.google.gson.reflect.TypeToken;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.rdf.engine.wrappers.RawRDBMSSelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.sql.AbstractSqlQueryUtil;

/**
 * Executes database queries and materializes their results as Java scalars and
 * collections.
 *
 * <p>
 * Methods accepting an engine manage their query resources and close them on
 * completion or failure. Query-structure and raw-query methods obtain wrappers
 * through {@link WrapperManager}; parameterized queries use prepared
 * statements. Methods accepting an existing {@link IRawSelectWrapper} consume
 * its remaining rows; the caller remains responsible for closing that wrapper.
 *
 * <p>
 * Collection methods materialize all available rows in memory. The
 * {@link ParameterizedQuery} overload applies its requested JDBC row limit;
 * other overloads require callers to set any limit in the query or statement.
 * Lists preserve wrapper iteration order; set and map ordering is described by
 * the individual methods.
 */
public class QueryExecutionUtility {

	private static final Logger classLogger = LogManager.getLogger(QueryExecutionUtility.class);

	private static final Gson GSON = new GsonBuilder().setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
			.disableHtmlEscaping().create();

	/**
	 * Describes SQL text, ordered JDBC bind values, and a requested result-row
	 * limit for reuse by query consumers.
	 *
	 * <p>
	 * This record only carries data: the consumer validates and executes the SQL,
	 * binds the parameters, and applies the limit. The parameter list is stored by
	 * reference without a defensive copy.
	 *
	 * @param sql        SQL containing parameter placeholders
	 * @param parameters values in placeholder order, starting with JDBC parameter 1
	 * @param limit      maximum rows to be applied by the query consumer
	 */
	public record ParameterizedQuery(String sql, List<Object> parameters, int limit) {
	}

	private QueryExecutionUtility() {

	}

	/**
	 * Synchronous work in a transaction owned by this utility. Close all statements
	 * and result sets before returning a materialized result. Do not close the
	 * connection, change auto-commit, or commit/roll back. Nested operations must
	 * use connection-based helpers, not another engine-based read/write call.
	 */
	@FunctionalInterface
	public interface JdbcWork<T> {
		T run(Connection connection) throws Exception;
	}

	/**
	 * Executes query-only work, rolling back a successful manual read transaction
	 * before releasing the connection. This is a caller contract: SQL is not
	 * inspected and the connection is not made read-only. Owns the entire
	 * transaction; do not use with another caller's unfinished transaction.
	 */
	public static <T> T read(IRDBMSEngine engine, JdbcWork<T> work) throws Exception {
		return executeJdbc(engine, false, work);
	}

	/**
	 * Executes one atomic operation and commits before returning its result,
	 * including when the acquired connection already has auto-commit disabled. Owns
	 * the entire transaction; do not use with another caller's unfinished
	 * transaction. A cleanup failure after commit does not undo committed writes.
	 * No failures are retried automatically.
	 */
	public static <T> T write(IRDBMSEngine engine, JdbcWork<T> work) throws Exception {
		return executeJdbc(engine, true, work);
	}

	private static <T> T executeJdbc(IRDBMSEngine engine, boolean write, JdbcWork<T> work) throws Exception {
		Objects.requireNonNull(engine, "engine");
		Objects.requireNonNull(work, "work");
		boolean pooling = engine.isConnectionPooling();
		Connection connection = Objects.requireNonNull(engine.getConnection(), "connection");
		if (pooling) {
			return withJdbcConnection(engine, connection, write, work);
		}
		// Engine proxies can share the same non-pooled connection.
		synchronized (connection) {
			return withJdbcConnection(engine, connection, write, work);
		}
	}

	private static <T> T withJdbcConnection(IRDBMSEngine engine, Connection connection, boolean write, JdbcWork<T> work)
			throws Exception {
		Boolean originalAutoCommit = null;
		boolean manualTransaction = false;
		boolean changeAttempted = false;
		boolean transactionEnded = false;
		boolean rollbackAttempted = false;
		Throwable failure = null;
		T result = null;
		try {
			originalAutoCommit = connection.getAutoCommit();
			manualTransaction = !originalAutoCommit;
			if (write && originalAutoCommit) {
				// A driver may change state before reporting failure.
				changeAttempted = true;
				connection.setAutoCommit(false);
				manualTransaction = true;
			}
			result = work.run(connection);
			if (write) {
				connection.commit();
			} else if (manualTransaction) {
				rollbackAttempted = true;
				try {
					connection.rollback();
				} catch (Exception | Error cleanup) {
					classLogger.error("Error completing JDBC read transaction", cleanup);
					throw cleanup;
				}
			}
			transactionEnded = true;
		} catch (Exception | Error e) {
			failure = e;
			if (!rollbackAttempted && (manualTransaction || changeAttempted)) {
				try {
					connection.rollback();
					transactionEnded = true;
				} catch (Exception | Error cleanup) {
					failure = jdbcCleanupFailure(failure, cleanup, "Error rolling back JDBC transaction");
				}
			}
		}
		// Never enable auto-commit after a failed rollback, or guess an unread state.
		if (changeAttempted && transactionEnded && originalAutoCommit != null) {
			try {
				connection.setAutoCommit(originalAutoCommit);
			} catch (Exception | Error cleanup) {
				failure = jdbcCleanupFailure(failure, cleanup, "Error restoring JDBC auto-commit");
			}
		}
		try {
			ConnectionUtils.closeConnectionIfPooling(engine, connection);
		} catch (Exception | Error cleanup) {
			failure = jdbcCleanupFailure(failure, cleanup, "Error releasing JDBC connection");
		}
		if (failure instanceof Exception) {
			throw (Exception) failure;
		}
		if (failure instanceof Error) {
			throw (Error) failure;
		}
		return result;
	}

	private static Throwable jdbcCleanupFailure(Throwable failure, Throwable cleanup, String message) {
		classLogger.error(message, cleanup);
		if (failure == null) {
			return cleanup;
		}
		if (failure != cleanup) {
			failure.addSuppressed(cleanup);
		}
		return failure;
	}

	/**
	 * Executes a query and returns the first column of its first row as a string.
	 * The value is cast to {@link String}, not converted with {@code toString()}.
	 * Later rows and columns are ignored.
	 *
	 * @param engine database engine used to execute the query
	 * @param qs     query whose first column is a string or {@code null}
	 * @return the first value, or {@code null} if it is null or there are no rows
	 * @throws IllegalArgumentException if execution, casting, iteration, or wrapper
	 *                                  closure fails
	 */
	public static String flushToString(IDatabaseEngine engine, SelectQueryStruct qs) {
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs)) {
			while (wrapper.hasNext()) {
				return (String) wrapper.next().getValues()[0];
			}
		} catch (Exception e) {
			classLogger.error("Error flushing query result to String", e);
			throw new IllegalArgumentException("Error executing query: " + e.getMessage());
		}

		return null;
	}

	/**
	 * Executes a query and returns the first non-null value in its first column,
	 * converted with {@link Number#intValue()}. Rows with a null first column are
	 * skipped; other columns are ignored.
	 *
	 * @param engine database engine used to execute the query
	 * @param qs     query whose first column contains numbers or nulls
	 * @return the converted value, or {@code null} if no non-null value is found
	 * @throws IllegalArgumentException if execution, numeric conversion, iteration,
	 *                                  or wrapper closure fails
	 */
	public static Integer flushToInteger(IDatabaseEngine engine, SelectQueryStruct qs) {
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs)) {
			while (wrapper.hasNext()) {
				Number val = ((Number) wrapper.next().getValues()[0]);
				if (val != null) {
					return val.intValue();
				}
			}
		} catch (Exception e) {
			classLogger.error("Error flushing query result to Integer", e);
			throw new IllegalArgumentException("Error executing query: " + e.getMessage());
		}

		return null;
	}

	/**
	 * Executes a query and returns the first non-null value in its first column,
	 * converted with {@link Number#longValue()}. Rows with a null first column are
	 * skipped; other columns are ignored.
	 *
	 * @param engine database engine used to execute the query
	 * @param qs     query whose first column contains numbers or nulls
	 * @return the converted value, or {@code null} if no non-null value is found
	 * @throws IllegalArgumentException if execution, numeric conversion, iteration,
	 *                                  or wrapper closure fails
	 */
	public static Long flushToLong(IDatabaseEngine engine, SelectQueryStruct qs) {
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs)) {
			while (wrapper.hasNext()) {
				Number val = ((Number) wrapper.next().getValues()[0]);
				if (val != null) {
					return val.longValue();
				}
			}
		} catch (Exception e) {
			classLogger.error("Error flushing query result to Long", e);
			throw new IllegalArgumentException("Error executing query: " + e.getMessage());
		}

		return null;
	}

	/**
	 * Executes a query and converts each first-column value with
	 * {@link Object#toString()}. Null first-column values are not supported.
	 *
	 * @param engine database engine used to execute the query
	 * @param qs     query whose first-column values are non-null
	 * @return a mutable list in row order, retaining duplicates; empty for no rows
	 * @throws IllegalArgumentException if a first-column value is null, or
	 *                                  execution, conversion, iteration, or wrapper
	 *                                  closure fails
	 */
	public static List<String> flushToListString(IDatabaseEngine engine, SelectQueryStruct qs) {
		List<String> values = new ArrayList<String>();
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs)) {
			while (wrapper.hasNext()) {
				values.add(wrapper.next().getValues()[0].toString());
			}
		} catch (Exception e) {
			classLogger.error("Error flushing query result to List<String>", e);
			throw new IllegalArgumentException("Error executing query: " + e.getMessage());
		}

		return values;
	}

	/**
	 * Executes a query and collects distinct first-column values after converting
	 * them with {@link Object#toString()}. Null first-column values are not
	 * supported.
	 *
	 * @param engine database engine used to execute the query
	 * @param qs     query whose first-column values are non-null
	 * @param order  {@code true} for natural string ordering in a {@link TreeSet};
	 *               {@code false} for a {@link HashSet} with unspecified order
	 * @return a mutable set of distinct strings, or an empty set for no rows
	 * @throws IllegalArgumentException if a first-column value is null, or
	 *                                  execution, conversion, iteration, or wrapper
	 *                                  closure fails
	 */
	public static Set<String> flushToSetString(IDatabaseEngine engine, SelectQueryStruct qs, boolean order) {
		Set<String> values = null;
		if (order) {
			values = new TreeSet<String>();
		} else {
			values = new HashSet<String>();
		}
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs)) {
			while (wrapper.hasNext()) {
				values.add(wrapper.next().getValues()[0].toString());
			}
		} catch (Exception e) {
			classLogger.error("Error flushing query result to Set<String>", e);
			throw new IllegalArgumentException("Error executing query: " + e.getMessage());
		}

		return values;
	}

	/**
	 * Executes a raw query and collects distinct first-column values after
	 * converting them with {@link Object#toString()}. The query text is passed to
	 * {@link WrapperManager} unchanged; this overload does not bind parameters.
	 * Null first-column values are not supported.
	 *
	 * @param engine database engine used to execute the query
	 * @param query  raw query whose first-column values are non-null
	 * @param order  {@code true} for natural string ordering in a {@link TreeSet};
	 *               {@code false} for a {@link HashSet} with unspecified order
	 * @return a mutable set of distinct strings, or an empty set for no rows
	 * @throws IllegalArgumentException if a first-column value is null, or
	 *                                  execution, conversion, iteration, or wrapper
	 *                                  closure fails
	 */
	public static Set<String> flushToSetString(IDatabaseEngine engine, String query, boolean order) {
		Set<String> values = null;
		if (order) {
			values = new TreeSet<String>();
		} else {
			values = new HashSet<String>();
		}
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, query)) {
			while (wrapper.hasNext()) {
				values.add(wrapper.next().getValues()[0].toString());
			}
		} catch (Exception e) {
			classLogger.error("Error flushing raw query result to Set<String>", e);
			throw new IllegalArgumentException("Error executing query: " + e.getMessage());
		}

		return values;
	}

	/**
	 * Executes a query and converts every row to a new array of strings in column
	 * order. String conversion represents null values as the literal
	 * {@code "null"}.
	 *
	 * @param engine database engine used to execute the query
	 * @param qs     query to execute
	 * @return a mutable list of string arrays in row order, or an empty list for no
	 *         rows
	 * @throws IllegalArgumentException if execution, conversion, iteration, or
	 *                                  wrapper closure fails
	 */
	public static List<String[]> flushRsToListOfStrArray(IDatabaseEngine engine, SelectQueryStruct qs) {
		List<String[]> ret = new ArrayList<String[]>();

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs)) {
			while (wrapper.hasNext()) {
				IHeadersDataRow headerRow = wrapper.next();
				Object[] values = headerRow.getValues();
				int len = values.length;
				String[] strVals = new String[len];
				for (int i = 0; i < len; i++) {
					strVals[i] = values[i] + "";
				}
				ret.add(strVals);
			}
		} catch (Exception e) {
			classLogger.error("Error flushing query result set to List<String[]>", e);
			throw new IllegalArgumentException("Error executing query: " + e.getMessage());
		}

		return ret;
	}

	/**
	 * Executes a query and collects each row's value array in column order. Values,
	 * including nulls, retain their wrapper-provided types; arrays are not copied.
	 *
	 * @param engine database engine used to execute the query
	 * @param qs     query to execute
	 * @return a mutable list of value arrays in row order, or an empty list for no
	 *         rows
	 * @throws IllegalArgumentException if execution, iteration, or wrapper closure
	 *                                  fails
	 */
	public static List<Object[]> flushRsToListOfObjArray(IDatabaseEngine engine, SelectQueryStruct qs) {
		List<Object[]> ret = new ArrayList<Object[]>();

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs)) {
			while (wrapper.hasNext()) {
				ret.add(wrapper.next().getValues());
			}
		} catch (Exception e) {
			classLogger.error("Error flushing query result set to List<Object[]>", e);
			throw new IllegalArgumentException("Error executing query: " + e.getMessage());
		}

		return ret;
	}

	/**
	 * Executes a query and collects the wrapper-provided row arrays without copying
	 * their values or changing nulls.
	 *
	 * @param engine database engine used to execute the query
	 * @param qs     query to execute
	 * @return a mutable list of value arrays in row order, or an empty list for no
	 *         rows
	 * @throws IllegalArgumentException if execution, iteration, or wrapper closure
	 *                                  fails
	 * @deprecated Use
	 *             {@link #flushRsToListOfObjArray(IDatabaseEngine, SelectQueryStruct)}.
	 */
	@Deprecated
	static List<Object[]> flushRsToMatrix(IDatabaseEngine engine, SelectQueryStruct qs) {
		List<Object[]> ret = new ArrayList<Object[]>();

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs)) {
			while (wrapper.hasNext()) {
				ret.add(wrapper.next().getValues());
			}
		} catch (Exception e) {
			classLogger.error("Error flushing query result set to matrix", e);
			throw new IllegalArgumentException("Error executing query: " + e.getMessage());
		}

		return ret;
	}

	/**
	 * Executes a query and maps each row's headers to its values using
	 * {@link #flushWrapperToMap(IRawSelectWrapper)}. JDBC CLOB and BLOB values are
	 * read as strings; other values, including nulls, retain their types.
	 *
	 * @param engine database engine used to execute the query
	 * @param qs     query to execute
	 * @return a mutable list of row maps in row order, or an empty list for no rows
	 * @throws IllegalArgumentException if execution, conversion, iteration, or
	 *                                  wrapper closure fails
	 */
	public static List<Map<String, Object>> flushRsToMap(IDatabaseEngine engine, SelectQueryStruct qs) {
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs)) {
			return flushWrapperToMap(wrapper);
		} catch (Exception e) {
			classLogger.error("Error flushing query result set to List<Map>", e);
			throw new IllegalArgumentException("Error executing query: " + e.getMessage());
		}
	}

	/**
	 * Executes a query and maps each row's headers to values, optionally parsing
	 * selected columns as JSON objects. Conversion follows
	 * {@link #flushWrapperToMap(IRawSelectWrapper, Set)}.
	 *
	 * @param engine  database engine used to execute the query
	 * @param qs      query to execute
	 * @param mapKeys exact, case-sensitive headers to parse as JSON objects; null
	 *                or empty disables JSON parsing. All columns remain in each
	 *                row.
	 * @return a mutable list of row maps in row order, or an empty list for no rows
	 * @throws IllegalArgumentException if execution, LOB conversion, iteration, or
	 *                                  wrapper closure fails
	 */
	public static List<Map<String, Object>> flushRsToMap(IDatabaseEngine engine, SelectQueryStruct qs,
			Set<String> mapKeys) {
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs)) {
			return flushWrapperToMap(wrapper, mapKeys);
		} catch (Exception e) {
			classLogger.error("Error flushing query result set to List<Map> with key filter", e);
			throw new IllegalArgumentException("Error executing query: " + e.getMessage());
		}
	}

	/**
	 * Executes a parameterized read and maps result headers to values using
	 * {@link #flushWrapperToMap(IRawSelectWrapper)}. Values are bound in list order
	 * with {@link PreparedStatement#setObject(int, Object)}, including nulls.
	 * Headers and wrapper-provided value types are preserved; JDBC CLOBs and BLOBs
	 * are read as strings. No JSON parsing or API-specific formatting is applied.
	 *
	 * <p>
	 * The query limit is applied with {@link PreparedStatement#setMaxRows(int)} and
	 * the timeout with {@link PreparedStatement#setQueryTimeout(int)}. Zero
	 * disables the respective JDBC limit. Callers enforce any application-specific
	 * maximums before calling this method.
	 *
	 * <p>
	 * The result set and statement are closed on success or failure. Pooled
	 * connections are released through {@link ConnectionUtils}; non-pooled engine
	 * connections remain open. Manual read transactions are completed by rollback.
	 * Cleanup failures are logged and retain the original failure when one exists.
	 *
	 * @param engine              relational database engine used to prepare the
	 *                            query
	 * @param query               non-null query with non-blank SQL, a non-null
	 *                            parameter list, and a non-negative row limit
	 * @param queryTimeoutSeconds non-negative JDBC timeout in seconds; zero means
	 *                            no timeout
	 * @return a mutable list of row maps in result order, or an empty list for no
	 *         rows; map key order is unspecified
	 * @throws IllegalArgumentException if arguments are invalid or preparing,
	 *                                  binding, executing, reading, or converting
	 *                                  the query fails; execution failures retain
	 *                                  their cause without including database
	 *                                  details in the exception message
	 */
	public static List<Map<String, Object>> flushRsToMap(IRDBMSEngine engine, ParameterizedQuery query,
			int queryTimeoutSeconds) {
		if (engine == null || query == null || query.sql() == null || query.sql().isBlank()
				|| query.parameters() == null || query.limit() < 0 || queryTimeoutSeconds < 0) {
			throw new IllegalArgumentException(
					"A database engine, SQL, parameters, and non-negative limits are required");
		}
		try {
			return read(engine, connection -> flushRsToMap(connection, query, queryTimeoutSeconds));
		} catch (Exception e) {
			classLogger.error("Error executing parameterized query", e);
			throw new IllegalArgumentException("Error executing parameterized query", e);
		}
	}

	/**
	 * Executes a parameterized query within a caller-owned transaction. Preserves
	 * the engine overload's limits, timeout, binding and result mapping, but only
	 * closes the statement and result set it creates. Does not change or close the
	 * connection, commit, or roll back. Failures propagate to the transaction
	 * owner.
	 */
	public static List<Map<String, Object>> flushRsToMap(Connection connection, ParameterizedQuery query,
			int queryTimeoutSeconds) throws Exception {
		if (connection == null || query == null || query.sql() == null || query.sql().isBlank()
				|| query.parameters() == null || query.limit() < 0 || queryTimeoutSeconds < 0) {
			throw new IllegalArgumentException(
					"A database connection, SQL, parameters, and non-negative limits are required");
		}
		try (PreparedStatement statement = connection.prepareStatement(query.sql())) {
			statement.setMaxRows(query.limit());
			statement.setQueryTimeout(queryTimeoutSeconds);
			for (int i = 0; i < query.parameters().size(); i++) {
				statement.setObject(i + 1, query.parameters().get(i));
			}
			try (ResultSet result = statement.executeQuery()) {
				return flushWrapperToMap(RawRDBMSSelectWrapper.flushRsToWrapper(result));
			}
		}
	}

	/**
	 * Executes a query and maps each row's first column to its second column.
	 * Additional columns are ignored. Later rows replace earlier values for the
	 * same key; null keys and values are retained.
	 *
	 * @param engine database engine used to execute the query
	 * @param qs     query returning at least two columns per row
	 * @return a mutable map with unspecified key order, or an empty map for no rows
	 * @throws IllegalArgumentException if a row has fewer than two columns, or
	 *                                  execution, iteration, or wrapper closure
	 *                                  fails
	 */
	public static Map<Object, Object> flushRsToKeyValueMap(IDatabaseEngine engine, SelectQueryStruct qs) {
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs)) {
			Map<Object, Object> result = new HashMap<>();
			try {
				while (wrapper.hasNext()) {
					IHeadersDataRow headerRow = wrapper.next();
					Object[] values = headerRow.getValues();
					result.put(values[0], values[1]);
				}
			} catch (Exception e) {
				classLogger.error("Error iterating query result set to key-value Map", e);
				throw new IllegalArgumentException("Error executing query: " + e.getMessage());
			}
			return result;
		} catch (Exception e) {
			classLogger.error("Error flushing query result set to key-value Map", e);
			throw new IllegalArgumentException("Error executing query: " + e.getMessage());
		}
	}

	/**
	 * Consumes the remaining rows of an existing wrapper and maps headers to
	 * values. Equivalent to {@link #flushWrapperToMap(IRawSelectWrapper, Set)} with
	 * no JSON columns selected. JDBC CLOB and BLOB values are read as strings;
	 * other values, including nulls, retain their types.
	 *
	 * <p>
	 * This method does not close the wrapper; the caller owns its lifecycle.
	 *
	 * @param wrapper executed wrapper positioned before the next row to consume
	 * @return a mutable list of row maps in iteration order, or an empty list if
	 *         the wrapper has no remaining rows; map key order is unspecified
	 * @throws IllegalArgumentException if reading or converting a row fails
	 */
	public static List<Map<String, Object>> flushWrapperToMap(IRawSelectWrapper wrapper) {
		return flushWrapperToMap(wrapper, null);
	}

	/**
	 * Consumes the remaining rows of an existing wrapper and maps each row's
	 * headers to values. Header spelling is preserved; duplicate headers overwrite
	 * earlier columns with the same header.
	 *
	 * <p>
	 * For headers in {@code mapKeys}, string values (including text read from JDBC
	 * CLOBs or BLOBs) are parsed as JSON objects. Successful parsing produces
	 * nested maps and lists, with numbers represented as {@link Long} or
	 * {@link Double}. If parsing does not produce an object, the original wrapper
	 * value is retained. For other headers, CLOBs and BLOBs are converted to
	 * strings and other values, including nulls, are retained unchanged. The
	 * selected headers do not filter which columns are returned.
	 *
	 * <p>
	 * This method does not close the wrapper; the caller owns its lifecycle.
	 *
	 * @param wrapper executed wrapper positioned before the next row to consume
	 * @param mapKeys exact, case-sensitive headers to parse as JSON objects; null
	 *                or empty disables JSON parsing
	 * @return a mutable list of row maps in iteration order, or an empty list if
	 *         the wrapper has no remaining rows; map key order is unspecified
	 * @throws IllegalArgumentException if iteration or LOB conversion fails;
	 *                                  invalid JSON retains the original value
	 *                                  instead
	 */
	public static List<Map<String, Object>> flushWrapperToMap(IRawSelectWrapper wrapper, Set<String> mapKeys) {
		List<Map<String, Object>> result = new ArrayList<>();
		try {
			while (wrapper.hasNext()) {
				IHeadersDataRow headerRow = wrapper.next();
				String[] headers = headerRow.getHeaders();
				Object[] values = headerRow.getValues();
				Map<String, Object> map = new HashMap<String, Object>();
				for (int i = 0; i < headers.length; i++) {
					String value = null;
					if (values[i] instanceof java.sql.Clob) {
						value = AbstractSqlQueryUtil.flushClobToString((java.sql.Clob) values[i]);
					} else if (values[i] instanceof java.sql.Blob) {
						value = AbstractSqlQueryUtil.flushBlobToString((java.sql.Blob) values[i]);
					}
					if (mapKeys != null && mapKeys.contains(headers[i])) {
						Map<String, Object> processedValue = convertJsonString(value == null ? values[i] : value);
						map.put(headers[i], processedValue == null ? values[i] : processedValue);
					} else {
						map.put(headers[i], value == null ? values[i] : value);
					}
				}
				result.add(map);
			}
		} catch (Exception e) {
			classLogger.error("Error flushing wrapper result set to List<Map>", e);
			throw new IllegalArgumentException("Error executing query: " + e.getMessage());
		}

		return result;
	}

	/**
	 * Attempts to parse a string value as a JSON object using the shared Gson
	 * configuration. Conversion errors are treated as an unavailable conversion.
	 *
	 * @param jsonString candidate value, expected to be a string containing a JSON
	 *                   object
	 * @return the parsed map, or {@code null} for null, non-string, or unparseable
	 *         input, or when parsing produces null
	 */
	private static Map<String, Object> convertJsonString(Object jsonString) {
		if (jsonString == null) {
			return null;
		}
		try {
			String json = (String) jsonString;
			Type type = new TypeToken<Map<String, Object>>() {
			}.getType();
			return GSON.fromJson(json, type);
		} catch (Exception e) {
			// Not a valid JSON object return null
			return null;
		}
	}
}
