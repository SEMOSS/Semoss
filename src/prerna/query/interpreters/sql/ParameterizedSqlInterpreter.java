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
package prerna.query.interpreters.sql;

import static prerna.query.interpreters.IQueryInterpreter.getAllSearchComparators;
import static prerna.query.interpreters.IQueryInterpreter.getNegSearchComparators;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import prerna.algorithm.api.SemossDataType;
import prerna.date.SemossDate;
import prerna.engine.api.IRDBMSEngine;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.BetweenQueryFilter;
import prerna.query.querystruct.filters.FunctionQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.joins.BasicRelationship;
import prerna.query.querystruct.joins.IRelation;
import prerna.query.querystruct.joins.SubqueryRelationship;
import prerna.query.querystruct.selectors.AbstractQuerySelector;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.IQuerySort;
import prerna.query.querystruct.selectors.QueryArithmeticSelector;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.query.querystruct.selectors.QueryIfSelector;
import prerna.rdf.engine.wrappers.RawPreparedRDBMSSelectWrapper;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.QueryExecutionUtility.ParameterizedQuery;
import prerna.util.sql.RdbmsTypeEnum;

/**
 * Compiles a {@link SelectQueryStruct} into SQL templates and ordered JDBC
 * values, using the supplied engine's metadata and SQL dialect. Compilation
 * opens no JDBC connection and leaves the input query structure unchanged. Each
 * call creates a fresh compilation context, so an interpreter instance can be
 * reused.
 *
 * <p>
 * SQL is assembled from {@link Fragment} objects that keep text and bindings
 * together. Concatenating fragments concatenates their parameter lists in the
 * same order; repeating an expression repeats its bindings. This preserves JDBC
 * placeholder order even when query-structure traversal differs from SQL clause
 * order. Values are bound separately and are never SQL-escaped or interpolated.
 *
 * <p>
 * The resulting {@link CompiledQuery} contains independent data and count
 * plans. Execution methods pass that plan to
 * {@link RawPreparedRDBMSSelectWrapper}, which owns statement execution,
 * streaming, counting, reset, and resource cleanup. Unsupported query shapes
 * are rejected rather than delegated to literal SQL generation.
 */
public final class ParameterizedSqlInterpreter {

	/**
	 * Supplies SQL dialect rules and physical metadata mappings. Compilation itself
	 * does not acquire a connection; execution delegates connection handling to the
	 * wrapper.
	 */
	private final IRDBMSEngine engine;

	/**
	 * Creates a compiler using an engine's SQL dialect and physical metadata
	 * mappings.
	 *
	 * @param engine non-null database whose metadata and dialect govern compilation
	 * @throws NullPointerException if the engine is null
	 */
	public ParameterizedSqlInterpreter(IRDBMSEngine engine) {
		this.engine = Objects.requireNonNull(engine, "engine");
	}

	/**
	 * Builds data and count templates without executing SQL or changing the query.
	 * Each template carries the values in its own placeholder order. Compilation
	 * normalizes supported scalar values and snapshots the parameter lists.
	 *
	 * @param query non-null structured SELECT, including its filters and subqueries
	 * @return the compiled data query and a count query that includes SQL
	 *         pagination
	 * @throws NullPointerException     if the query or required metadata is null
	 * @throws IllegalArgumentException if a query shape or value is unsupported or
	 *                                  invalid
	 */
	public CompiledQuery compile(SelectQueryStruct query) {
		return new Compilation(engine, Objects.requireNonNull(query, "query")).compile();
	}

	/**
	 * Compiles and executes a query using an engine-acquired connection. The
	 * returned wrapper manages the read lifecycle and must be closed by the caller.
	 * Execution uses no JDBC statement timeout.
	 *
	 * @param query structured SELECT to compile and execute
	 * @return an executed wrapper that streams the result rows
	 * @throws Exception if compilation, execution, or resource initialization fails
	 */
	public RawPreparedRDBMSSelectWrapper execute(SelectQueryStruct query) throws Exception {
		return RawPreparedRDBMSSelectWrapper.executeParameterized(engine, compile(query), 0);
	}

	/**
	 * Compiles and executes inside the caller's existing connection and
	 * transaction. Closing the wrapper releases its statements and results without
	 * committing, rolling back, changing auto-commit, or closing the supplied
	 * connection.
	 *
	 * @param connection     non-null connection whose lifecycle remains with the
	 *                       caller
	 * @param query          structured SELECT to compile and execute
	 * @param timeoutSeconds non-negative JDBC timeout for data and count
	 *                       statements; zero means no statement timeout
	 * @return an executed streaming wrapper that the caller must close
	 * @throws Exception if compilation, execution, or resource initialization fails
	 */
	public RawPreparedRDBMSSelectWrapper execute(Connection connection, SelectQueryStruct query, int timeoutSeconds)
			throws Exception {
		return RawPreparedRDBMSSelectWrapper.executeParameterized(engine, connection, compile(query), timeoutSeconds);
	}

	/**
	 * The executable result of compilation: one plan for rows and another for their
	 * count. Each {@link ParameterizedQuery} carries SQL with placeholders, ordered
	 * bindings, and a JDBC maximum-row limit. The compiler expresses pagination in
	 * SQL and leaves that JDBC limit at zero.
	 *
	 * <p>
	 * The count plan wraps the relevant SELECT, preserving distinct/grouping and
	 * SQL pagination. An unpaged count omits unnecessary ordering. Its bindings
	 * belong to its own SQL template and must not be replaced with the data query's
	 * list. Both parameter lists are copied into unmodifiable lists; their elements
	 * are not recursively copied.
	 *
	 * @param query      data-returning SQL template and its bindings
	 * @param countQuery count SQL template and its independently ordered bindings
	 */
	public record CompiledQuery(ParameterizedQuery query, ParameterizedQuery countQuery) {
		/**
		 * Validates both plans and snapshots their parameter lists, including null
		 * values.
		 *
		 * @param query      data-returning plan to retain
		 * @param countQuery count plan to retain
		 * @throws NullPointerException     if either plan is null
		 * @throws IllegalArgumentException if SQL is blank, bindings are null, or a row
		 *                                  limit is negative
		 */
		public CompiledQuery {
			query = snapshot(query);
			countQuery = snapshot(countQuery);
		}

		/**
		 * Copies a validated plan's parameter list so later list edits do not change
		 * the retained bindings. This is a shallow copy: mutable parameter objects
		 * themselves are not cloned here.
		 *
		 * @param query plan with nonblank SQL, a binding list, and a non-negative row
		 *              limit
		 * @return a plan with the same SQL/limit and an unmodifiable copy of the
		 *         bindings
		 * @throws NullPointerException     if the plan is null
		 * @throws IllegalArgumentException if the plan's SQL, bindings, or limit is
		 *                                  invalid
		 */
		private static ParameterizedQuery snapshot(ParameterizedQuery query) {
			Objects.requireNonNull(query, "query");
			if (query.sql() == null || query.sql().isBlank() || query.parameters() == null || query.limit() < 0) {
				throw new IllegalArgumentException("SQL, parameters, and a non-negative row limit are required");
			}
			return new ParameterizedQuery(query.sql(),
					Collections.unmodifiableList(new ArrayList<>(query.parameters())), query.limit());
		}

		/**
		 * Returns the data query's SQL template, without substituting parameter values.
		 *
		 * @return SQL containing the placeholders to prepare through JDBC
		 */
		public String sql() {
			return query.sql();
		}

		/**
		 * Returns the data query's retained bindings. List index zero corresponds to
		 * JDBC parameter one; callers must preserve this order when binding.
		 *
		 * @return the unmodifiable parameter list, which can contain null values
		 */
		public List<Object> parameters() {
			return query.parameters();
		}
	}

	/**
	 * A piece of SQL and the values belonging to its placeholders, in textual
	 * order. Fragments can represent a constant, expression, clause, or complete
	 * subquery. They are assembled together so SQL rewrites do not lose or reorder
	 * bindings.
	 *
	 * <p>
	 * For example, joining {@code column = } with a value fragment produces
	 * {@code column = ?} and a one-element binding list. Appending that fragment
	 * twice also appends its value twice. The record itself does not validate SQL
	 * or copy the supplied list; assembly methods create the required combined
	 * lists.
	 *
	 * @param sql    SQL text, possibly empty or containing placeholders
	 * @param values values for this fragment's placeholders in left-to-right order
	 */
	private record Fragment(String sql, List<Object> values) {
		/**
		 * Creates a fragment containing compiler-generated or validated SQL structure.
		 * User values belong in {@link #value(Object)}, not in this text.
		 *
		 * @param sql SQL structure with no bind placeholders of its own
		 * @return the text with an empty binding list
		 */
		static Fragment text(String sql) {
			return new Fragment(sql, List.of());
		}

		/**
		 * Creates one JDBC placeholder and retains its corresponding value separately.
		 *
		 * @param value already-normalized value to bind, including null
		 * @return the SQL text {@code ?} and its one-element binding list
		 */
		static Fragment value(Object value) {
			return new Fragment("?", Collections.singletonList(value));
		}

		/**
		 * Appends another fragment without modifying either input. Bindings from the
		 * right-hand fragment follow this fragment's bindings, matching the SQL order.
		 *
		 * @param other fragment to append
		 * @return combined SQL and a newly combined binding list
		 */
		Fragment then(Fragment other) {
			List<Object> combined = new ArrayList<>(values);
			combined.addAll(other.values);
			return new Fragment(sql + other.sql, combined);
		}

		/**
		 * Appends SQL structure that contributes no additional parameter values.
		 *
		 * @param text compiler-generated or validated SQL text to append
		 * @return the combined fragment with the existing binding order preserved
		 */
		Fragment then(String text) {
			return then(text(text));
		}

		/**
		 * Joins fragments with a SQL separator and flattens their bindings in list
		 * order. An empty input produces empty SQL and no values.
		 *
		 * @param fragments ordered fragments to combine
		 * @param separator SQL between fragments, such as a comma or AND; contains no
		 *                  bindings
		 * @return the joined SQL and its ordered bindings
		 */
		static Fragment join(List<Fragment> fragments, String separator) {
			StringBuilder sql = new StringBuilder();
			List<Object> values = new ArrayList<>();
			for (int i = 0; i < fragments.size(); i++) {
				Fragment fragment = fragments.get(i);
				sql.append(i == 0 ? "" : separator).append(fragment.sql);
				values.addAll(fragment.values);
			}
			return new Fragment(sql.toString(), values);
		}
	}

	/**
	 * Mutable state for compiling one SELECT scope. The inherited interpreter
	 * provides metadata resolution helpers only; this class builds its own SQL and
	 * bindings. Child scopes have independent table aliases and projections but
	 * share the identity-based recursion guard with their ancestors.
	 */
	private static final class Compilation extends SqlInterpreter {
		/**
		 * The input SELECT for this scope; compilation reads it without modifying it.
		 */
		private final SelectQueryStruct select;

		/**
		 * Queries on the current compilation path, compared by object identity and
		 * shared with child scopes. A repeated active object indicates a cycle; an
		 * object may be reused in another branch once its earlier compilation finishes.
		 */
		private final Set<SelectQueryStruct> activeQueries;

		/**
		 * Maps conceptual table or derived-source names to generated SQL aliases in
		 * this scope. Insertion order determines aliases such as {@code t0} and
		 * {@code t1}.
		 */
		private final Map<String, String> tableAliases = new LinkedHashMap<>();

		/**
		 * Validated physical table paths used when rendering ordinary FROM/JOIN
		 * sources.
		 */
		private final Map<String, String> physicalTables = new LinkedHashMap<>();

		/**
		 * Child compilation scopes used to resolve derived-table columns and their
		 * types.
		 */
		private final Map<String, Compilation> derivedTables = new LinkedHashMap<>();

		/**
		 * Compiled derived sources, retained with bindings until inserted into
		 * FROM/JOIN SQL.
		 */
		private final Map<String, Fragment> derivedQueries = new LinkedHashMap<>();

		/**
		 * Result labels mapped to selectors for projected-alias sorting and
		 * derived-column lookup.
		 */
		private final Map<String, IQuerySelector> projections = new LinkedHashMap<>();

		/**
		 * Creates a root scope with a fresh recursion guard based on object identity.
		 *
		 * @param engine database supplying the SQL dialect and metadata mappings
		 * @param select query structure to compile in this scope
		 */
		Compilation(IRDBMSEngine engine, SelectQueryStruct select) {
			this(engine, select, Collections.newSetFromMap(new IdentityHashMap<>()));
		}

		/**
		 * Creates a scope sharing its ancestors' recursion guard. Alias and projection
		 * maps remain local so nested queries cannot resolve columns through outer
		 * scopes.
		 *
		 * @param engine        database supplying the SQL dialect and metadata mappings
		 * @param select        query structure to compile in this scope
		 * @param activeQueries identity-based set of queries currently being compiled
		 */
		Compilation(IRDBMSEngine engine, SelectQueryStruct select, Set<SelectQueryStruct> activeQueries) {
			super(engine);
			this.select = select;
			this.activeQueries = activeQueries;
		}

		/**
		 * Compiles this scope as a top-level query, retaining its requested output
		 * ordering.
		 *
		 * @return independent data and count plans with ordered bindings
		 */
		CompiledQuery compile() {
			return compile(false);
		}

		/**
		 * Guards compilation against query-object cycles such as A containing B
		 * containing A. The active marker is removed on success or failure, allowing a
		 * completed subquery to be reused elsewhere in the query structure.
		 *
		 * @param nested true when embedding this SELECT in another query; unnecessary
		 *               ordering is omitted from unpaged subqueries
		 * @return the compiled data and count plans for this scope
		 * @throws IllegalArgumentException if this query is already active or its
		 *                                  structure is unsupported
		 */
		private CompiledQuery compile(boolean nested) {
			// Set.add returns false if this exact query is already being compiled
			// by an ancestor, for example A -> B -> A. Stop before recursing again.
			if (!activeQueries.add(select)) {
				throw unsupported("cyclic query structures");
			}
			try {
				return compose(nested);
			} finally {
				// Runs on success or failure, including before returning. Removing the
				// query allows the same subquery to be reused in another branch.
				activeQueries.remove(select);
			}
		}

		/**
		 * Builds SELECT, FROM/JOIN, filters, grouping, ordering, and pagination as
		 * fragments. Derived sources are compiled first for column resolution, then
		 * their bindings are inserted at their actual position in the final SQL. The
		 * count plan is built from structured fragments rather than by parsing the
		 * completed SQL string.
		 *
		 * @param nested true for an embedded SELECT whose unpaged ordering can be
		 *               omitted
		 * @return data and count plans, each with bindings matching its SQL placeholder
		 *         order
		 * @throws IllegalArgumentException if query structure, aliases, or expressions
		 *                                  are unsupported
		 */
		private CompiledQuery compose(boolean nested) {
			Objects.requireNonNull(queryUtil, "query utility");
			if (select instanceof HardSelectQueryStruct || select.getCustomFrom() != null
					|| select.getCustomFromAliasName() != null || select.getSelectors().isEmpty()
					|| select.getQueryAll() || select.getFrame() != null || !select.getPragmap().isEmpty()
					|| select.from != null || select.body != null || select.filter != null
					|| !select.nselectors.isEmpty() || !select.joins.isEmpty() || !select.aliasHash.isEmpty()) {
				throw unsupported("raw, frame, query-all, or legacy query structure");
			}
			// Register derived sources before resolving any outer columns. Their SQL is
			// inserted with its bindings later, at its actual position in FROM.
			for (IRelation relation : select.getRelations()) {
				if (relation instanceof SubqueryRelationship subquery) {
					String name = identifier(subquery.getQueryAlias());
					if (derivedTables.containsKey(name)) {
						throw unsupported("duplicate derived-table aliases");
					}
					Compilation child = child(subquery.getQs());
					ParameterizedQuery query = child.compile(true).query();
					derivedTables.put(name, child);
					derivedQueries.put(name, new Fragment(query.sql(), query.parameters()));
				}
			}
			List<Fragment> selectors = new ArrayList<>();
			List<String> labels = new ArrayList<>();
			for (IQuerySelector selector : select.getSelectors()) {
				Fragment expression = projectedExpression(selector);
				String label = selector instanceof AbstractQuerySelector abstractSelector
						? abstractSelector.getExplicitAlias()
						: null;
				if (label == null || label.isEmpty()) {
					// Constant-derived aliases would leak values into the SQL template.
					label = expression.values.isEmpty() ? selector.getAlias() : "value_" + (selectors.size() + 1);
				}
				if (labels.contains(label)) {
					throw unsupported("duplicate result aliases");
				}
				labels.add(label);
				projections.put(label, selector);
				selectors.add(expression.then(" AS " + alias(label)));
			}
			Fragment where = filters(combinedFilters(), " AND ");
			List<Fragment> groups = new ArrayList<>();
			for (IQuerySelector group : select.getGroupBy()) {
				groups.add(expression(group));
			}
			Fragment having = filters(select.getHavingFilters().getFilters(), " AND ");
			List<Fragment> orders = new ArrayList<>();
			for (IQuerySort sort : select.getCombinedOrderBy()) {
				if (!(sort instanceof QueryColumnOrderBySelector column)) {
					throw unsupported("custom ordering");
				}
				String direction = column.getSortDirString();
				if (!Set.of("", "ASC", "DESC", "ASCENDING", "DESCENDING").contains(direction)) {
					throw unsupported("sort direction");
				}
				Fragment ordered = column.isPrimKeyColumn() && projections.containsKey(column.getTable())
						? Fragment.text(alias(column.getTable()))
						: expression(column);
				orders.add(ordered.then(" " + column.getSortDir().name()));
			}
			Fragment result = Fragment.text(select.isDistinct() ? "SELECT DISTINCT " : "SELECT ")
					.then(Fragment.join(selectors, ", ")).then(" FROM ").then(from());
			if (!where.sql.isEmpty()) {
				result = result.then(" WHERE ").then(where);
			}
			if (!groups.isEmpty()) {
				result = result.then(" GROUP BY ").then(Fragment.join(groups, ", "));
			}
			if (!having.sql.isEmpty()) {
				result = result.then(" HAVING ").then(having);
			}
			Fragment base = result;
			boolean paged = select.getLimit() > 0 || select.getOffset() > 0;
			if (!orders.isEmpty() && (!nested || paged)) {
				result = result.then(" ORDER BY ").then(Fragment.join(orders, ", "));
			}
			Fragment countBody = base;
			if (paged) {
				if (queryUtil.getDbType() == RdbmsTypeEnum.TERADATA || queryUtil.getDbType() == RdbmsTypeEnum.SYNAPSE) {
					countBody = windowPage(base, labels);
					result = nested ? countBody : countBody.then(" ORDER BY parameterized_row_number");
				} else {
					result = paginate(result, orders.isEmpty(), labels.get(0));
					countBody = result;
				}
			}
			Fragment count = Fragment.text("SELECT COUNT(*) FROM (").then(countBody).then(") parameterized_count");
			return new CompiledQuery(new ParameterizedQuery(result.sql, result.values, 0),
					new ParameterizedQuery(count.sql, count.values, 0));
		}

		/**
		 * Selects filters according to the query's precedence flags. Ignoring filters
		 * returns none; override mode excludes overlapping implicit filters and omits
		 * frame/panel filters. Otherwise all explicit, implicit, frame, and panel
		 * filters participate. The input filter collections and values are left
		 * unchanged.
		 *
		 * @return filters to combine with AND in the WHERE clause
		 */
		private List<IQueryFilter> combinedFilters() {
			if (select.ignoreFilters) {
				return List.of();
			}
			List<IQueryFilter> combined = new ArrayList<>(select.getExplicitFilters().getFilters());
			if (select.isOverrideImplicit()) {
				Set<String> explicitColumns = select.getExplicitFilters().getAllFilteredColumns();
				for (IQueryFilter filter : select.getImplicitFilters().getFilters()) {
					if (Collections.disjoint(filter.getAllUsedColumns(), explicitColumns)) {
						combined.add(filter);
					}
				}
			} else {
				combined.addAll(select.getImplicitFilters().getFilters());
				combined.addAll(select.getFrameImplicitFilters().getFilters());
				combined.addAll(select.getPanelImplicitFilters().getFilters());
			}
			return combined;
		}

		/**
		 * Creates an independent alias/metadata scope for an embedded query while
		 * sharing this compilation's active-query set. Outer table aliases are not
		 * inherited.
		 *
		 * @param query non-null subquery to compile
		 * @return a fresh child scope using the same engine and recursion guard
		 * @throws NullPointerException if the subquery is null
		 */
		private Compilation child(SelectQueryStruct query) {
			return new Compilation((IRDBMSEngine) engine, Objects.requireNonNull(query, "subquery"), activeQueries);
		}

		/**
		 * Appends the dialect's pagination syntax using the current SELECT's
		 * limit/offset. User-supplied bounds become bindings in the order required by
		 * the dialect. For offset-only queries, some dialects require an additional
		 * unlimited-limit value. Required fallback ordering uses the first projected
		 * label. Dialects requiring a ROW_NUMBER rewrite are handled separately by
		 * {@link #windowPage(Fragment, List)}.
		 *
		 * @param query      SELECT fragment before pagination, including any explicit
		 *                   ORDER BY
		 * @param needsOrder true when no explicit ORDER BY was rendered
		 * @param firstLabel first projected label to use when the dialect requires
		 *                   ordering
		 * @return the query with pagination SQL and its additional ordered bindings
		 */
		private Fragment paginate(Fragment query, boolean needsOrder, String firstLabel) {
			long limit = select.getLimit();
			long offset = Math.max(0, select.getOffset());
			RdbmsTypeEnum dialect = queryUtil.getDbType();
			if (dialect == RdbmsTypeEnum.TRINO || dialect == RdbmsTypeEnum.ATHENA) {
				if (offset > 0) {
					query = query.then(" OFFSET ").then(Fragment.value(offset));
				}
				return limit > 0 ? query.then(" LIMIT ").then(Fragment.value(limit)) : query;
			}
			if (dialect == RdbmsTypeEnum.HIVE) {
				query = query.then(" LIMIT ");
				if (offset > 0) {
					query = query.then(Fragment.value(offset)).then(", ");
				}
				return query.then(Fragment.value(limit > 0 ? limit : Long.MAX_VALUE));
			}
			if (dialect == RdbmsTypeEnum.SQL_SERVER) {
				if (needsOrder) {
					query = query.then(" ORDER BY " + alias(firstLabel));
				}
				query = query.then(" OFFSET ").then(Fragment.value(offset)).then(" ROWS");
				return limit > 0 ? query.then(" FETCH NEXT ").then(Fragment.value(limit)).then(" ROWS ONLY") : query;
			}
			if (dialect == RdbmsTypeEnum.ORACLE || dialect == RdbmsTypeEnum.DB2 || dialect == RdbmsTypeEnum.DERBY) {
				if (offset > 0) {
					query = query.then(" OFFSET ").then(Fragment.value(offset)).then(" ROWS");
				}
				return limit > 0 ? query.then(" FETCH NEXT ").then(Fragment.value(limit)).then(" ROWS ONLY") : query;
			}
			if (limit > 0) {
				if (dialect == RdbmsTypeEnum.IMPALA && offset > 0 && needsOrder) {
					query = query.then(" ORDER BY " + alias(firstLabel));
				}
				query = query.then(" LIMIT ").then(Fragment.value(limit));
			} else if (dialect == RdbmsTypeEnum.MYSQL || dialect == RdbmsTypeEnum.MARIADB) {
				query = query.then(" LIMIT 18446744073709551615");
			} else if (dialect == RdbmsTypeEnum.SQLITE) {
				query = query.then(" LIMIT -1");
			} else if (dialect == RdbmsTypeEnum.BIG_QUERY || dialect == RdbmsTypeEnum.SAP_HANA
					|| dialect == RdbmsTypeEnum.IMPALA) {
				if (dialect == RdbmsTypeEnum.IMPALA && needsOrder) {
					query = query.then(" ORDER BY " + alias(firstLabel));
				}
				query = query.then(" LIMIT ").then(Fragment.value(Long.MAX_VALUE));
			}
			return offset > 0 ? query.then(" OFFSET ").then(Fragment.value(offset)) : query;
		}

		/**
		 * Implements pagination for Teradata and Synapse using a derived ROW_NUMBER
		 * column and bound row-range predicates. Ordering references projected labels;
		 * the outer SELECT excludes the generated row number from the returned columns.
		 * Without an explicit sort, the first projected label supplies the window
		 * ordering.
		 *
		 * @param base   SELECT fragment before output ordering and pagination
		 * @param labels projected result labels in SELECT order
		 * @return the page-producing SELECT with the original bindings followed by row
		 *         bounds
		 * @throws IllegalArgumentException if a label is reserved or sorting uses an
		 *                                  unselected column
		 * @throws ArithmeticException      if offset plus limit overflows a long
		 */
		private Fragment windowPage(Fragment base, List<String> labels) {
			if (labels.contains("parameterized_row_number")) {
				throw unsupported("reserved pagination alias");
			}
			List<String> order = new ArrayList<>();
			for (IQuerySort sort : select.getCombinedOrderBy()) {
				QueryColumnOrderBySelector column = (QueryColumnOrderBySelector) sort;
				int position = column.isPrimKeyColumn() ? labels.indexOf(column.getTable()) : -1;
				for (int i = 0; position < 0 && i < select.getSelectors().size(); i++) {
					if (Objects.equals(select.getSelectors().get(i).getQueryStructName(),
							column.getQueryStructName())) {
						position = i;
						break;
					}
				}
				if (position < 0) {
					throw unsupported("pagination sorting by an unselected column on this dialect");
				}
				order.add("parameterized_source." + alias(labels.get(position)) + " " + column.getSortDir().name());
			}
			if (order.isEmpty()) {
				order.add("parameterized_source." + alias(labels.get(0)));
			}
			Fragment numbered = Fragment.text("SELECT parameterized_source.*, ROW_NUMBER() OVER (ORDER BY "
					+ String.join(", ", order) + ") AS parameterized_row_number FROM (").then(base)
					.then(") parameterized_source");
			List<String> projection = labels.stream().map(label -> "parameterized_page." + alias(label)).toList();
			Fragment result = Fragment.text("SELECT " + String.join(", ", projection) + " FROM (").then(numbered)
					.then(") parameterized_page WHERE parameterized_row_number > ")
					.then(Fragment.value(Math.max(0, select.getOffset())));
			if (select.getLimit() > 0) {
				result = result.then(" AND parameterized_row_number <= ")
						.then(Fragment.value(Math.addExact(Math.max(0, select.getOffset()), select.getLimit())));
			}
			return result;
		}

		/**
		 * Compiles a SELECT expression, adding an explicit type cast for H2 constant
		 * projections. H2 must resolve derived-table column types during preparation,
		 * before JDBC binds values; count queries also wrap the SELECT as a derived
		 * table. The cast supplies a type without embedding the constant value in SQL.
		 *
		 * @param selector expression to project before its result alias is appended
		 * @return the expression and bindings, with an H2 type hint where required
		 */
		private Fragment projectedExpression(IQuerySelector selector) {
			Fragment expression = expression(selector);
			if (queryUtil.getDbType() != RdbmsTypeEnum.H2_DB || !(selector instanceof QueryConstantSelector)) {
				return expression;
			}
			// H2 resolves derived-table columns during prepare, before JDBC can supply
			// parameter types. Count queries wrap projections in derived tables too.
			Object value = expression.values.getFirst();
			String type;
			if (value instanceof Boolean) {
				type = "BOOLEAN";
			} else if (value instanceof Byte || value instanceof Short || value instanceof Integer) {
				type = "INTEGER";
			} else if (value instanceof Long) {
				type = "BIGINT";
			} else if (value instanceof Float || value instanceof Double) {
				type = "DOUBLE";
			} else if (value instanceof BigDecimal decimal) {
				int scale = Math.max(0, decimal.scale());
				type = "DECIMAL(" + Math.max(1, Math.max(decimal.precision() - decimal.scale(), 0) + scale) + ","
						+ scale + ")";
			} else if (value instanceof java.sql.Date) {
				type = "DATE";
			} else if (value instanceof Timestamp) {
				type = "TIMESTAMP(9)";
			} else if (value instanceof OffsetDateTime) {
				type = "TIMESTAMP(9) WITH TIME ZONE";
			} else {
				type = "VARCHAR";
			}
			return Fragment.text("CAST(").then(expression).then(" AS " + type + ")");
		}

		/**
		 * Recursively renders a selector as an SQL expression without its result alias.
		 * Columns resolve through physical metadata or a child query's projected
		 * labels; bare concepts retain the legacy primary-key mapping. Constants become
		 * bindings, and supported arithmetic, CASE, and function expressions compose
		 * child fragments in SQL order. Function names, options, and operators are
		 * validated as structure.
		 *
		 * @param selector column, constant, arithmetic, conditional, or supported
		 *                 function selector
		 * @return the SQL expression and its ordered parameter values
		 * @throws IllegalArgumentException if a selector, identifier, function, or
		 *                                  option is unsupported
		 */
		private Fragment expression(IQuerySelector selector) {
			if (selector instanceof QueryColumnSelector column) {
				if (column.getTableAlias() != null && !column.getTableAlias().isEmpty()) {
					throw unsupported("explicit table aliases");
				}
				String table = table(column.getTable());
				Compilation derived = derivedTables.get(column.getTable());
				if (derived != null) {
					if (!derived.projections.containsKey(column.getColumn())) {
						throw unsupported("unknown derived-table column " + column.getColumn());
					}
					return Fragment.text(table + "." + alias(column.getColumn()));
				}
				String name = column.isPrimKeyColumn() ? getPrimKey4Table(column.getTable())
						: getPhysicalPropertyNameFromConceptualName(column.getTable(), identifier(column.getColumn()));
				return Fragment.text(table + "." + sqlIdentifier(name));
			}
			if (selector instanceof QueryConstantSelector constant) {
				return Fragment.value(value(constant.getConstant(), null));
			}
			if (selector instanceof QueryArithmeticSelector arithmetic) {
				String operator = arithmetic.getMathExpr();
				if (!Set.of("+", "-", "*", "/").contains(operator)) {
					throw unsupported("arithmetic operator");
				}
				Fragment left = expression(arithmetic.getLeftSelector());
				Fragment right = expression(arithmetic.getRightSelector());
				if (operator.equals("/")) {
					right = Fragment.text("NULLIF(").then(right).then(", 0)");
				}
				return Fragment.text("(CAST(").then(left).then(" AS DECIMAL) " + operator + " CAST(").then(right)
						.then(" AS DECIMAL))");
			}
			if (selector instanceof QueryIfSelector conditional) {
				Fragment result = Fragment.text("CASE WHEN ").then(filter(conditional.getCondition())).then(" THEN ")
						.then(expression(conditional.getPrecedent()));
				if (conditional.getAntecedent() != null) {
					result = result.then(" ELSE ").then(expression(conditional.getAntecedent()));
				}
				return result.then(" END");
			}
			if (selector instanceof QueryFunctionSelector function) {
				String name = Objects.requireNonNull(function.getFunction(), "function").toUpperCase(Locale.ROOT);
				boolean distinct = function.isDistinct() || name.startsWith("UNIQUE");
				if (name.startsWith("UNIQUE")) {
					name = name.substring("UNIQUE".length());
				}
				if (name.equals("MEAN") || name.equals("AVERAGE")) {
					name = "AVG";
				}
				if (!Set.of("COUNT", "SUM", "MIN", "MAX", "AVG", "LOWER", "UPPER", "COALESCE", "CAST", "REGEXLIKE",
						"PATINDEX").contains(name) || !function.getAdditionalFunctionParams().isEmpty()
						|| (function.getColCast() != null && !function.getColCast().isEmpty())
						|| function.getSeparator() != null) {
					throw unsupported("function or function options");
				}
				int size = function.getInnerSelector().size();
				boolean validArity = switch (name) {
				case "COALESCE" -> size >= 2;
				case "REGEXLIKE" -> size == 2 || size == 3;
				case "PATINDEX" -> size == 2;
				default -> size == 1;
				};
				if (!validArity || (distinct
						&& Set.of("LOWER", "UPPER", "COALESCE", "CAST", "REGEXLIKE", "PATINDEX").contains(name))) {
					throw unsupported("function arity or distinct option");
				}
				List<Fragment> arguments = new ArrayList<>();
				for (IQuerySelector argument : function.getInnerSelector()) {
					arguments.add(expression(argument));
				}
				if (name.equals("CAST")) {
					return Fragment.text("CAST(").then(arguments.getFirst())
							.then(" AS " + castType(function.getDataType()) + ")");
				}
				return Fragment.text(queryUtil.getSqlFunctionSyntax(name) + "(" + (distinct ? "DISTINCT " : ""))
						.then(Fragment.join(arguments, ", ")).then(")");
			}
			throw unsupported("selector " + selector.getSelectorType());
		}

		/**
		 * Normalizes and validates a CAST target against the supported SQL type
		 * spellings and optional numeric size/precision arguments. Type names are SQL
		 * structure and cannot be represented by JDBC value placeholders. The database
		 * still validates whether a supported spelling and its numeric arguments are
		 * valid for that dialect.
		 *
		 * @param type non-null requested SQL type, such as VARCHAR or DECIMAL(18, 2)
		 * @return the trimmed, upper-case type expression
		 * @throws NullPointerException     if the type is null
		 * @throws IllegalArgumentException if the type spelling or argument syntax is
		 *                                  unsupported
		 */
		private String castType(String type) {
			String normalized = Objects.requireNonNull(type, "cast type").trim().toUpperCase(Locale.ROOT);
			if (!normalized.matches(
					"(?:INT|INTEGER|SMALLINT|BIGINT|INT64|FLOAT64|DECIMAL|NUMERIC|NUMBER|REAL|FLOAT|DOUBLE|BOOLEAN|BOOL|BIT|DATE|TIMESTAMP|DATETIME|DATETIME2|TEXT|STRING|CHAR|VARCHAR|VARCHAR2|NVARCHAR)(?:\\([0-9]+(?:\\s*,\\s*[0-9]+)?\\))?")) {
				throw unsupported("cast type");
			}
			return normalized;
		}

		/**
		 * Compiles filters in list order and joins them into a clause body. No WHERE or
		 * HAVING keyword is added; an empty list produces an empty fragment.
		 *
		 * @param filters   predicates to compile in their existing order
		 * @param separator compiler-supplied SQL connector, usually AND or OR with
		 *                  spaces
		 * @return the clause body and its ordered bindings
		 */
		private Fragment filters(List<IQueryFilter> filters, String separator) {
			List<Fragment> parts = new ArrayList<>();
			for (IQueryFilter filter : filters) {
				parts.add(filter(filter));
			}
			return Fragment.join(parts, separator);
		}

		/**
		 * Compiles one predicate, recursively handling boolean groups, BETWEEN,
		 * supported boolean functions, and comparisons. Literal-left comparisons are
		 * reversed so the column can be processed on the left. Single-column subqueries
		 * receive their own compilation scope; scalar values are bound separately from
		 * SQL structure. Column-to-value membership and search semantics are delegated
		 * to {@link #membership(IQuerySelector, String, NounMetadata)}.
		 *
		 * @param filter structured predicate to compile
		 * @return predicate SQL with values in placeholder order
		 * @throws IllegalArgumentException if the predicate, comparator, or operands
		 *                                  are unsupported
		 */
		private Fragment filter(IQueryFilter filter) {
			if (filter instanceof AndQueryFilter and) {
				return group(and.getFilterList(), " AND ", true);
			}
			if (filter instanceof OrQueryFilter or) {
				return group(or.getFilterList(), " OR ", false);
			}
			if (filter instanceof BetweenQueryFilter between) {
				SemossDataType type = dataType(between.getColumn());
				return expression(between.getColumn()).then(" BETWEEN ")
						.then(Fragment.value(value(between.getStart(), type))).then(" AND ")
						.then(Fragment.value(value(between.getEnd(), type)));
			}
			if (filter instanceof FunctionQueryFilter function) {
				if (!"RegexLike".equalsIgnoreCase(function.getFunctionSelector().getFunction())) {
					throw unsupported("non-boolean filter function");
				}
				return expression(function.getFunctionSelector());
			}
			if (!(filter instanceof SimpleQueryFilter simple)) {
				throw unsupported("filter " + filter.getQueryFilterType());
			}
			NounMetadata left = simple.getLComparison();
			NounMetadata right = simple.getRComparison();
			String comparator = Objects.requireNonNull(simple.getComparator(), "comparator").trim();
			// Use current noun types, not SimpleQueryFilter's potentially cached type.
			if (left.getNounType() == PixelDataType.COLUMN && right.getNounType() == PixelDataType.COLUMN) {
				return expression((IQuerySelector) left.getValue()).then(" " + comparison(comparator) + " ")
						.then(expression((IQuerySelector) right.getValue()));
			}
			if (right.getNounType() == PixelDataType.COLUMN && left.getNounType() != PixelDataType.COLUMN) {
				NounMetadata swap = left;
				left = right;
				right = swap;
				comparator = IQueryFilter.getReverseNumericalComparator(comparator);
			}
			if (left.getNounType() == PixelDataType.COLUMN && right.getNounType() == PixelDataType.QUERY_STRUCT) {
				SelectQueryStruct nested = (SelectQueryStruct) right.getValue();
				if (nested.getSelectors().size() != 1) {
					throw unsupported("subquery filters with multiple projected columns");
				}
				ParameterizedQuery query = child(nested).compile(true).query();
				String operator = switch (comparator) {
				case "=", "==" -> "IN";
				case "!=", "<>" -> "NOT IN";
				default -> comparison(comparator);
				};
				return expression((IQuerySelector) left.getValue()).then(" " + operator + " (")
						.then(new Fragment(query.sql(), query.parameters())).then(")");
			}
			if (isScalar(left) && isScalar(right)) {
				return Fragment
						.value(value(left.getValue(), SemossDataType.convertFromSemossDataType(left.getNounType())))
						.then(" " + comparison(comparator) + " ").then(Fragment.value(value(right.getValue(),
								SemossDataType.convertFromSemossDataType(right.getNounType()))));
			}
			if (left.getNounType() == PixelDataType.COLUMN && Set.of("~", "!~", "~*", "!~*").contains(comparator)) {
				if (queryUtil.getDbType() != RdbmsTypeEnum.POSTGRES || !isScalar(right)) {
					throw unsupported("regex comparator for this dialect or operand");
				}
				return expression((IQuerySelector) left.getValue()).then(" " + comparator + " ")
						.then(Fragment.value(value(right.getValue(), SemossDataType.STRING)));
			}
			if (left.getNounType() != PixelDataType.COLUMN
					|| SemossDataType.convertFromSemossDataType(right.getNounType()) == null
							&& right.getNounType() != PixelDataType.NULL_VALUE) {
				throw unsupported("subquery, task, lambda, or non-scalar filter operands");
			}
			return membership((IQuerySelector) left.getValue(), comparator, right);
		}

		/**
		 * Checks whether a noun's declared type represents a scalar value or null. This
		 * classifies the operand type only; value conversion separately validates the
		 * actual Java object before creating a binding.
		 *
		 * @param noun operand whose current noun type is inspected
		 * @return true for a recognized scalar type or explicit null type
		 */
		private boolean isScalar(NounMetadata noun) {
			return noun.getNounType() == PixelDataType.NULL_VALUE
					|| SemossDataType.convertFromSemossDataType(noun.getNounType()) != null;
		}

		/**
		 * Compiles a parenthesized boolean group. Empty AND groups are true and empty
		 * OR groups are false, expressed as constant SQL predicates without bindings.
		 *
		 * @param children    predicates within the group
		 * @param separator   compiler-supplied AND or OR connector, including spaces
		 * @param emptyResult truth value to render when there are no child predicates
		 * @return the grouped predicate or its empty-group truth value
		 */
		private Fragment group(List<IQueryFilter> children, String separator, boolean emptyResult) {
			return children.isEmpty() ? Fragment.text(emptyResult ? "1=1" : "1=0")
					: Fragment.text("(").then(filters(children, separator)).then(")");
		}

		/**
		 * Compiles a column/expression comparison against one value or a collection.
		 * Equality uses IN/NOT IN; ordered comparisons require one non-null value;
		 * search comparators produce bound LIKE patterns with dialect-specific pattern
		 * handling. Search normalization changes the bound pattern without
		 * interpolating it into SQL.
		 *
		 * <p>
		 * Nulls become IS NULL/IS NOT NULL conditions, including legacy null-token
		 * handling for non-string metadata types. Empty collections have explicit
		 * true/false semantics rather than generating empty IN lists. Mixed null/value
		 * predicates combine the null check with the non-null comparison. Multiple
		 * search patterns retain the existing interpreter's OR semantics, including
		 * negative searches.
		 *
		 * @param selector   left-hand column or expression
		 * @param comparator equality, ordered, or supported search comparator
		 * @param noun       right-hand scalar or collection, including its declared
		 *                   type
		 * @return the combined predicate and normalized ordered bindings
		 * @throws IllegalArgumentException if values cannot be converted or the
		 *                                  comparison is unsupported
		 */
		private Fragment membership(IQuerySelector selector, String comparator, NounMetadata noun) {
			boolean search = getAllSearchComparators().contains(comparator);
			boolean negative = search ? getNegSearchComparators().contains(comparator)
					: comparator.equals("!=") || comparator.equals("<>");
			boolean equality = comparator.equals("==") || comparator.equals("=") || negative;
			if (!search) {
				comparison(comparator);
			}
			Fragment column = expression(selector);
			SemossDataType type = dataType(selector);
			SemossDataType bindingType = type == null ? SemossDataType.convertFromSemossDataType(noun.getNounType())
					: type;
			Collection<?> inputs = noun.getValue() instanceof Collection<?> values ? values
					: Collections.singletonList(noun.getValue());
			List<Object> values = new ArrayList<>();
			boolean includeNull = false;
			for (Object input : inputs) {
				boolean nullToken = type != null && SemossDataType.isNotString(type)
						&& ("null".equals(input) || "nan".equals(input)
								|| ((comparator.equals("==") || comparator.equals("=")) && "".equals(input)));
				if (input == null || nullToken) {
					includeNull = true;
				} else {
					Object normalized = value(input, search ? null : bindingType);
					values.add(normalized);
					if (search && Set.of("n", "nu", "nul", "null").contains(normalized)) {
						includeNull = true;
					}
				}
			}
			if (includeNull && !search && !equality) {
				throw unsupported("null with an ordered comparator");
			}
			Fragment predicate;
			if (values.isEmpty()) {
				predicate = Fragment.text(negative ? "1=1" : "1=0");
			} else if (search) {
				List<Fragment> patterns = new ArrayList<>();
				for (Object input : values) {
					String pattern = input.toString().toLowerCase(Locale.ROOT);
					// These dialects interpret backslash as LIKE's default escape character.
					if (switch (queryUtil.getDbType()) {
					case H2_DB, POSTGRES, MYSQL, MARIADB, REDSHIFT, BIG_QUERY, HIVE, IMPALA, SPARK, DATABRICKS -> true;
					default -> false;
					}) {
						pattern = pattern.replace("\\", "\\\\");
					}
					if (comparator.equals("?like") || comparator.equals("?nlike") || comparator.endsWith("ends")) {
						pattern = "%" + pattern;
					}
					if (comparator.equals("?like") || comparator.equals("?nlike") || comparator.endsWith("begins")) {
						pattern += "%";
					}
					Fragment operand = type == SemossDataType.STRING || type == SemossDataType.FACTOR ? column
							: Fragment.text("CAST(").then(column).then(" AS " + searchCastType() + ")");
					patterns.add(Fragment.text("LOWER(").then(operand).then(negative ? ") NOT LIKE " : ") LIKE ")
							.then(Fragment.value(pattern)));
				}
				// Preserve the existing interpreter's OR semantics for multiple patterns.
				predicate = Fragment.text("(").then(Fragment.join(patterns, " OR ")).then(")");
			} else if (equality) {
				List<Fragment> parameters = values.stream().map(Fragment::value).toList();
				predicate = column.then(negative ? " NOT IN (" : " IN (").then(Fragment.join(parameters, ", "))
						.then(")");
			} else {
				if (values.size() != 1) {
					throw unsupported("multiple values with an ordered comparator");
				}
				predicate = column.then(" " + comparison(comparator) + " ").then(Fragment.value(values.get(0)));
			}
			if (!includeNull) {
				return predicate;
			}
			Fragment nullCheck = column.then(negative ? " IS NOT NULL" : " IS NULL");
			return values.isEmpty() ? nullCheck
					: Fragment.text("(").then(nullCheck).then(negative ? " AND " : " OR ").then(predicate).then(")");
		}

		/**
		 * Determines the type used to normalize values compared with a selector.
		 * Derived columns resolve through their child's projected selector. Other
		 * selectors use an explicit type when present, then non-basic engine metadata
		 * for columns, including the legacy primary-key mapping for bare concepts.
		 *
		 * @param selector expression whose comparison-value type is needed
		 * @return the resolved SEMOSS type, or null when no recognized type is
		 *         available
		 */
		private SemossDataType dataType(IQuerySelector selector) {
			if (selector instanceof QueryColumnSelector column && derivedTables.containsKey(column.getTable())) {
				Compilation derived = derivedTables.get(column.getTable());
				IQuerySelector projected = derived.projections.get(column.getColumn());
				return projected == null ? null : derived.dataType(projected);
			}
			String type = selector.getDataType();
			if (type == null && selector instanceof QueryColumnSelector column && !engine.isBasic()) {
				String property = column.isPrimKeyColumn() ? getPrimKey4Table(column.getTable())
						: getPhysicalPropertyNameFromConceptualName(column.getTable(), column.getColumn());
				String table = getPhysicalTableNameFromConceptualName(column.getTable());
				type = engine.getDataTypes("http://semoss.org/ontologies/Concept/" + property + "/" + table);
				if (type == null) {
					type = engine
							.getDataTypes("http://semoss.org/ontologies/Relation/Contains/" + property + "/" + table);
				}
			}
			return SemossDataType.convertStringToDataType(type);
		}

		/**
		 * Registers a source on first use and returns its generated alias in this
		 * scope. Ordinary tables also resolve to validated physical paths; derived
		 * sources keep their compiled child query instead. Registration does not append
		 * SQL or bindings.
		 *
		 * @param name conceptual table name or registered derived-source name
		 * @return the stable generated alias for this scope, such as t0
		 * @throws IllegalArgumentException if a conceptual or physical identifier is
		 *                                  unsupported
		 */
		private String table(String name) {
			if (!tableAliases.containsKey(name)) {
				qualifiedIdentifier(name);
				if (!derivedTables.containsKey(name)) {
					physicalTables.put(name, qualifiedIdentifier(getPhysicalTableNameFromConceptualName(name)));
				}
				tableAliases.put(name, "t" + tableAliases.size());
			}
			return tableAliases.get(name);
		}

		/**
		 * Renders a FROM/JOIN source with its generated alias. A physical source
		 * contributes only table text; a derived source contributes its parenthesized
		 * SELECT and all of that child query's bindings at this position in the parent
		 * SQL.
		 *
		 * @param name conceptual table name or registered derived-source name
		 * @return the source declaration and any nested-query bindings
		 */
		private Fragment tableDeclaration(String name) {
			String tableAlias = table(name);
			Fragment derived = derivedQueries.get(name);
			return derived == null ? Fragment.text(physicalTables.get(name) + " " + tableAlias)
					: Fragment.text("(").then(derived).then(") " + tableAlias);
		}

		/**
		 * Parses an explicit join endpoint. This compiler requires both table and
		 * column; it does not infer a join column from a bare table name or OWL
		 * relationship. Identifier validation occurs when the resulting selector is
		 * rendered.
		 *
		 * @param endpoint join endpoint in table__column form
		 * @return a column selector for the endpoint
		 * @throws IllegalArgumentException if the endpoint is null or lacks the
		 *                                  required two-part form
		 */
		private QueryColumnSelector joinColumn(String endpoint) {
			if (endpoint == null || endpoint.split("__", -1).length != 2) {
				throw unsupported("inferred joins; specify table__column endpoints");
			}
			return new QueryColumnSelector(endpoint);
		}

		/**
		 * Normalizes supported join spellings, including Pixel's dot-separated form,
		 * into SQL keywords. Currently accepts inner, left, and right joins.
		 *
		 * @param type non-null join spelling from the relationship
		 * @return INNER JOIN, LEFT JOIN, or RIGHT JOIN
		 * @throws NullPointerException     if the join type is null
		 * @throws IllegalArgumentException if the join type is unsupported
		 */
		private String joinType(String type) {
			return switch (Objects.requireNonNull(type, "join type").toLowerCase(Locale.ROOT).replace('.', ' ')
					.trim()) {
			case "inner", "inner join" -> "INNER JOIN";
			case "left", "left outer", "left join", "left outer join" -> "LEFT JOIN";
			case "right", "right outer", "right join", "right outer join" -> "RIGHT JOIN";
			default -> throw unsupported("join type");
			};
		}

		/**
		 * Builds the FROM/JOIN clause body from registered sources and explicit
		 * relations. Relations must form an ordered tree: each join connects an
		 * included source to a new source. Derived-table joins can contribute several
		 * AND-connected conditions. Their child bindings are inserted with the source
		 * before the ON-clause bindings. Unjoined sources, repeated sources, and
		 * unsupported relationship shapes fail instead of producing an implicit
		 * Cartesian product or inferred join.
		 *
		 * @return the clause body without the FROM keyword, with bindings in SQL order
		 * @throws IllegalArgumentException if sources or relationships cannot form the
		 *                                  supported join tree
		 */
		private Fragment from() {
			List<IRelation> joins = new ArrayList<>(select.getRelations());
			if (joins.isEmpty()) {
				if (tableAliases.size() != 1) {
					throw unsupported("missing join between tables");
				}
				return tableDeclaration(tableAliases.keySet().iterator().next());
			}
			String root;
			IRelation first = joins.getFirst();
			if (first instanceof BasicRelationship basic) {
				root = joinColumn(basic.getFromConcept()).getTable();
			} else if (first instanceof SubqueryRelationship subquery && !subquery.getJoinOnDetails().isEmpty()) {
				String[] on = subquery.getJoinOnDetails().getFirst();
				if (on.length != 3) {
					throw unsupported("join condition");
				}
				String left = joinColumn(on[0]).getTable();
				root = left.equals(subquery.getQueryAlias()) ? joinColumn(on[1]).getTable() : left;
			} else {
				throw unsupported("relationship without join conditions");
			}
			Set<String> included = new LinkedHashSet<>();
			included.add(root);
			Fragment result = tableDeclaration(root);
			for (IRelation relation : joins) {
				String joinedTable;
				String kind;
				List<Fragment> conditions = new ArrayList<>();
				if (relation instanceof BasicRelationship basic) {
					QueryColumnSelector left = joinColumn(basic.getFromConcept());
					QueryColumnSelector right = joinColumn(basic.getToConcept());
					if (!included.contains(left.getTable()) || included.contains(right.getTable())) {
						throw unsupported("joins must form an ordered tree without self joins");
					}
					joinedTable = right.getTable();
					kind = joinType(basic.getJoinType());
					conditions.add(expression(left)
							.then(" " + comparison(basic.getComparator() == null ? "=" : basic.getComparator()) + " ")
							.then(expression(right)));
				} else if (relation instanceof SubqueryRelationship subquery) {
					joinedTable = subquery.getQueryAlias();
					kind = joinType(subquery.getJoinType());
					if (included.contains(joinedTable) || subquery.getJoinOnDetails().isEmpty()) {
						throw unsupported("repeated derived-table alias or missing join condition");
					}
					for (String[] on : subquery.getJoinOnDetails()) {
						if (on.length != 3) {
							throw unsupported("join condition");
						}
						QueryColumnSelector left = joinColumn(on[0]);
						QueryColumnSelector right = joinColumn(on[1]);
						if (!(left.getTable().equals(joinedTable) && included.contains(right.getTable()))
								&& !(right.getTable().equals(joinedTable) && included.contains(left.getTable()))) {
							throw unsupported("disconnected derived-table join");
						}
						conditions.add(expression(left).then(" " + comparison(on[2] == null ? "=" : on[2]) + " ")
								.then(expression(right)));
					}
				} else {
					throw unsupported("relationship type");
				}
				included.add(joinedTable);
				result = result.then(" " + kind + " ").then(tableDeclaration(joinedTable)).then(" ON ")
						.then(Fragment.join(conditions, " AND "));
			}
			if (!included.containsAll(tableAliases.keySet())) {
				throw unsupported("unjoined table");
			}
			return result;
		}

		/**
		 * Validates and renders a dot-separated table path. Ordinary dialects validate
		 * each segment and quote keywords as needed. BigQuery permits dashes in path
		 * segments and quotes the entire project/dataset/table path with backticks.
		 *
		 * @param name non-null conceptual or physical table path
		 * @return the validated path in dialect-appropriate identifier syntax
		 * @throws NullPointerException     if the name is null
		 * @throws IllegalArgumentException if any segment has unsupported characters or
		 *                                  is empty
		 */
		private String qualifiedIdentifier(String name) {
			Objects.requireNonNull(name, "identifier");
			if (queryUtil.getDbType() == RdbmsTypeEnum.BIG_QUERY) {
				// Project IDs commonly contain dashes; quote the complete table path.
				for (String part : name.split("\\.", -1)) {
					if (!part.matches("[A-Za-z_][A-Za-z0-9_$-]*")) {
						throw unsupported("BigQuery table path");
					}
				}
				return "`" + name + "`";
			}
			List<String> parts = new ArrayList<>();
			for (String part : name.split("\\.", -1)) {
				parts.add(sqlIdentifier(part));
			}
			return String.join(".", parts);
		}

		/**
		 * Validates one unqualified identifier and quotes it when the query utility
		 * marks it as a keyword. This accepts SQL structure only, not arbitrary SQL
		 * expressions.
		 *
		 * @param name column or table-path segment to render
		 * @return the validated identifier, quoted when required for a keyword
		 * @throws IllegalArgumentException if the identifier has an unsupported
		 *                                  spelling
		 */
		private String sqlIdentifier(String name) {
			identifier(name);
			return queryUtil.isSelectorKeyword(name) ? alias(name) : name;
		}

		/**
		 * Validates and quotes one result alias for use in projections and references.
		 * Uses explicit backticks for dialects that require them and the query
		 * utility's quoting convention for the remaining dialects.
		 *
		 * @param name unqualified alias to render
		 * @return the quoted alias
		 * @throws IllegalArgumentException if the alias has an unsupported spelling
		 */
		private String alias(String name) {
			identifier(name);
			return switch (queryUtil.getDbType()) {
			case BIG_QUERY, HIVE, IMPALA, SPARK, DATABRICKS -> "`" + name + "`";
			default -> queryUtil.getEscapeKeyword(name);
			};
		}

		/**
		 * Chooses the dialect's text type for applying LIKE to a non-string expression.
		 * The returned text is compiler-selected SQL structure, not a bound value.
		 *
		 * @return a text CAST target appropriate to the current dialect
		 */
		private String searchCastType() {
			return switch (queryUtil.getDbType()) {
			case BIG_QUERY, HIVE, IMPALA, SPARK, DATABRICKS -> "STRING";
			case MYSQL, MARIADB -> "CHAR";
			case SQL_SERVER, SYNAPSE -> "NVARCHAR(4000)";
			default -> queryUtil.getVarcharDataTypeName();
			};
		}
	}

	/**
	 * Validates a single simple identifier before it is used as SQL structure.
	 * Names must begin with a letter or underscore, followed by letters, digits,
	 * underscores, or dollar signs. Qualified paths are validated one segment at a
	 * time elsewhere.
	 *
	 * @param name unquoted schema, table, column, or alias segment
	 * @return the supplied name unchanged after validation
	 * @throws IllegalArgumentException if the name is null or has unsupported
	 *                                  characters
	 */
	private static String identifier(String name) {
		if (name == null || !name.matches("[A-Za-z_][A-Za-z0-9_$]*")) {
			throw unsupported("identifier; use simple schema, table, column, and alias names");
		}
		return name;
	}

	/**
	 * Maps supported equality and ordered comparators to SQL operators. Pixel
	 * equality and inequality spellings normalize to SQL equivalents. Search/regex
	 * comparators are handled separately when compiling filters.
	 *
	 * @param comparator equality or ordered comparison operator
	 * @return the normalized SQL operator
	 * @throws IllegalArgumentException if the operator is unsupported
	 */
	private static String comparison(String comparator) {
		return switch (comparator) {
		case "=", "==" -> "=";
		case "!=", "<>" -> "<>";
		case "<", "<=", ">", ">=" -> comparator;
		default -> throw unsupported("comparator");
		};
	}

	/**
	 * Normalizes one value for JDBC binding without SQL escaping. Enums use their
	 * text; a type hint converts textual numbers, booleans, dates, and timestamps.
	 * Supported Java/SEMOSS temporal objects become JDBC values, and BigInteger
	 * becomes BigDecimal. Collections are expanded by the caller, not bound as
	 * scalar objects.
	 *
	 * <p>
	 * ZonedDateTime and SemossDate conversions retain local date/time fields;
	 * OffsetDateTime retains its offset, and Instant is converted with
	 * Timestamp.from. Copies of supported mutable JDBC date/timestamp inputs are
	 * made during this normalization, independently of the later parameter-list
	 * snapshot.
	 *
	 * @param input scalar to normalize; null remains null
	 * @param type  optional comparison type used for textual and SEMOSS date
	 *              conversion
	 * @return a supported JDBC value, with untyped strings left unescaped
	 * @throws IllegalArgumentException if the value is unsupported or cannot be
	 *                                  converted to its type
	 */
	private static Object value(Object input, SemossDataType type) {
		if (input == null) {
			return null;
		}
		if (input instanceof Enum<?> enumeration) {
			input = enumeration.toString();
		}
		if (input instanceof String text && type != null) {
			return switch (type) {
			case INT -> Long.valueOf(text);
			case DOUBLE -> new BigDecimal(text);
			case BOOLEAN -> {
				if (!text.equalsIgnoreCase("true") && !text.equalsIgnoreCase("false")) {
					throw new IllegalArgumentException("Expected a boolean filter value");
				}
				yield Boolean.valueOf(text);
			}
			case DATE -> java.sql.Date.valueOf(text);
			case TIMESTAMP -> Timestamp.valueOf(text.replace('T', ' '));
			default -> text;
			};
		}
		if (input instanceof SemossDate date) {
			LocalDateTime local = Objects.requireNonNull(date.getZonedDateTime(), "date").toLocalDateTime();
			return type == SemossDataType.DATE
					|| type == null && !date.patternHasTime() && local.toLocalTime().equals(LocalTime.MIDNIGHT)
							? java.sql.Date.valueOf(local.toLocalDate())
							: Timestamp.valueOf(local);
		}
		if (input instanceof Timestamp timestamp) {
			return Timestamp.valueOf(timestamp.toLocalDateTime());
		}
		if (input instanceof java.sql.Date date) {
			return java.sql.Date.valueOf(date.toLocalDate());
		}
		if (input instanceof LocalDate date) {
			return java.sql.Date.valueOf(date);
		}
		if (input instanceof LocalDateTime date) {
			return Timestamp.valueOf(date);
		}
		if (input instanceof ZonedDateTime date) {
			return Timestamp.valueOf(date.toLocalDateTime());
		}
		if (input instanceof OffsetDateTime date) {
			return date;
		}
		if (input instanceof Instant instant) {
			return Timestamp.from(instant);
		}
		if (input instanceof java.util.Date date) {
			return new Timestamp(date.getTime());
		}
		if (input instanceof BigInteger integer) {
			return new BigDecimal(integer);
		}
		if (input instanceof String || input instanceof Boolean || input instanceof Byte || input instanceof Short
				|| input instanceof Integer || input instanceof Long || input instanceof Float
				|| input instanceof Double || input instanceof BigDecimal) {
			return input;
		}
		throw unsupported("non-scalar parameter value");
	}

	/**
	 * Creates a consistent compilation error for a feature that cannot be rendered
	 * through the prepared-query path. Callers throw the result rather than falling
	 * back to literal SQL generation.
	 *
	 * @param feature description of the unsupported query feature or value
	 * @return the exception to throw
	 */
	private static IllegalArgumentException unsupported(String feature) {
		return new IllegalArgumentException("Parameterized SQL does not support " + feature);
	}
}
