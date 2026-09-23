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
package prerna.engine.impl.guardrail;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.GuardrailTypeEnum;
import prerna.engine.api.IEngine;
import prerna.engine.impl.function.FunctionParameter;
import prerna.logging.IgnoreEngineLogging;
import prerna.om.Insight;
import prerna.query.parsers.SqlQueryPolicyParser;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.nounmeta.GuardrailNounMetadata;
import prerna.util.Constants;

/**
 * SQL authorization guardrail driven by JSQLParser's statement and expression
 * AST. Policies can require read, edit, or owner access for operation groups or
 * individual operations and can independently govern functions, variables,
 * relations, routines, parser failures, and structural query risks.
 *
 * <p>
 * The secure defaults require database ownership for {@code DELETE}, all DDL
 * (including {@code TRUNCATE}), routine execution, session changes, metadata
 * statements, recursive CTEs, and row locks. Ordinary writes require edit
 * access and reads require view access. Unparseable and stacked statements, and
 * {@code UPDATE}/{@code DELETE} without a {@code WHERE}, are denied.
 * </p>
 *
 * <p>
 * SQL is never classified with keyword or regular-expression parsing. If the
 * bundled JSQLParser version cannot parse a vendor extension, the configured
 * {@link #PARSER_FAILURE_POLICY_KEY} decides whether it is denied, restricted
 * to an owner, or allowed.
 * </p>
 */
public class SqlQueryGuardrailEngine extends AbstractGuardrailReactorFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(SqlQueryGuardrailEngine.class);

	public static final String SQL_DIALECT_KEY = "SQL_DIALECT";
	public static final String SQUARE_BRACKET_QUOTATION_KEY = "SQUARE_BRACKET_QUOTATION";
	public static final String PARSER_FAILURE_POLICY_KEY = "PARSER_FAILURE_POLICY";
	public static final String MULTI_STATEMENT_POLICY_KEY = "MULTI_STATEMENT_POLICY";
	public static final String OPERATION_POLICY_KEY = "OPERATION_POLICY";
	public static final String FUNCTION_POLICY_KEY = "FUNCTION_POLICY";
	public static final String VARIABLE_POLICY_KEY = "VARIABLE_POLICY";
	public static final String KEYWORD_POLICY_KEY = "KEYWORD_POLICY";
	public static final String RELATION_POLICY_KEY = "RELATION_POLICY";
	public static final String ROUTINE_POLICY_KEY = "ROUTINE_POLICY";
	public static final String PROTECT_UNMATCHED_IDENTIFIERS_KEY = "PROTECT_UNMATCHED_IDENTIFIERS";
	public static final String DELETE_WITHOUT_WHERE_POLICY_KEY = "DELETE_WITHOUT_WHERE_POLICY";
	public static final String UPDATE_WITHOUT_WHERE_POLICY_KEY = "UPDATE_WITHOUT_WHERE_POLICY";
	public static final String SELECT_STAR_POLICY_KEY = "SELECT_STAR_POLICY";
	public static final String CARTESIAN_JOIN_POLICY_KEY = "CARTESIAN_JOIN_POLICY";
	public static final String RECURSIVE_CTE_POLICY_KEY = "RECURSIVE_CTE_POLICY";
	public static final String JOIN_LIMIT_POLICY_KEY = "JOIN_LIMIT_POLICY";
	public static final String MAX_JOINS_KEY = "MAX_JOINS";
	public static final String MAX_QUERY_LENGTH_KEY = "MAX_QUERY_LENGTH";

	private static final String QUERY_PARAM = "query";
	private static final String DATABASE_ID_PARAM = "databaseId";

	private final Map<String, Requirement> defaultFunctionPolicy = new LinkedHashMap<>();
	private final Map<String, Requirement> defaultVariablePolicy = new LinkedHashMap<>();
	private final Map<String, Requirement> defaultKeywordPolicy = new LinkedHashMap<>();
	private final Map<String, Requirement> defaultRelationPolicy = new LinkedHashMap<>();
	private final Map<String, Requirement> defaultRoutinePolicy = new LinkedHashMap<>();
	private final Map<String, Requirement> operationPolicy = new LinkedHashMap<>();
	private final Map<String, Requirement> functionPolicy = new LinkedHashMap<>();
	private final Map<String, Requirement> variablePolicy = new LinkedHashMap<>();
	private final Map<String, Requirement> keywordPolicy = new LinkedHashMap<>();
	private final Map<String, Requirement> relationPolicy = new LinkedHashMap<>();
	private final Map<String, Requirement> routinePolicy = new LinkedHashMap<>();

	private SqlDialect dialect;
	private SquareBracketQuotation squareBracketQuotation;
	private Requirement parserFailurePolicy;
	private Requirement multiStatementPolicy;
	private Requirement deleteWithoutWherePolicy;
	private Requirement updateWithoutWherePolicy;
	private Requirement selectStarPolicy;
	private Requirement cartesianJoinPolicy;
	private Requirement recursiveCtePolicy;
	private Requirement joinLimitPolicy;
	private int maxJoins;
	private int maxQueryLength;

	public SqlQueryGuardrailEngine() {
		this.keysToGet = new String[] { QUERY_PARAM, DATABASE_ID_PARAM };
		this.keyRequired = new int[] { 1, 0 };
		this.functionName = "authorizeSqlQuery";
		this.functionDescription = "Parses SQL and applies operation, identifier, structure, and database-access policies.";
		this.parameters = List.of(new FunctionParameter(QUERY_PARAM, "String", "The SQL statement to authorize"),
				new FunctionParameter(DATABASE_ID_PARAM, "String",
						"The protected database engine id, used to resolve caller access"));
		this.requiredParameters = List.of(QUERY_PARAM);
		configurePolicy(new Properties());
	}

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
		configurePolicy(smssProp);
	}

	void configurePolicy(Properties properties) {
		this.dialect = SqlDialect.from(properties.getProperty(SQL_DIALECT_KEY, SqlDialect.GENERIC.name()));
		this.squareBracketQuotation = SquareBracketQuotation
				.from(properties.getProperty(SQUARE_BRACKET_QUOTATION_KEY, SquareBracketQuotation.AUTO.name()));
		this.parserFailurePolicy = readRequirement(properties, PARSER_FAILURE_POLICY_KEY, Requirement.DENY);
		this.multiStatementPolicy = readRequirement(properties, MULTI_STATEMENT_POLICY_KEY, Requirement.DENY);
		this.deleteWithoutWherePolicy = readRequirement(properties, DELETE_WITHOUT_WHERE_POLICY_KEY, Requirement.DENY);
		this.updateWithoutWherePolicy = readRequirement(properties, UPDATE_WITHOUT_WHERE_POLICY_KEY, Requirement.DENY);
		this.selectStarPolicy = readRequirement(properties, SELECT_STAR_POLICY_KEY,
				Requirement.FOLLOW_ENGINE_PERMISSIONS);
		this.cartesianJoinPolicy = readRequirement(properties, CARTESIAN_JOIN_POLICY_KEY,
				Requirement.FOLLOW_ENGINE_PERMISSIONS);
		this.recursiveCtePolicy = readRequirement(properties, RECURSIVE_CTE_POLICY_KEY, Requirement.OWNER);
		this.joinLimitPolicy = readRequirement(properties, JOIN_LIMIT_POLICY_KEY, Requirement.DENY);
		this.maxJoins = readNonNegativeInt(properties, MAX_JOINS_KEY, 0);
		this.maxQueryLength = readNonNegativeInt(properties, MAX_QUERY_LENGTH_KEY, 100_000);

		this.operationPolicy.clear();
		this.functionPolicy.clear();
		this.variablePolicy.clear();
		this.keywordPolicy.clear();
		this.relationPolicy.clear();
		this.routinePolicy.clear();
		this.defaultFunctionPolicy.clear();
		this.defaultVariablePolicy.clear();
		this.defaultKeywordPolicy.clear();
		this.defaultRelationPolicy.clear();
		this.defaultRoutinePolicy.clear();

		if (Boolean.parseBoolean(properties.getProperty(PROTECT_UNMATCHED_IDENTIFIERS_KEY, "true"))) {
			loadDefaultIdentifierPolicy(this.dialect);
		}

		overlayOperationPolicy(properties, this.operationPolicy);
		overlayPolicy(properties, FUNCTION_POLICY_KEY, this.functionPolicy);
		overlayPolicy(properties, VARIABLE_POLICY_KEY, this.variablePolicy);
		overlayPolicy(properties, KEYWORD_POLICY_KEY, this.keywordPolicy);
		overlayPolicy(properties, RELATION_POLICY_KEY, this.relationPolicy);
		overlayPolicy(properties, ROUTINE_POLICY_KEY, this.routinePolicy);
	}

	private void loadDefaultIdentifierPolicy(SqlDialect dialect) {
		if (dialect == SqlDialect.GENERIC || dialect == SqlDialect.POSTGRESQL) {
			addPolicies(this.defaultFunctionPolicy, Requirement.DENY, "PG_SLEEP*", "PG_READ_*", "PG_LS_*",
					"PG_STAT_FILE", "PG_LOGDIR_LS", "LO_IMPORT", "LO_EXPORT", "DBLINK*", "VERSION", "CURRENT_DATABASE",
					"CURRENT_SCHEMA", "CURRENT_SETTING", "INET_SERVER_ADDR", "INET_SERVER_PORT", "PG_BACKEND_PID",
					"PG_CONF_LOAD_TIME", "PG_POSTMASTER_START_TIME", "PG_IS_IN_RECOVERY");
			addSchemaPolicies(this.defaultRelationPolicy, Requirement.OWNER, "PG_CATALOG");
			this.defaultRelationPolicy.put("PG_*", Requirement.OWNER);
		}
		if (dialect == SqlDialect.GENERIC || dialect == SqlDialect.SQL_SERVER) {
			addPolicies(this.defaultFunctionPolicy, Requirement.DENY, "DB_NAME", "DB_ID", "SERVERPROPERTY", "HOST_NAME",
					"APP_NAME", "SUSER_SNAME", "IS_SRVROLEMEMBER", "HAS_DBACCESS", "DATABASEPROPERTYEX",
					"OBJECT_DEFINITION", "LOGINPROPERTY", "OPENROWSET", "OPENDATASOURCE", "OPENQUERY");
			addPolicies(this.defaultVariablePolicy, Requirement.DENY, "@@*");
			addPolicies(this.defaultRoutinePolicy, Requirement.DENY, "XP_*", "SP_OA*", "SP_CONFIGURE", "SP_EXECUTESQL");
			addSchemaPolicies(this.defaultRelationPolicy, Requirement.OWNER, "SYS");
			addPolicies(this.defaultRelationPolicy, Requirement.OWNER, "SYSOBJECTS", "SYSCOLUMNS", "SYSDATABASES",
					"SYSUSERS", "SYSPROCESSES");
		}
		if (dialect == SqlDialect.GENERIC || dialect == SqlDialect.MYSQL || dialect == SqlDialect.MARIADB) {
			addPolicies(this.defaultFunctionPolicy, Requirement.DENY, "SLEEP", "BENCHMARK", "LOAD_FILE", "VERSION",
					"DATABASE", "SCHEMA", "CONNECTION_ID");
			addPolicies(this.defaultVariablePolicy, Requirement.DENY, "@@*");
			addSchemaPolicies(this.defaultRelationPolicy, Requirement.OWNER, "MYSQL", "PERFORMANCE_SCHEMA", "SYS");
		}
		if (dialect == SqlDialect.GENERIC || dialect == SqlDialect.ORACLE) {
			addPolicies(this.defaultFunctionPolicy, Requirement.DENY, "DBMS_LOCK.SLEEP", "DBMS_SESSION.SLEEP",
					"UTL_HTTP.REQUEST", "UTL_INADDR.GET_HOST_ADDRESS", "UTL_FILE.FOPEN", "SYS_CONTEXT",
					"ORA_DATABASE_NAME");
			addPolicies(this.defaultRoutinePolicy, Requirement.DENY, "DBMS_SCHEDULER.*", "DBMS_JOB.*", "UTL_FILE.*",
					"UTL_HTTP.*", "UTL_INADDR.*", "DBMS_LDAP.*");
			addSchemaPolicies(this.defaultRelationPolicy, Requirement.OWNER, "SYS", "SYSTEM");
			this.defaultRelationPolicy.put("V$*", Requirement.OWNER);
			this.defaultRelationPolicy.put("*.V$*", Requirement.OWNER);
			this.defaultRelationPolicy.put("GV$*", Requirement.OWNER);
			this.defaultRelationPolicy.put("*.GV$*", Requirement.OWNER);
			addPolicies(this.defaultRelationPolicy, Requirement.OWNER, "DBA_*", "ALL_*", "USER_*");
		}
		addPolicies(this.defaultFunctionPolicy, Requirement.DENY, "CURRENT_USER", "SESSION_USER", "SYSTEM_USER", "USER",
				"CURRENT_ROLE");
		addPolicies(this.defaultKeywordPolicy, Requirement.DENY, "CURRENT_USER", "SESSION_USER", "SYSTEM_USER", "USER",
				"CURRENT_ROLE", "CURRENT_SCHEMA");
		addSchemaPolicies(this.defaultRelationPolicy, Requirement.OWNER, "INFORMATION_SCHEMA");
	}

	private static void addPolicies(Map<String, Requirement> policy, Requirement requirement, String... identifiers) {
		for (String identifier : identifiers) {
			policy.put(identifier, requirement);
		}
	}

	private static void addSchemaPolicies(Map<String, Requirement> policy, Requirement requirement, String... schemas) {
		for (String schema : schemas) {
			policy.put(schema + ".*", requirement);
			policy.put("*." + schema + ".*", requirement);
		}
	}

	@Override
	public GuardrailNounMetadata execute(NounStore ns, GenRowStruct curRow) {
		return execute(ns, curRow, null);
	}

	@Override
	@IgnoreEngineLogging
	public GuardrailNounMetadata execute(NounStore ns, GenRowStruct curRow, IEngine targetEngine) {
		Map<String, String> keyValue = organizeKeys(ns, curRow);
		String query = keyValue.get(QUERY_PARAM);
		String databaseId = resolveDatabaseId(targetEngine, keyValue.get(DATABASE_ID_PARAM));
		CallerAccess callerAccess = resolveCallerAccess(ns, databaseId);
		return evaluateQuery(query, callerAccess);
	}

	String resolveDatabaseId(IEngine targetEngine, String configuredDatabaseId) {
		if (targetEngine == null) {
			return configuredDatabaseId;
		}
		try {
			if (targetEngine.getCatalogType() != IEngine.CATALOG_TYPE.DATABASE) {
				classLogger.warn("SQL query guardrail received a non-database target engine");
				return null;
			}
			return targetEngine.getEngineId();
		} catch (Exception e) {
			classLogger.error("Unable to read the target database identity for SQL authorization", e);
			return null;
		}
	}

	GuardrailNounMetadata evaluateQuery(String query, CallerAccess callerAccess) {
		Map<String, Object> details = new LinkedHashMap<>();
		details.put("parser", "JSQLParser");
		details.put("dialectPolicy", this.dialect.name());
		details.put("callerAccess", callerAccess.name());
		List<Map<String, Object>> violations = new ArrayList<>();

		if (query == null || query.isBlank()) {
			addViolation(violations, "QUERY", "EMPTY", Requirement.DENY, callerAccess);
			return result(query, details, violations);
		}
		if (this.maxQueryLength > 0 && query.length() > this.maxQueryLength) {
			addViolation(violations, "QUERY_LENGTH", query.length() + ">" + this.maxQueryLength, Requirement.DENY,
					callerAccess);
			return result(query, details, violations);
		}

		SqlQueryPolicyParser.Analysis analysis;
		try {
			analysis = SqlQueryPolicyParser.parse(query, useSquareBracketQuotation());
			details.put("parserStatus", "PARSED");
		} catch (Exception e) {
			details.put("parserStatus", "FAILED");
			details.put("parserError", e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
			applyRequirement(violations, "PARSER_FAILURE", this.dialect.name(), this.parserFailurePolicy, callerAccess);
			return result(query, details, violations);
		}

		details.put("statementCount", analysis.statementCount);
		details.put("operations", analysis.operations.stream().map(Enum::name).toList());
		if (analysis.statementCount == 0) {
			applyRequirement(violations, "PARSER_FAILURE", "NO_STATEMENTS", this.parserFailurePolicy, callerAccess);
			return result(query, details, violations);
		}
		if (analysis.statementCount > 1) {
			applyRequirement(violations, "MULTI_STATEMENT", Integer.toString(analysis.statementCount),
					this.multiStatementPolicy, callerAccess);
		}

		for (SqlQueryPolicyParser.Operation operation : analysis.operations) {
			Requirement requirement = resolveOperationRequirement(operation);
			applyRequirement(violations, "OPERATION", operation.name(), requirement, callerAccess);
		}

		if (analysis.deleteWithoutWhere) {
			applyRequirement(violations, "STRUCTURE", "DELETE_WITHOUT_WHERE", this.deleteWithoutWherePolicy,
					callerAccess);
		}
		if (analysis.updateWithoutWhere) {
			applyRequirement(violations, "STRUCTURE", "UPDATE_WITHOUT_WHERE", this.updateWithoutWherePolicy,
					callerAccess);
		}
		if (analysis.selectStar) {
			applyRequirement(violations, "STRUCTURE", "SELECT_STAR", this.selectStarPolicy, callerAccess);
		}
		if (analysis.cartesianJoin) {
			applyRequirement(violations, "STRUCTURE", "CARTESIAN_JOIN", this.cartesianJoinPolicy, callerAccess);
		}
		if (analysis.recursiveCte) {
			applyRequirement(violations, "STRUCTURE", "RECURSIVE_CTE", this.recursiveCtePolicy, callerAccess);
		}
		if (this.maxJoins > 0 && analysis.joinCount > this.maxJoins) {
			applyRequirement(violations, "STRUCTURE", "JOIN_COUNT_" + analysis.joinCount, this.joinLimitPolicy,
					callerAccess);
		}

		applyIdentifierPolicies(violations, "FUNCTION", analysis.functions, this.functionPolicy,
				this.defaultFunctionPolicy, callerAccess, true);
		applyIdentifierPolicies(violations, "VARIABLE", analysis.variables, this.variablePolicy,
				this.defaultVariablePolicy, callerAccess, false);
		applyIdentifierPolicies(violations, "KEYWORD", analysis.keywords, this.keywordPolicy, this.defaultKeywordPolicy,
				callerAccess, false);
		applyIdentifierPolicies(violations, "RELATION", analysis.relations, this.relationPolicy,
				this.defaultRelationPolicy, callerAccess, false);
		applyIdentifierPolicies(violations, "ROUTINE", analysis.routines, this.routinePolicy, this.defaultRoutinePolicy,
				callerAccess, true);

		return result(query, details, violations);
	}

	private boolean useSquareBracketQuotation() {
		return this.squareBracketQuotation == SquareBracketQuotation.ENABLED
				|| (this.squareBracketQuotation == SquareBracketQuotation.AUTO
						&& this.dialect == SqlDialect.SQL_SERVER);
	}

	private CallerAccess resolveCallerAccess(NounStore ns, String databaseId) {
		Insight insight = getInsight(ns);
		User user = insight == null ? null : insight.getUser();
		if (user == null || databaseId == null || databaseId.isBlank()) {
			return CallerAccess.NONE;
		}

		try {
			if (SecurityEngineUtils.userIsOwner(user, databaseId)) {
				return CallerAccess.OWNER;
			}
			if (SecurityEngineUtils.userCanEditEngine(user, databaseId)) {
				return CallerAccess.EDIT;
			}
			if (SecurityEngineUtils.userCanViewEngine(user, databaseId)) {
				return CallerAccess.READ_ONLY;
			}
		} catch (Exception e) {
			classLogger.error("Unable to resolve caller access for SQL guardrail on database {}", databaseId, e);
		}
		return CallerAccess.NONE;
	}

	private Insight getInsight(NounStore ns) {
		if (ns == null) {
			return null;
		}
		GenRowStruct insightNoun = ns.getGenRowStruct(Constants.INSIGHT);
		if (insightNoun == null || insightNoun.isEmpty()) {
			return null;
		}
		Object value = insightNoun.get(0);
		return value instanceof Insight ? (Insight) value : null;
	}

	private Requirement resolveOperationRequirement(SqlQueryPolicyParser.Operation operation) {
		Requirement configured = findOperationRequirement(this.operationPolicy, operation);
		return Requirement.stricterOf(minimumOperationRequirement(operation),
				configured == null ? Requirement.FOLLOW_ENGINE_PERMISSIONS : configured);
	}

	private static Requirement findOperationRequirement(Map<String, Requirement> policy,
			SqlQueryPolicyParser.Operation operation) {
		Requirement requirement = policy.get(operation.name());
		if (requirement == null) {
			requirement = policy.get(operation.group.name());
		}
		return requirement == null ? policy.get("*") : requirement;
	}

	private static Requirement minimumOperationRequirement(SqlQueryPolicyParser.Operation operation) {
		switch (operation.group) {
		case READ:
		case METADATA:
			return Requirement.READ_ONLY;
		case ROUTINE:
			// running a stored routine hands control to code the guardrail cannot see,
			// so it is denied outright rather than being treated as an ordinary write
			return Requirement.DENY;
		case UNKNOWN:
			return Requirement.DENY;
		default:
			return Requirement.EDIT;
		}
	}

	private void applyIdentifierPolicies(List<Map<String, Object>> violations, String type, Iterable<String> values,
			Map<String, Requirement> configuredPolicy, Map<String, Requirement> defaultPolicy,
			CallerAccess callerAccess, boolean matchQualifiedLeaf) {
		for (String value : values) {
			Requirement requirement = findIdentifierRequirement(configuredPolicy, value, matchQualifiedLeaf);
			if (requirement == null) {
				requirement = findIdentifierRequirement(defaultPolicy, value, matchQualifiedLeaf);
			}
			if (requirement != null) {
				applyRequirement(violations, type, value, requirement, callerAccess);
			}
		}
	}

	private Requirement findIdentifierRequirement(Map<String, Requirement> policy, String value,
			boolean matchQualifiedLeaf) {
		Requirement exact = policy.get(value);
		if (exact != null) {
			return exact;
		}
		String leaf = value;
		if (matchQualifiedLeaf) {
			int finalQualifier = value.lastIndexOf('.');
			if (finalQualifier >= 0 && finalQualifier + 1 < value.length()) {
				leaf = value.substring(finalQualifier + 1);
				exact = policy.get(leaf);
				if (exact != null) {
					return exact;
				}
			}
		}

		Requirement matched = null;
		int matchedSpecificity = -1;
		int matchedWildcards = Integer.MAX_VALUE;
		for (Map.Entry<String, Requirement> entry : policy.entrySet()) {
			String pattern = entry.getKey();
			if (pattern.indexOf('*') >= 0 && (wildcardMatches(pattern, value)
					|| (matchQualifiedLeaf && !leaf.equals(value) && wildcardMatches(pattern, leaf)))) {
				int specificity = pattern.length() - count(pattern, '*');
				int wildcards = count(pattern, '*');
				if (specificity > matchedSpecificity
						|| (specificity == matchedSpecificity && wildcards < matchedWildcards)
						|| (specificity == matchedSpecificity && wildcards == matchedWildcards
								&& (matched == null || entry.getValue().ordinal() > matched.ordinal()))) {
					matched = entry.getValue();
					matchedSpecificity = specificity;
					matchedWildcards = wildcards;
				}
			}
		}
		return matched;
	}

	private static int count(String value, char character) {
		int count = 0;
		for (int i = 0; i < value.length(); i++) {
			if (value.charAt(i) == character) {
				count++;
			}
		}
		return count;
	}

	static boolean wildcardMatches(String pattern, String value) {
		String[] pieces = pattern.split("\\*", -1);
		int valueIndex = 0;
		for (int i = 0; i < pieces.length; i++) {
			String piece = pieces[i];
			if (piece.isEmpty()) {
				continue;
			}
			int found = value.indexOf(piece, valueIndex);
			if (found < 0 || (i == 0 && !pattern.startsWith("*") && found != 0)) {
				return false;
			}
			valueIndex = found + piece.length();
		}
		return pattern.endsWith("*") || pieces[pieces.length - 1].isEmpty() || valueIndex == value.length();
	}

	private static void applyRequirement(List<Map<String, Object>> violations, String type, String value,
			Requirement requirement, CallerAccess callerAccess) {
		if (!requirement.isSatisfiedBy(callerAccess)) {
			addViolation(violations, type, value, requirement, callerAccess);
		}
	}

	private static void addViolation(List<Map<String, Object>> violations, String type, String value,
			Requirement requirement, CallerAccess callerAccess) {
		Map<String, Object> violation = new LinkedHashMap<>();
		violation.put("type", type);
		violation.put("value", value);
		violation.put("required", requirement.name());
		violation.put("actual", callerAccess.name());
		violations.add(violation);
	}

	private static GuardrailNounMetadata result(String query, Map<String, Object> details,
			List<Map<String, Object>> violations) {
		boolean pass = violations.isEmpty();
		details.put("classification", pass ? "ALLOWED" : "BLOCKED");
		details.put("violations", violations);
		return new GuardrailNounMetadata(pass, query, details);
	}

	private static Requirement readRequirement(Properties properties, String key, Requirement defaultValue) {
		String value = properties.getProperty(key);
		return value == null || value.isBlank() ? defaultValue : Requirement.from(value);
	}

	private static int readNonNegativeInt(Properties properties, String key, int defaultValue) {
		String value = properties.getProperty(key);
		if (value == null || value.isBlank()) {
			return defaultValue;
		}
		try {
			int parsed = Integer.parseInt(value.trim());
			if (parsed < 0) {
				throw new IllegalArgumentException(key + " cannot be negative");
			}
			return parsed;
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException(key + " must be a whole number", e);
		}
	}

	private static void overlayPolicy(Properties properties, String key, Map<String, Requirement> destination) {
		String json = properties.getProperty(key);
		if (json == null || json.isBlank()) {
			return;
		}
		try {
			JSONObject policy = new JSONObject(json);
			for (String policyKey : policy.keySet()) {
				destination.put(policyKey.trim().toUpperCase(Locale.ROOT),
						Requirement.from(policy.get(policyKey).toString()));
			}
		} catch (Exception e) {
			throw new IllegalArgumentException(key + " must be a JSON object mapping policy names to "
					+ "FOLLOW_ENGINE_PERMISSIONS, DENY, READ_ONLY, EDIT, or OWNER", e);
		}
	}

	private static void overlayOperationPolicy(Properties properties, Map<String, Requirement> destination) {
		String json = properties.getProperty(OPERATION_POLICY_KEY);
		if (json == null || json.isBlank()) {
			return;
		}
		try {
			JSONObject policy = new JSONObject(json);
			for (String policyKey : policy.keySet()) {
				String normalizedKey = policyKey.trim().toUpperCase(Locale.ROOT);
				if (!isOperationPolicyKey(normalizedKey)) {
					throw new IllegalArgumentException(
							OPERATION_POLICY_KEY + " contains unsupported key: " + policyKey);
				}
				destination.put(normalizedKey, Requirement.from(policy.get(policyKey).toString()));
			}
		} catch (IllegalArgumentException e) {
			throw e;
		} catch (Exception e) {
			throw new IllegalArgumentException(OPERATION_POLICY_KEY
					+ " must be a JSON object mapping *, an operation group, or an exact operation to "
					+ "FOLLOW_ENGINE_PERMISSIONS, DENY, READ_ONLY, EDIT, or OWNER", e);
		}
	}

	private static boolean isOperationPolicyKey(String key) {
		if ("*".equals(key)) {
			return true;
		}
		try {
			SqlQueryPolicyParser.Operation.valueOf(key);
			return true;
		} catch (IllegalArgumentException ignored) {
			// Try the coarser operation vocabulary next.
		}
		try {
			SqlQueryPolicyParser.OperationGroup.valueOf(key);
			return true;
		} catch (IllegalArgumentException ignored) {
			return false;
		}
	}

	@Override
	public GuardrailTypeEnum getGuardrailType() {
		return GuardrailTypeEnum.EMBEDDED_SQL_QUERY;
	}

	@Override
	public String getDefaultMarkdown() {
		return """
				# SQL query authorization guardrail

				This guardrail parses every statement and authorizes the resulting AST operations
				against the caller's access to the protected database. It does not scan SQL with keyword or regex
				matching. Parser failures deny by default, which is important for unsupported vendor commands such as SQL Server
				`WAITFOR`, PostgreSQL `COPY`, or Oracle procedural blocks.

				Authorization model:

				- Platform minimums always apply: reads and metadata require `READ_ONLY`; mutations, DDL, routines,
				  session changes, transactions, and row locks require `EDIT`; unknown operations are denied.
				- A guardrail rule can make that minimum more restrictive. `FOLLOW_ENGINE_PERMISSIONS` leaves normal
				  engine authorization in control; no guardrail setting can grant view, edit, or owner access.
				- Unmatched operations follow normal engine permissions. Unlisted identifiers may additionally use
				  dialect-specific safety rules.

				Default additional restrictions:

				- Recursive CTEs require `OWNER` access. Multiple statements, parser failures, and update/delete
				  without a `WHERE` are denied.
				- Known delay, file-access, external-connection, and server-fingerprinting functions and variables
				  are denied. System catalogs require owner access.

				## Configuration

				`SQL_DIALECT` accepts `GENERIC`, `POSTGRESQL`, `SQL_SERVER`, `MYSQL`, `MARIADB`, or `ORACLE`;
				it selects identifier policy defaults. `SQUARE_BRACKET_QUOTATION` accepts `AUTO`, `ENABLED`, or
				`DISABLED` and configures SQL Server bracket handling during parsing.

				`OPERATION_POLICY`, `FUNCTION_POLICY`, `VARIABLE_POLICY`, `KEYWORD_POLICY`, `RELATION_POLICY`,
				and `ROUTINE_POLICY` are JSON objects. Values are `FOLLOW_ENGINE_PERMISSIONS`, `DENY`,
				`READ_ONLY`, `EDIT`, or `OWNER`.

				Operation policy keys:

				| Group | Exact operations in the group |
				| --- | --- |
				| `DDL` | `ALTER_TABLE`, `ALTER_VIEW`, `COMMENT`, `CREATE_INDEX`, `CREATE_TABLE`, `CREATE_VIEW`, `DROP`, `SELECT_INTO`, `TRUNCATE` |
				| `LOCK` | `SELECT_FOR_UPDATE` |
				| `METADATA` | `DESCRIBE`, `SHOW` |
				| `READ` | `EXPLAIN`, `SELECT`, `VALUES` |
				| `ROUTINE` | `BLOCK`, `EXECUTE` |
				| `SESSION` | `DECLARE`, `SET`, `USE` |
				| `TRANSACTION` | `COMMIT` |
				| `UNKNOWN` | `UNKNOWN` |
				| `WRITE` | `DELETE`, `INSERT`, `MERGE`, `REPLACE`, `UPDATE`, `UPSERT` |

				Use `*` as the catch-all operation key. Within the configured map, exact operations override their
				group, and a group overrides `*`. When no configured operation rule matches, normal engine permissions
				remain in control.
				`FOLLOW_ENGINE_PERMISSIONS` leaves normal engine authorization unchanged, `READ_ONLY` requires existing view
				access, `EDIT` requires existing edit access, `OWNER` requires existing database ownership, and `DENY`
				blocks everyone. The platform minimum is always enforced after policy resolution, so policies cannot
				grant database access.

				To leave inserts under normal engine permissions but deny all other writes, use:

				```properties
				OPERATION_POLICY {"WRITE":"DENY","INSERT":"FOLLOW_ENGINE_PERMISSIONS"}
				```

				The configured exact `INSERT` rule wins over the configured `WRITE` group. Other writes, including
				`DELETE`, match configured `WRITE: DENY`. Insert still requires platform
				edit access because `FOLLOW_ENGINE_PERMISSIONS` cannot reduce the mandatory access floor.

				To leave only selects under normal engine permissions and deny every other operation, use:

				```properties
				OPERATION_POLICY {"SELECT":"FOLLOW_ENGINE_PERMISSIONS","*":"DENY"}
				```

				A viewer can select, but a user without view access cannot. Every other operation matches `DENY`,
				including for database owners.

				Identifier keys support `*` wildcards; an exact key wins over a wildcard, and the most-specific
				wildcard wins deterministically. Any configured identifier match overrides the built-in safety rule;
				built-in rules are used only when no configured identifier rule matches.

				Example override:

				```properties
				SQL_DIALECT                 POSTGRESQL
				OPERATION_POLICY            {"DELETE":"OWNER","TRUNCATE":"DENY","DDL":"OWNER","WRITE":"EDIT"}
				FUNCTION_POLICY             {"CURRENT_DATABASE":"OWNER","PG_SLEEP":"DENY"}
				KEYWORD_POLICY              {"CURRENT_USER":"OWNER"}
				RELATION_POLICY             {"PG_CATALOG.*":"OWNER","INFORMATION_SCHEMA.*":"READ_ONLY"}
				PARSER_FAILURE_POLICY       DENY
				MULTI_STATEMENT_POLICY      DENY
				DELETE_WITHOUT_WHERE_POLICY DENY
				UPDATE_WITHOUT_WHERE_POLICY DENY
				```

				Structural options also include `SELECT_STAR_POLICY`, `CARTESIAN_JOIN_POLICY`,
				`RECURSIVE_CTE_POLICY`, `MAX_JOINS`, `JOIN_LIMIT_POLICY`, and `MAX_QUERY_LENGTH`.
				`MULTI_STATEMENT_POLICY` applies when raw multi-statement SQL reaches one database engine call. The
				`SqlQuery` reactor's supported batch path AST-splits the script first, so the engine guardrail checks
				each resulting statement independently.
				`PROTECT_UNMATCHED_IDENTIFIERS` defaults to `true`. When enabled, known-dangerous identifier rules apply
				to identifiers not matched by a configured rule. When disabled, unmatched identifiers have no extra
				guardrail restriction. A configured `*` decides every value in its policy map, so there are no unmatched
				values in that map. The guardrail resolves only the caller's actual view, edit, or owner access to the
				protected database and never elevates it.

				## Database pipeline

				Save the following as `pipeline.json` in the database engine's assets folder, set
				`PIPELINE pipeline.json` in the database SMSS, and restart or reload the database:

				```json
				{
				  "pipelines": {
				    "execQuery": { "input": [{
				      "reactorClass": "prerna.reactor.interceptor.GenericGuardrailInputReactor",
				      "params": {
				        "guardrailEngineId": "%1$s",
				        "inputMapping": { "query": "arg0" },
				        "blockOnGuardrailFailure": true,
				        "blockErrorMessage": "The SQL statement is not permitted by database policy."
				      }
				    }]},
				    "insertData": { "input": [{
				      "reactorClass": "prerna.reactor.interceptor.GenericGuardrailInputReactor",
				      "params": {
				        "guardrailEngineId": "%1$s",
				        "inputMapping": { "query": "arg0" },
				        "blockOnGuardrailFailure": true,
				        "blockErrorMessage": "The SQL statement is not permitted by database policy."
				      }
				    }]},
				    "removeData": { "input": [{
				      "reactorClass": "prerna.reactor.interceptor.GenericGuardrailInputReactor",
				      "params": {
				        "guardrailEngineId": "%1$s",
				        "inputMapping": { "query": "arg0" },
				        "blockOnGuardrailFailure": true,
				        "blockErrorMessage": "The SQL statement is not permitted by database policy."
				      }
				    }]}
				  }
				}
				```

				The engine proxy supplies the actual target engine directly to the guardrail's in-memory execute call;
				it is never placed in a noun, argument map, verdict, or audit payload. The guardrail uses its engine id
				and the current insight's user to resolve database ownership and edit/view access. A direct call outside
				the pipeline may instead supply the optional `databaseId` noun. Keep parser failures fail-closed unless
				every allowed vendor extension has an equivalent database-side permission boundary.

				This is an application authorization layer, not a replacement for database controls. Run the database
				connection with least privilege and configure its statement timeout/resource governor. Those controls
				cover planner cost, locking, and vendor syntax that an application parser cannot safely predict.
				"""
				.formatted(getEngineId());
	}

	enum CallerAccess {
		NONE, READ_ONLY, EDIT, OWNER
	}

	enum Requirement {
		FOLLOW_ENGINE_PERMISSIONS, READ_ONLY, EDIT, OWNER, DENY;

		static Requirement stricterOf(Requirement left, Requirement right) {
			return left.ordinal() >= right.ordinal() ? left : right;
		}

		static Requirement from(String value) {
			String normalized = value.trim().toUpperCase(Locale.ROOT);
			if ("READ".equals(normalized) || "VIEW".equals(normalized)) {
				normalized = READ_ONLY.name();
			}
			try {
				return valueOf(normalized);
			} catch (IllegalArgumentException e) {
				throw new IllegalArgumentException(
						"Policy requirement must be FOLLOW_ENGINE_PERMISSIONS, DENY, READ_ONLY, EDIT, or OWNER: "
								+ value,
						e);
			}
		}

		boolean isSatisfiedBy(CallerAccess actual) {
			switch (this) {
			case FOLLOW_ENGINE_PERMISSIONS:
				return true;
			case READ_ONLY:
				return actual == CallerAccess.READ_ONLY || actual == CallerAccess.EDIT || actual == CallerAccess.OWNER;
			case EDIT:
				return actual == CallerAccess.EDIT || actual == CallerAccess.OWNER;
			case OWNER:
				return actual == CallerAccess.OWNER;
			case DENY:
			default:
				return false;
			}
		}
	}

	private enum SqlDialect {
		GENERIC, POSTGRESQL, SQL_SERVER, MYSQL, MARIADB, ORACLE;

		static SqlDialect from(String value) {
			String normalized = value.trim().toUpperCase(Locale.ROOT).replace('-', '_');
			if ("POSTGRES".equals(normalized)) {
				return POSTGRESQL;
			}
			if ("SQLSERVER".equals(normalized) || "MSSQL".equals(normalized)) {
				return SQL_SERVER;
			}
			try {
				return valueOf(normalized);
			} catch (IllegalArgumentException e) {
				throw new IllegalArgumentException("Unsupported SQL_DIALECT: " + value, e);
			}
		}
	}

	private enum SquareBracketQuotation {
		AUTO, ENABLED, DISABLED;

		static SquareBracketQuotation from(String value) {
			String normalized = value.trim().toUpperCase(Locale.ROOT);
			if ("TRUE".equals(normalized)) {
				return ENABLED;
			}
			if ("FALSE".equals(normalized)) {
				return DISABLED;
			}
			try {
				return valueOf(normalized);
			} catch (IllegalArgumentException e) {
				throw new IllegalArgumentException("SQUARE_BRACKET_QUOTATION must be AUTO, ENABLED, or DISABLED", e);
			}
		}
	}
}
