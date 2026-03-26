package prerna.reactor.qs;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import prerna.sablecc2.om.ReactorKeysEnum;

/**
 * Unified SQL Query Reactor that accepts Base64-encoded SQL queries: 1. Decodes
 * Base64-encoded SQL query 2. Parses SQL to detect query type (SELECT vs
 * modification) 3. Validates user permissions based on query type 4. Delegates
 * to appropriate existing reactors
 * 
 * Usage: SqlQueryBase64(database=["myDb"], query=[base64_encoded_sql],
 * limit=[100], commit=[true])
 */
public class SqlQueryBase64Reactor extends AbstractSqlQueryReactor {

	public SqlQueryBase64Reactor() {
	}

	@Override
	protected String getDecodedQuery() {
		try {
			return new String(Base64.getDecoder().decode(this.keyValue.get(ReactorKeysEnum.QUERY_KEY.getKey())),
					StandardCharsets.UTF_8);
		} catch (Exception e) {
			throw new IllegalArgumentException("Failed to decode SQL query: not valid Base64", e);
		}
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.QUERY_KEY.getKey())) {
			return "The SQL query to execute, provided as a Base64-encoded UTF-8 string.";
		} else if (key.equals(ReactorKeysEnum.DATABASE.getKey())) {
			return "The database id";
		} else if (key.equals(ReactorKeysEnum.LIMIT.getKey())) {
			return "Limits the number of rows retrieved by the SQL query";
		} else {
			return super.getDescriptionForKey(key);
		}
	}

	@Override
	public String getReactorDescription() {
		return "Execute a Base64-encoded SQL query against a database";
	}
}