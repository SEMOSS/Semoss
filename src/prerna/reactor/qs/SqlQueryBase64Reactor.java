package prerna.reactor.qs;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import prerna.sablecc2.om.ReactorKeysEnum;

/**
 * Executes Base64-encoded sql query against a database.
 */
public class SqlQueryBase64Reactor extends AbstractSqlQueryReactor {

	@Override
	protected String getDecodedQuery() {
		try {
			return new String(Base64.getDecoder().decode(this.keyValue.get(ReactorKeysEnum.QUERY_KEY.getKey())),
					StandardCharsets.UTF_8);
		} catch (Exception e) {
			throw new IllegalArgumentException("Failed to decode python code: input is not base64-encoded utf-8 string",
					e);
		}
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.QUERY_KEY.getKey())) {
			return "The sql query to execute. The query should be passed in as a base64-encoded utf-8 string";
		}
		return super.getDescriptionForKey(key);
	}
}