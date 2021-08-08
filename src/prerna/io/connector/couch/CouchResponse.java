package prerna.io.connector.couch;

public class CouchResponse {
	
	private final int statusCode;
	private final String responseBody;
	private final String revision;

	public CouchResponse(int statusCode, String responseBody, String revision) {
		this.statusCode = statusCode;
		this.responseBody = responseBody;
		this.revision = revision;
	}

	public int getStatusCode() {
		return statusCode;
	}

	public String getResponseBody() {
		return responseBody;
	}

	public String getRevision() {
		return revision;
	}

	@Override
	public String toString() {
		return "CouchResponse [statusCode=" + statusCode + ", responseBody=" + responseBody + ", revision=" + revision
				+ "]";
	}
	
	
}
