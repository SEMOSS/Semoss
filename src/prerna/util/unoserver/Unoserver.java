package prerna.util.unoserver;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.entity.mime.HttpMultipartMode;
import org.apache.hc.client5.http.entity.mime.MultipartEntityBuilder;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.net.URIBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import prerna.security.HttpHelperUtility;
import prerna.util.Utility;

/**
 * Client for the unoserver microservice that converts documents from one format
 * to another (backed by LibreOffice / unoconvert).
 * <p>
 * The base URL of the deployed service is read from the {@code UNOSERVER}
 * property in the RDF_Map.prop file, falling back to a {@code UNOSERVER}
 * environment variable when the property is absent. The service exposes:
 * <ul>
 * <li>{@code GET /health} - returns {@code {"ok": true}} when the underlying
 * unoserver is ready, otherwise HTTP 503.</li>
 * <li>{@code POST /convert?to=<format>} - accepts a multipart {@code file} part
 * and returns the converted document as raw bytes.</li>
 * </ul>
 */
public class Unoserver {

	private static final Logger classLogger = LogManager.getLogger(Unoserver.class);

	/**
	 * Key holding the base URL of the deployed unoserver microservice. Resolved first
	 * from RDF_Map.prop and then, as a fallback, from an environment variable of the
	 * same name.
	 */
	public static final String UNOSERVER = "UNOSERVER";

	/** Target format used when the caller does not specify one (mirrors the service default). */
	public static final String DEFAULT_TARGET_FORMAT = "pdf";

	private static final String HEALTH_PATH = "/health";
	private static final String CONVERT_PATH = "/convert";

	/** Base URL of the unoserver microservice with any trailing slash stripped. */
	private final String baseUrl;

	/**
	 * Reads and validates the {@code UNOSERVER} URL from the RDF map, falling back to
	 * the {@code UNOSERVER} environment variable.
	 *
	 * @throws IllegalArgumentException if the {@code UNOSERVER} property is not defined
	 *                                  in the RDF_Map.prop file or as an environment
	 *                                  variable
	 */
	public Unoserver() {
		this.baseUrl = getConfiguredUrl();
	}

	/**
	 * Pull the {@code UNOSERVER} base URL out of the RDF map and ensure it is set.
	 * The RDF_Map.prop value takes precedence; when it is absent we fall back to a
	 * {@code UNOSERVER} environment variable.
	 */
	private static String getConfiguredUrl() {
		String url = Utility.getDIHelperProperty(UNOSERVER);
		if (url == null || url.trim().isEmpty()) {
			// getDIHelperProperty only looks at the RDF map, so check the environment ourselves
			url = System.getenv(UNOSERVER);
		}
		if (url == null || (url = url.trim()).isEmpty()) {
			throw new IllegalArgumentException("The \"" + UNOSERVER
					+ "\" property is not defined in the RDF_Map.prop file or as a \"" + UNOSERVER
					+ "\" environment variable. Add it pointing to the deployed "
					+ "unoserver microservice (e.g. " + UNOSERVER + "\thttps://your-unoserver-host).");
		}
		// drop any trailing slash so we can safely append the endpoint paths
		while (url.endsWith("/")) {
			url = url.substring(0, url.length() - 1);
		}
		return url;
	}

	public String getBaseUrl() {
		return this.baseUrl;
	}

	/////////////////////////////////////////////////////////////////////////////////////////
	// health check
	/////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * Hits {@code GET /health} and reports whether the service is ready. Never
	 * throws - any failure is logged and reported as {@code false}.
	 *
	 * @return {@code true} when unoserver responds 2xx with {@code {"ok": true}}
	 */
	public boolean isHealthy() {
		try {
			checkHealth();
			return true;
		} catch (RuntimeException e) {
			classLogger.warn("unoserver health check failed: " + e.getMessage());
			return false;
		}
	}

	/**
	 * Hits {@code GET /health} and throws if unoserver is not reachable or reports
	 * that it is not ready.
	 *
	 * @throws IllegalStateException if the service cannot be reached or is not ready
	 */
	public void checkHealth() {
		String url = this.baseUrl + HEALTH_PATH;
		Map<String, String> headers = new HashMap<>();
		headers.put("accept", "application/json");

		String response;
		try {
			// getRequest throws on a non-2xx status (e.g. 503 not ready) or a connection failure
			response = HttpHelperUtility.getRequest(url, headers, null, null, null);
		} catch (RuntimeException e) {
			throw new IllegalStateException("unoserver health check failed at " + url + ": " + e.getMessage(), e);
		}

		// a healthy service returns {"ok": true}
		boolean ok = false;
		if (response != null && !response.trim().isEmpty()) {
			try {
				ok = new JSONObject(response).optBoolean("ok", false);
			} catch (RuntimeException e) {
				// 2xx but an unexpected body - treat the service as up rather than failing the call
				classLogger.warn("unoserver /health returned an unexpected body: " + response);
				ok = true;
			}
		}
		if (!ok) {
			throw new IllegalStateException("unoserver at " + url + " reported it is not ready (response: " + response + ")");
		}
	}

	/////////////////////////////////////////////////////////////////////////////////////////
	// convert
	/////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * Converts a file on disk to the requested format.
	 *
	 * @param source       the source document to convert
	 * @param targetFormat the format to convert to (e.g. "pdf", "docx"); defaults
	 *                     to {@link #DEFAULT_TARGET_FORMAT} when blank
	 * @return the converted document as bytes
	 */
	public byte[] convert(File source, String targetFormat) {
		if (source == null || !source.exists() || !source.isFile()) {
			throw new IllegalArgumentException(
					"Source file to convert does not exist: " + (source == null ? "null" : source.getAbsolutePath()));
		}
		byte[] content;
		try {
			content = FileUtils.readFileToByteArray(source);
		} catch (IOException e) {
			classLogger.error("Failed to read source file " + source.getAbsolutePath(), e);
			throw new IllegalArgumentException("Could not read source file to convert: " + source.getAbsolutePath(), e);
		}
		return convert(content, source.getName(), targetFormat);
	}

	/**
	 * Converts the contents of an input stream to the requested format.
	 *
	 * @param source       stream of the source document
	 * @param fileName      original file name (must include the extension so
	 *                      unoserver can detect the source format)
	 * @param targetFormat  the format to convert to; defaults to
	 *                      {@link #DEFAULT_TARGET_FORMAT} when blank
	 * @return the converted document as bytes
	 */
	public byte[] convert(InputStream source, String fileName, String targetFormat) {
		if (source == null) {
			throw new IllegalArgumentException("No file content provided to convert");
		}
		byte[] content;
		try {
			content = IOUtils.toByteArray(source);
		} catch (IOException e) {
			classLogger.error("Failed to read source stream for '" + fileName + "'", e);
			throw new IllegalArgumentException("Could not read source content to convert for '" + fileName + "'", e);
		}
		return convert(content, fileName, targetFormat);
	}

	/**
	 * Converts the given bytes to the requested format. This is the core call that
	 * the other {@code convert} overloads delegate to. A health check is performed
	 * first so configuration / availability problems surface with a clear message
	 * before the (potentially large) upload is attempted.
	 *
	 * @param content      raw bytes of the source document
	 * @param fileName     original file name (must include the extension so
	 *                     unoserver can detect the source format)
	 * @param targetFormat the format to convert to; defaults to
	 *                     {@link #DEFAULT_TARGET_FORMAT} when blank
	 * @return the converted document as bytes
	 */
	public byte[] convert(byte[] content, String fileName, String targetFormat) {
		if (content == null || content.length == 0) {
			throw new IllegalArgumentException("No file content provided to convert");
		}
		if (fileName == null || fileName.trim().isEmpty()) {
			throw new IllegalArgumentException(
					"A file name (with extension) is required so unoserver can detect the source format");
		}
		fileName = fileName.trim();
		if (FilenameUtils.getExtension(fileName).isEmpty()) {
			classLogger.warn("Source file name '" + fileName
					+ "' has no extension; unoserver may be unable to detect the source format");
		}

		String to = (targetFormat == null || targetFormat.trim().isEmpty()) ? DEFAULT_TARGET_FORMAT
				: targetFormat.trim();

		// fail fast if the service is not up before sending the file
		checkHealth();

		URI uri;
		try {
			uri = new URIBuilder(this.baseUrl + CONVERT_PATH).addParameter("to", to).build();
		} catch (URISyntaxException e) {
			throw new IllegalArgumentException("Invalid unoserver URL: " + this.baseUrl + CONVERT_PATH, e);
		}
		final String url = uri.toString();

		try (CloseableHttpClient httpClient = HttpHelperUtility.getCustomClient(null, null, null, null)) {
			HttpPost httpPost = new HttpPost(uri);

			MultipartEntityBuilder builder = MultipartEntityBuilder.create();
			builder.setMode(HttpMultipartMode.EXTENDED);
			builder.addBinaryBody("file", content, ContentType.APPLICATION_OCTET_STREAM, fileName);
			httpPost.setEntity(builder.build());

			final String requestedFormat = to;
			final String sourceName = fileName;
			return httpClient.execute(httpPost, new HttpClientResponseHandler<byte[]>() {
				@Override
				public byte[] handleResponse(ClassicHttpResponse response) throws IOException {
					int statusCode = response.getCode();
					HttpEntity entity = response.getEntity();
					if (statusCode >= 200 && statusCode < 300) {
						return entity != null ? EntityUtils.toByteArray(entity) : null;
					}
					String errorBody;
					try {
						errorBody = entity != null ? EntityUtils.toString(entity, StandardCharsets.UTF_8) : "";
					} catch (ParseException e) {
						errorBody = "";
					}
					throw new IOException("unoserver failed to convert '" + sourceName + "' to '" + requestedFormat
							+ "' (HTTP " + statusCode + "): " + errorBody);
				}
			});
		} catch (IOException e) {
			classLogger.error("Failed to convert '" + fileName + "' to '" + to + "' via unoserver at " + url, e);
			throw new IllegalArgumentException(
					"Could not convert '" + fileName + "' to '" + to + "' via unoserver: " + e.getMessage(), e);
		}
	}

	/**
	 * Convenience: converts the source file and writes the converted bytes to the
	 * given output path, creating parent directories as needed.
	 *
	 * @param source         the source document to convert
	 * @param targetFormat   the format to convert to
	 * @param outputFilePath where to write the converted document
	 * @return the written output file
	 */
	public File convertToFile(File source, String targetFormat, String outputFilePath) {
		byte[] converted = convert(source, targetFormat);
		File out = new File(outputFilePath);
		try {
			FileUtils.writeByteArrayToFile(out, converted);
		} catch (IOException e) {
			classLogger.error("Failed to write converted file to " + outputFilePath, e);
			throw new IllegalArgumentException(
					"Converted '" + source.getName() + "' but failed to save it to " + outputFilePath, e);
		}
		return out;
	}

}
