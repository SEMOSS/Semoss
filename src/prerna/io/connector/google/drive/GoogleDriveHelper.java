package prerna.io.connector.google.drive;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;

import prerna.security.HttpHelperUtility;

import com.google.gson.reflect.TypeToken;
import org.apache.hc.core5.http.ContentType;
import prerna.util.Constants;

public class GoogleDriveHelper {

	private static final String GOOGLE_DRIVE_UPLOAD = "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart";
	private static final String GOOGLE_DRIVE_READ = "https://www.googleapis.com/drive/v3/files/%s?fields=id,name,mimeType,size";
	private static final String GOOGLE_DRIVE_FILE_READ = "https://www.googleapis.com/drive/v3/files/%s?alt=media";
	private static final String GOOGLE_DRIVE_DOWNLOAD = "https://www.googleapis.com/drive/v3/files/%s?alt=media";
	private static final String GOOGLE_DRIVE_DELETE = "https://www.googleapis.com/drive/v3/files/%s";
	private static final String boundary = "----MyBoundary" + System.currentTimeMillis();
	private static final String LINE_FEED = "\r\n";
	
	
	private static final Logger classLogger = LogManager.getLogger(GoogleDriveHelper.class);

	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping()
			.setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE).setPrettyPrinting().create();

	public static Map<String, Object> uploadFile(String accessToken, String fileName, String filePath)
			throws Exception {
		try {
			// creates a new File object that represents a file at the path specified by
			// filePath.
			File file = new File(filePath);
			// body(header and content) - multipart
			// a unique string to separate the parts(body) in multipart
			// everything between the ---boundary is considered as new/separate part
		
			// Header to send in the post body with bearer token and content type
			Map<String, String> headers = getBearerHeader(accessToken, boundary);

			// Json with name and content Type structure
			Map<String, Object> metadata = new HashMap<>();
			metadata.put("name", fileName);
			metadata.put("mimeType", mimeType(fileName));
			String metadataJson = GSON.toJson(metadata);

			// create ByteArrayOutputStream for write
			ByteArrayOutputStream baos = new ByteArrayOutputStream();

			// --MyBoundary + line feed
			// content-type : json + line feed
			// line feed - add a space
			// metadata
			// line feed
			// This is for metadata part
			baos.write(("--" + boundary + LINE_FEED).getBytes("UTF-8"));
			baos.write(("Content-Type: application/json; charset=UTF-8" + LINE_FEED).getBytes("UTF-8"));
			baos.write(LINE_FEED.getBytes("UTF-8"));
			baos.write(metadataJson.getBytes("UTF-8"));
			baos.write(LINE_FEED.getBytes("UTF-8"));

			// File part
			// --MyBoundary + line feed
			// content-type : mimetype + line feed
			// line feed - add a space
			// this is for file part
			baos.write(("--" + boundary + LINE_FEED).getBytes("UTF-8"));
			baos.write(("Content-Type: " + mimeType(fileName) + LINE_FEED).getBytes("UTF-8"));
			baos.write(LINE_FEED.getBytes("UTF-8"));

			// Read bytes form the file to be uploaded
			// We read files in chunks
			FileInputStream fis = new FileInputStream(file);
			byte[] buffer = new byte[4096];
			int bytesRead;
			while ((bytesRead = fis.read(buffer)) != -1) {
				baos.write(buffer, 0, bytesRead);
			}
			fis.close();

			// space(line feed)
			// signal ends of the multipart body
			baos.write(LINE_FEED.getBytes("UTF-8"));
			baos.write(("--" + boundary + "--" + LINE_FEED).getBytes("UTF-8"));

			String response = HttpHelperUtility.postRequestBytesBody(GOOGLE_DRIVE_UPLOAD, headers, baos.toByteArray(),
					ContentType.APPLICATION_OCTET_STREAM, null, null, null);

			Map<String, Object> json = GSON.fromJson(response, new TypeToken<Map<String, Object>>() {
			}.getType());
			Map<String, Object> result = new HashMap<>();
			result.put("id", json.get("id"));
			result.put("success", true);

			return result;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		}
	}
	
	public static Map<String, Object> readFile(String accessToken, String fileId) throws Exception {
		try {
			//metadata
			Map<String, String> headers = getBearerHeader(accessToken, boundary);
			String url = String.format(GOOGLE_DRIVE_READ, fileId);
			String response = HttpHelperUtility.getRequest(url, headers, null, null, null);
			Map<String, Object> json = GSON.fromJson(response, new TypeToken<Map<String, Object>>() {}.getType());
			String mimeType = (String) json.get("mimeType");
			Object content = null;
			//for files or text files
			if (!mimeType.contains("folder")) {
				String fileUrl = String.format(GOOGLE_DRIVE_FILE_READ, fileId);
				// done for text file and json
				if (mimeType.startsWith("text/") || mimeType.equals("application/json")) {
					content = HttpHelperUtility.getRequest(fileUrl, headers, null, null, null);
				} else {
					//Clickable link
					byte[] fileBytes = HttpHelperUtility.getRequestBytes(fileUrl, headers, null, null, null);
					content = Base64.getEncoder().encodeToString(fileBytes);
				}
			}
			Map<String, Object> map = new HashMap<>();
			map.put("content", content);
			return map;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		}
	}
	
	public static boolean downloadFile(String accessToken, String fileId, String path, String fileName) throws Exception {
		try {
			Map<String, String> headers = getBearerHeader(accessToken, boundary);
			String downloadUrl = String.format(GOOGLE_DRIVE_DOWNLOAD, fileId);
			byte[] fileBytes = HttpHelperUtility.getRequestBytes(downloadUrl, headers, null, null, null);
			String filePath = null;
			if (fileName == null || fileName.isEmpty()) {
				filePath = path;
			} else {
				filePath = Paths.get(path, fileName).toString();
			}
			//write in the path
			try (FileOutputStream fos = new FileOutputStream(filePath)) {
				fos.write(fileBytes);
			}
			
			return true;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			classLogger.warn("Failed to download file", e.getMessage());
			return false;
		}
	}
	
	public static Map<String, Object> deleteFile(String accessToken, String fileId) throws Exception {
		try {
			Map<String, String> headers = getBearerHeader(accessToken, boundary);
			String deleteUrl = String.format(GOOGLE_DRIVE_DELETE, fileId);
			HttpHelperUtility.deleteRequestStringBody(deleteUrl, headers, null, null, null);
			Map<String, Object> result = new HashMap<>();
			result.put("status", true);
			return result;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		}
	}

	private static String mimeType(String fileName) {
		if (fileName.endsWith(".jpg") || fileName.endsWith(".jpeg"))
			return "image/jpeg";
		else if (fileName.endsWith(".png"))
			return "image/png";
		else if (fileName.endsWith(".pdf"))
			return "application/pdf";
		else if (fileName.endsWith(".txt"))
			return "text/plain";
		else if (fileName.endsWith(".ppt")) 
			return "application/ppt";
		else if (fileName.endsWith(".docx"))
			return "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
		else
			return "application/octet-stream";
	}

	public static Map<String, String> getBearerHeader(String accessToken, String boundary) {
		Map<String, String> headers = new HashMap<>();
		headers.put("Authorization", "Bearer " + accessToken);
		headers.put("Content-Type", "multipart/related; boundary=" + boundary);
		return headers;
	}

}
