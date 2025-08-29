package prerna.io.connector.google.drive;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

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
	
	private static final Logger classLogger = LogManager.getLogger(GoogleDriveHelper.class);

	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping()
			.setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE).setPrettyPrinting().create();

	private static final String GOOGLE_DRIVE_UPLOAD = "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart";
	private static final String GOOGLE_DRIVE_READ = "https://www.googleapis.com/drive/v3/files/%s";
	private static final String GOOGLE_DRIVE_DOWNLOAD = "https://www.googleapis.com/drive/v3/files/%s?alt=media";
	private static final String GOOGLE_DRIVE_DELETE = "https://www.googleapis.com/drive/v3/files/%s";
	private static final String GOOGLE_DRIVE_LIST = "https://www.googleapis.com/drive/v3/files?pageSize=%s&fields=files(id,name,mimeType)";
	private static final String GOOGLE_FILE_VIEW_LINK ="https://drive.google.com/file/d/%s/view";
	private static final String boundary = "----MyBoundary" + System.currentTimeMillis();
	private static final String LINE_FEED = "\r\n";

	private GoogleDriveHelper() {

	}

	/**
	 * 
	 * @param accessToken
	 * @param fileName
	 * @param filePath
	 * @return
	 * @throws Exception
	 */
	public static Map<String, Object> uploadFile(String accessToken, String fileName, String filePath)
			throws Exception {
		try {
			File file = new File(filePath);
			Map<String, String> headers = getBearerHeader(accessToken, boundary);

			Map<String, Object> metadata = new HashMap<>();
			metadata.put("name", fileName);
			metadata.put("mimeType", mimeType(fileName));
			String metadataJson = GSON.toJson(metadata);

			ByteArrayOutputStream baos = new ByteArrayOutputStream();

			baos.write(("--" + boundary + LINE_FEED).getBytes("UTF-8"));
			baos.write(("Content-Type: application/json; charset=UTF-8" + LINE_FEED).getBytes("UTF-8"));
			baos.write(LINE_FEED.getBytes("UTF-8"));
			baos.write(metadataJson.getBytes("UTF-8"));
			baos.write(LINE_FEED.getBytes("UTF-8"));

			baos.write(("--" + boundary + LINE_FEED).getBytes("UTF-8"));
			baos.write(("Content-Type: " + mimeType(fileName) + LINE_FEED).getBytes("UTF-8"));
			baos.write(LINE_FEED.getBytes("UTF-8"));

			FileInputStream fis = new FileInputStream(file);
			byte[] buffer = new byte[4096];
			int bytesRead;
			while ((bytesRead = fis.read(buffer)) != -1) {
				baos.write(buffer, 0, bytesRead);
			}
			fis.close();

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
	
	/**
	 * 
	 * @param accessToken
	 * @param fileId
	 * @return
	 * @throws Exception
	 */
	public static Map<String, Object> readFile(String accessToken, String fileId) throws Exception {
		try {
			Map<String, String> headers = getBearerHeader(accessToken, boundary);
			String url = String.format(GOOGLE_DRIVE_READ, fileId);
			
			String response = HttpHelperUtility.getRequest(url, headers, null, null, null);
			Map<String, Object> json = GSON.fromJson(response, new TypeToken<Map<String, Object>>() {}.getType());
			
			String viewLink = String.format(GOOGLE_FILE_VIEW_LINK, fileId);
			Map<String, Object> map = new HashMap<>();
			map.put("id", json.get("id"));
			map.put("link", viewLink);
			return map;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		}
	}
	
	/**
	 * 
	 * @param accessToken
	 * @param fileId
	 * @param path
	 * @param fileName
	 * @return
	 * @throws Exception
	 */
	public static boolean downloadFile(String accessToken, String fileId, String path, String fileName) throws Exception {
		try {
			Map<String, String> headers = getBearerHeader(accessToken, boundary);
			String downloadUrl = String.format(GOOGLE_DRIVE_DOWNLOAD, fileId);
			
			byte[] fileBytes = HttpHelperUtility.getRequestBytes(downloadUrl, headers, null, null, null);
			
			String filePath;
			if (fileName == null || fileName.isEmpty()) {
				filePath = path;
			} else {
				filePath = Paths.get(path, fileName).toString();
			}
			
			File file = new File(filePath);
			
	        File parent = file.getParentFile();
	        if (parent != null && !parent.exists()) {
	            parent.mkdirs();
	        }
			try (FileOutputStream fos = new FileOutputStream(file)) {
				fos.write(fileBytes);
				fos.flush();
			}
			
			return true;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			classLogger.warn("Failed to download file", e.getMessage());
			return false;
		}
	}
	
	/**
	 * 
	 * @param accessToken
	 * @param fileId
	 * @return
	 * @throws Exception
	 */
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
	
	/**
	 * 
	 * @param accessToken
	 * @param fileName
	 * @param limit
	 * @return
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public static List<Map<String, Object>> fileIdList(String accessToken, int limit) throws Exception {
		try {
			Map<String, String> headers = getBearerHeader(accessToken, boundary);
			String listUrl = String.format(GOOGLE_DRIVE_LIST, limit);
			String response = HttpHelperUtility.getRequest(listUrl, headers, null, null, null);
			Map<String, Object> json = GSON.fromJson(response, new TypeToken<Map<String, Object>>() {}.getType());
			List<Map<String, Object>> files = (List<Map<String, Object>>) json.get("files");
			return files;
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
			return "application/vnd.ms-powerpoint";
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
