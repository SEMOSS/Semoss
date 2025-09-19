package prerna.io.connector.google.drive;

import java.util.Map;
import java.util.HashMap;
import java.util.List;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.file.Paths;
import java.io.ByteArrayOutputStream;

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

	private static final Gson GSON = new GsonBuilder()
			.disableHtmlEscaping()
			.setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
			.setPrettyPrinting()
			.create();
	
	private static final String ID = "id";
	private static final String SUCCESS = "success";
	private static final String boundary = "----MyBoundary" + System.currentTimeMillis();

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
		final String NAME = "name";
		final String UTF_8 = "UTF-8";
		final String SEPARATOR = "--";
		final String MIME_TYPE = "mimeType";
		final String LINE_FEED = "\r\n";
		final String CONTENT_TYPE = "Content-Type: ";
		final String CONTENT_TYPE_APPLICATION_JSON_CHARSET_UTF_8 = "Content-Type: application/json; charset=UTF-8";
		final String GOOGLE_DRIVE_UPLOAD = "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart";
		
		
		try {
			File file = new File(filePath);
			Map<String, String> headers = getBearerHeader(accessToken, boundary);
			Map<String, Object> metadata = new HashMap<>();
			metadata.put(NAME, fileName);
			metadata.put(MIME_TYPE, mimeType(fileName));
			String metadataJson = GSON.toJson(metadata);
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			
			baos.write((SEPARATOR + boundary + LINE_FEED).getBytes(UTF_8));
			baos.write((CONTENT_TYPE_APPLICATION_JSON_CHARSET_UTF_8 + LINE_FEED).getBytes(UTF_8));
			baos.write(LINE_FEED.getBytes(UTF_8));
			baos.write(metadataJson.getBytes(UTF_8));
			baos.write(LINE_FEED.getBytes(UTF_8));

			baos.write((SEPARATOR + boundary + LINE_FEED).getBytes(UTF_8));
			baos.write((CONTENT_TYPE + mimeType(fileName) + LINE_FEED).getBytes(UTF_8));
			baos.write(LINE_FEED.getBytes(UTF_8));

			FileInputStream fis = new FileInputStream(file);
			byte[] buffer = new byte[4096];
			int bytesRead;
			while ((bytesRead = fis.read(buffer)) != -1) {
				baos.write(buffer, 0, bytesRead);
			}
			fis.close();
			
			baos.write(LINE_FEED.getBytes(UTF_8));
			baos.write((SEPARATOR + boundary + SEPARATOR + LINE_FEED).getBytes(UTF_8));
			
			String response = HttpHelperUtility.postRequestBytesBody(GOOGLE_DRIVE_UPLOAD, headers, baos.toByteArray(),
					ContentType.APPLICATION_OCTET_STREAM, null, null, null);
			Map<String, Object> json = GSON.fromJson(response, new TypeToken<Map<String, Object>>() {}.getType());
			Map<String, Object> result = new HashMap<>();
			result.put(ID, json.get(ID));
			result.put(SUCCESS, true);
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
		final String VIEW_LINK = "viewLink";
		final String GOOGLE_FILE_VIEW_LINK ="https://drive.google.com/file/d/%s/view";
		final String GOOGLE_DRIVE_READ = "https://www.googleapis.com/drive/v3/files/%s";
		
		try {
			Map<String, String> headers = getBearerHeader(accessToken, boundary);
			String url = String.format(GOOGLE_DRIVE_READ, fileId);
			String response = HttpHelperUtility.getRequest(url, headers, null, null, null);
			Map<String, Object> json = GSON.fromJson(response, new TypeToken<Map<String, Object>>() {}.getType());
			String viewLink = String.format(GOOGLE_FILE_VIEW_LINK, fileId);
			Map<String, Object> map = new HashMap<>();
			map.put(ID, json.get(ID));
			map.put(VIEW_LINK, viewLink);
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
	public static void downloadFile(String accessToken, String fileId, String path, String fileName) throws Exception {
		final String GOOGLE_DRIVE_DOWNLOAD = "https://www.googleapis.com/drive/v3/files/%s?alt=media";
		
		try {
			Map<String, String> headers = getBearerHeader(accessToken, boundary);
			String downloadUrl = String.format(GOOGLE_DRIVE_DOWNLOAD, fileId);
			byte[] fileBytes = HttpHelperUtility.getRequestBytes(downloadUrl, headers, null, null, null);
			if (fileBytes == null || fileBytes.length == 0) {
				throw new IllegalStateException("Downloaded file content is empty for fileId = " + fileId);
			}
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
	public static Map<String, Object> deleteFile(String accessToken, String fileId) throws Exception {
		final String GOOGLE_DRIVE_DELETE = "https://www.googleapis.com/drive/v3/files/%s";
		
		try {
			Map<String, String> headers = getBearerHeader(accessToken, boundary);
			String deleteUrl = String.format(GOOGLE_DRIVE_DELETE, fileId);
			HttpHelperUtility.deleteRequestStringBody(deleteUrl, headers, null, null, null);
			Map<String, Object> result = new HashMap<>();
			result.put(SUCCESS, true);
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
		final String FILES = "files";
		final String GOOGLE_DRIVE_LIST = "https://www.googleapis.com/drive/v3/files?pageSize=%s&fields=files(id,name,mimeType)";
		
		try {
			Map<String, String> headers = getBearerHeader(accessToken, boundary);
			String listUrl = String.format(GOOGLE_DRIVE_LIST, limit);
			String response = HttpHelperUtility.getRequest(listUrl, headers, null, null, null);
			Map<String, Object> json = GSON.fromJson(response, new TypeToken<Map<String, Object>>() {}.getType());
			List<Map<String, Object>> files = (List<Map<String, Object>>) json.get(FILES);
			return files;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		}
	}

	private static String mimeType(String fileName) {
		final String EXT_JPG = ".jpg";
	    final String EXT_PNG = ".png";
	    final String EXT_PDF = ".pdf";
	    final String EXT_TXT = ".txt";
	    final String EXT_PPT = ".ppt";
	    final String EXT_JPEG = ".jpeg";
	    final String EXT_DOCX = ".docx";
	    final String MIME_PNG = "image/png";
	    final String MIME_TXT = "text/plain";
	    final String MIME_JPEG = "image/jpeg";
	    final String MIME_PDF = "application/pdf";
	    final String MIME_PPT = "application/vnd.ms-powerpoint";
	    final String MIME_OCTET_STREAM = "application/octet-stream";
	    final String MIME_DOCX = "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
	    
		if (fileName.endsWith(EXT_JPG) || fileName.endsWith(EXT_JPEG))
	        return MIME_JPEG;
	    else if (fileName.endsWith(EXT_PNG))
	        return MIME_PNG;
	    else if (fileName.endsWith(EXT_PDF))
	        return MIME_PDF;
	    else if (fileName.endsWith(EXT_TXT))
	        return MIME_TXT;
	    else if (fileName.endsWith(EXT_PPT))
	        return MIME_PPT;
	    else if (fileName.endsWith(EXT_DOCX))
	        return MIME_DOCX;
	    else
	        return MIME_OCTET_STREAM;
	}

	public static Map<String, String> getBearerHeader(String accessToken, String boundary) {
		final String HEADER_AUTHORIZATION = "Authorization";
		final String BEARER = "Bearer ";
	    final String HEADER_CONTENT_TYPE = "Content-Type";
	    final String MULTIPART_RELATED_BOUNDARY = "multipart/related; boundary=";
	    
		Map<String, String> headers = new HashMap<>();
		headers.put(HEADER_AUTHORIZATION, BEARER + accessToken);
		if (boundary != null && !boundary.isEmpty()) {
			headers.put(HEADER_CONTENT_TYPE, MULTIPART_RELATED_BOUNDARY + boundary);
		}
		return headers;
	}

}
