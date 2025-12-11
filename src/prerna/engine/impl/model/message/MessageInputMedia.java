package prerna.engine.impl.model.message;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tika.Tika;
import org.apache.tika.mime.MediaType;

import prerna.cluster.util.ClusterUtil;

public class MessageInputMedia {

	private static final Logger classLogger = LogManager.getLogger(MessageInputMedia.class);

	public enum MEDIA_INPUT_TYPE {
		FILE, URL
	}

	private String fileName;
	private String base64Data;
	private String fileFormat;
	private String mimeType;
	private String sourceUrl;
	private MEDIA_INPUT_TYPE mediaInputType;

	private transient String roomFolder;

	/** Factory method for file-based image */
	public static MessageInputMedia fromFile(String filePath, String roomId, String messageId, String roomFolder) {
		MessageInputMedia info = new MessageInputMedia();
		info.roomFolder = roomFolder; // /opt/semoshome/room-123123123/
		String fullFilePath = roomFolder + "/" + filePath;
		info.fileName = extractFileName(fullFilePath);
		info.fileFormat = extractFormat(info.fileName);
		info.mimeType = guessMimeType(fullFilePath, info.fileFormat);
		info.base64Data = encodeFileToBase64(fullFilePath, info.mimeType);
		info.mediaInputType = MEDIA_INPUT_TYPE.FILE;

		// Optionally, set imageUrl if you want to expose uploaded images as URLs
		ClusterUtil.pushRoom(roomId);
		return info;
	}

	/** Factory method for image URL (no file data is loaded) */
	public static MessageInputMedia fromUrl(String url) {
		MessageInputMedia info = new MessageInputMedia();
		info.sourceUrl = url;
		info.mediaInputType = MEDIA_INPUT_TYPE.URL;
		return info;
	}

	// Setters and getters

	public void setRoomFolder(String roomFolder) {
		this.roomFolder = roomFolder;
	}

	public MEDIA_INPUT_TYPE getMediaInputType() {
		return mediaInputType;
	}

	public void setMediaInputType(MEDIA_INPUT_TYPE mediaInputType) {
		this.mediaInputType = mediaInputType;
	}

	public String getFileName() {
		return fileName;
	}

	public String getFileFormat() {
		return fileFormat;
	}

	public String getMimeType() {
		return mimeType;
	}

	public String getSourceUrl() {
		return sourceUrl;
	}

	public void setSourceUrl(String sourceUrl) {
		this.sourceUrl = sourceUrl;
	}

	public String getBase64Data() {
		// Lazy load if needed (e.g. restored from DB without base64)
		if (base64Data == null && roomFolder != null && fileName != null) {
			String fullImageFilePath = roomFolder + "/" + fileName;
			base64Data = encodeFileToBase64(fullImageFilePath, mimeType);
		}
		return base64Data;
	}

	public void setBase64Data(String base64Data) {
		this.base64Data = base64Data;
	}

	// Extraction & utilities

	public static String extractFolderPath(String path) {
		if (path == null) {
			return "";
		}
		File file = new File(path);
		String parent = file.getParent();
		return parent != null ? parent.replace("\\", "/") : "";
	}

	public static String extractFileName(String path) {
		if (path == null) {
			return "";
		}
		int idx = path.lastIndexOf('/');
		if (idx == -1) {
			idx = path.lastIndexOf('\\');
		}
		if (idx != -1 && idx + 1 < path.length()) {
			return path.substring(idx + 1);
		}
		return path;
	}

	public static String extractFormat(String fileName) {
		int idx = fileName.lastIndexOf('.');
		String extension = (idx != -1 && idx + 1 < fileName.length()) ? fileName.substring(idx + 1).toLowerCase()
				: "png";
		if ("jpg".equals(extension)) {
			extension = "jpeg";
		}
		return extension;
	}

	private static String guessMimeType(String localPath, String format) {
		try {
			Path p = Paths.get(localPath);
			Tika tika = new Tika();
			String detectedType = tika.detect(p);
			MediaType mediaType = MediaType.parse(detectedType);
			if (mediaType != null) {
				MediaType baseType = mediaType.getBaseType();
				return baseType.toString();
			}
		} catch (IOException ignore) {
		}

		// fall back if tika cannot predict
		if ("jpg".equals(format) || "jpeg".equals(format)) {
			return "image/jpeg";
		}
		if ("png".equals(format)) {
			return "image/png";
		}
		if ("gif".equals(format)) {
			return "image/gif";
		}
		return "application/octet-stream";
	}
	
	private static boolean isEncodableMimeType(String mimeType) {
	    return List.of("image", "application/pdf")
	        .parallelStream()
	        .anyMatch(pattern -> mimeType.contains(pattern));
	}

	public static String encodeFileToBase64(String fullFilePath, String mimeType) {
		if (isEncodableMimeType(mimeType)) {
			try {
				byte[] fileContent = Files.readAllBytes(Paths.get(fullFilePath));
				return Base64.getEncoder().encodeToString(fileContent);
			} catch (IOException e) {
				classLogger.error("Unable to get base64 encoding of " + fullFilePath, e);
				return "";
			}
		}
		return "";
	}

	// Used for OpenAI: "data:image/png;base64,...."
	public String getFullDataUrl() {
		return "data:" + getMimeType() + ";base64," + getBase64Data();
	}

	/**
	 * For passing to LLM or APIs: Returns only relevant fields based on type.
	 */
	public Map<String, Object> toMap() {
		Map<String, Object> m = new HashMap<>();
		m.put("mediaInputType", mediaInputType != null ? mediaInputType.name().toLowerCase() : null);
		if (mediaInputType == MEDIA_INPUT_TYPE.FILE) {
			m.put("fileName", fileName);
			m.put("fileFormat", fileFormat);
			m.put("mimeType", mimeType);
			m.put("base64Data", base64Data);
		} else if (mediaInputType == MEDIA_INPUT_TYPE.URL) {
			m.put("sourceUrl", sourceUrl);
		}
		return m;
	}
}