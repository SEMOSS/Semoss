package prerna.engine.impl.model.message;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import prerna.cluster.util.ClusterUtil;


public class ImageInfo {
	private String folderPath;
	private String fileName;
	private transient String base64Data; // transient to not store into the db.
	private String format;
	private String mimeType;
	private transient String roomFolder;

	public static ImageInfo fromFile(String filePath, String userId, String roomId, String messageId, String roomFolder) {
		// create the image info object from file
		ImageInfo info = new ImageInfo();
		
		info.roomFolder = roomFolder; // ex. /opt/semosshome/room-123123123/
		String fullFilePath = roomFolder + "/" + filePath;
		info.fileName = extractFileName(fullFilePath);

		info.folderPath = extractFolderPath(filePath); // ? what does this do?

		info.format = extractFormat(info.fileName);
		info.mimeType = guessMimeType(fullFilePath, info.format);

		info.base64Data = encodeFileToBase64(fullFilePath);

		//finally push image to the cloud
		ClusterUtil.pushRoom(roomId);
		return info;
	}

	public void setRoomFolder(String roomFolder) {
		this.roomFolder = roomFolder;

	}

	public static String extractFolderPath(String path) {
		if (path == null)
			return "";
		File file = new File(path);
		String parent = file.getParent();
		return parent != null ? parent.replace("\\", "/") : "";
	}

	public static String extractFileName(String path) {
		if (path == null)
			return "";
		int idx = path.lastIndexOf('/');
		if (idx == -1)
			idx = path.lastIndexOf('\\');
		if (idx != -1 && idx + 1 < path.length())
			return path.substring(idx + 1);
		return path;
	}

	public static String extractFormat(String fileName) {
		int idx = fileName.lastIndexOf('.');
		String extension = (idx != -1 && idx + 1 < fileName.length()) ? fileName.substring(idx + 1).toLowerCase()
				: "png";
		if ("jpg".equals(extension))
			extension = "jpeg";
		return extension;
	}

	public static String guessMimeType(String localPath, String format) {
		try {
			String mime = Files.probeContentType(Paths.get(localPath));
			if (mime != null)
				return mime;
		} catch (IOException ignore) {
		}
// Fallback:
		if (format.equals("jpg") || format.equals("jpeg"))
			return "image/jpeg";
		if (format.equals("png"))
			return "image/png";
		if (format.equals("gif"))
			return "image/gif";
		return "application/octet-stream";
	}

	public static String encodeFileToBase64(String fullFilePath) {
		try {
			byte[] fileContent = Files.readAllBytes(Paths.get(fullFilePath));
			return Base64.getEncoder().encodeToString(fileContent);
		} catch (IOException e) {
			e.printStackTrace();
			return "";
		}
	}

// Used for OpenAI: "data:image/png;base64,...."
	public String getFullDataUrl() {
		return "data:" + getMimeType() + ";base64," + getBase64Data();
	}

	public String getBase64Data() {

		String fullImageFilePath = roomFolder + "/" + folderPath + "/" + fileName;

		if (base64Data == null && fullImageFilePath != null) {

			base64Data = encodeFileToBase64(fullImageFilePath); // Load/calculate on demand
		}
		return base64Data;
	}

//public String getLocalPath()   { return localPath; }
	public String getFolderPath() {
		return folderPath;
	}

	public String getFileName() {
		return fileName;
	}

	public String getFormat() {
		return format;
	}

	public String getMimeType() {
		return mimeType;
	}

	public Map<String, Object> toMap() {
		Map<String, Object> m = new HashMap<>();
		m.put("fileName", fileName);
		m.put("filePath", folderPath);
		m.put("format", format);
		m.put("mimeType", mimeType);
		m.put("base64", base64Data);
		return m;
	}

	public void setBase64Data(String base64Data) {
		this.base64Data = base64Data;
	}

}