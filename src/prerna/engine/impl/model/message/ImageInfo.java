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
package prerna.engine.impl.model.message;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import org.apache.tika.Tika;
import org.apache.tika.mime.MediaType;
import prerna.cluster.util.ClusterUtil;

public class ImageInfo {

  public enum ImageSourceType {
    FILE,
    URL
  }

  //    private String folderPath;
  private String fileName;
  private String base64Data;
  private String fileFormat;
  private String mimeType;
  private String imageUrl;
  private ImageSourceType imageType;
  private transient String roomFolder; // Not persisted

  /** Factory method for file-based image */
  public static ImageInfo fromFile(
      String filePath, String roomId, String messageId, String roomFolder) {
    ImageInfo info = new ImageInfo();
    info.roomFolder = roomFolder; // /opt/semoshome/room-123123123/
    String fullFilePath = roomFolder + "/" + filePath;
    info.fileName = extractFileName(fullFilePath);
    //        info.folderPath = extractFolderPath(filePath);
    info.fileFormat = extractFormat(info.fileName);
    info.mimeType = guessMimeType(fullFilePath, info.fileFormat);
    info.base64Data = encodeFileToBase64(fullFilePath);
    info.imageType = ImageSourceType.FILE;
    // Optionally, set imageUrl if you want to expose uploaded images as URLs
    ClusterUtil.pushRoom(roomId);
    return info;
  }

  /** Factory method for image URL (no file data is loaded) */
  public static ImageInfo fromUrl(String url) {
    ImageInfo info = new ImageInfo();
    info.imageUrl = url;
    info.imageType = ImageSourceType.URL;
    return info;
  }

  // Setters and getters

  public void setRoomFolder(String roomFolder) {
    this.roomFolder = roomFolder;
  }

  public ImageSourceType getImageType() {
    return imageType;
  }

  public void setImageType(ImageSourceType imageType) {
    this.imageType = imageType;
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

  public String getImageUrl() {
    return imageUrl;
  }

  public void setImageUrl(String imageUrl) {
    this.imageUrl = imageUrl;
  }

  public String getBase64Data() {
    // Lazy load if needed (e.g. restored from DB without base64)
    if (base64Data == null && roomFolder != null && fileName != null) {
      String fullImageFilePath = roomFolder + "/" + fileName;
      base64Data = encodeFileToBase64(fullImageFilePath);
    }
    return base64Data;
  }

  public void setBase64Data(String base64Data) {
    this.base64Data = base64Data;
  }

  // Extraction & utilities

  public static String extractFolderPath(String path) {
    if (path == null) return "";
    File file = new File(path);
    String parent = file.getParent();
    return parent != null ? parent.replace("\\", "/") : "";
  }

  public static String extractFileName(String path) {
    if (path == null) return "";
    int idx = path.lastIndexOf('/');
    if (idx == -1) idx = path.lastIndexOf('\\');
    if (idx != -1 && idx + 1 < path.length()) return path.substring(idx + 1);
    return path;
  }

  public static String extractFormat(String fileName) {
    int idx = fileName.lastIndexOf('.');
    String extension =
        (idx != -1 && idx + 1 < fileName.length())
            ? fileName.substring(idx + 1).toLowerCase()
            : "png";
    if ("jpg".equals(extension)) extension = "jpeg";
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
    // Fallback:
    if ("jpg".equals(format) || "jpeg".equals(format)) return "image/jpeg";
    if ("png".equals(format)) return "image/png";
    if ("gif".equals(format)) return "image/gif";
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

  /** For passing to LLM or APIs: Returns only relevant fields based on type. */
  public Map<String, Object> toMap() {
    Map<String, Object> m = new HashMap<>();
    m.put("imageType", imageType != null ? imageType.name().toLowerCase() : null);
    if (imageType == ImageSourceType.FILE) {
      m.put("fileName", fileName);
      m.put("fileFormat", fileFormat);
      m.put("mimeType", mimeType);
      m.put("base64", base64Data);
    } else if (imageType == ImageSourceType.URL) {
      m.put("imageUrl", imageUrl);
    }
    return m;
  }
}
