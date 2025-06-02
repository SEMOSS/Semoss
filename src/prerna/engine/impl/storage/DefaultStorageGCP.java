package prerna.engine.impl.storage;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import prerna.engine.api.StorageTypeEnum;
import prerna.util.Utility;
import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import java.util.Base64;
import java.util.UUID;
import java.io.ByteArrayInputStream;
import java.io.InputStream;


public class DefaultStorageGCP extends GoogleCloudNativeBlobStorageEngine {
	
	private static final Logger classLogger = LogManager.getLogger(DefaultStorageGCP.class);
	
	/**
	 * Verifies that the required folder structure exists for a user in GCS.
	 * Creates the folders if they don't exist.
	 * 
	 * @param userId the user ID to create folder structure for
	 * @return true if verification/creation was successful, false otherwise
	 */
	private boolean verifyUserFolderStructure(String userId) {
		if (userId == null || userId.isEmpty()) {
			classLogger.error("User ID is null or empty");
			return false;
		}
		
		String userPath = userId + "/";
		String imagePath = userPath + "images/";
		String audioPath = userPath + "audio/";
		
		try {
			// Check and create user root folder
			if (!folderExists(userPath)) {
				createFolder(userPath);
				classLogger.info("Created user folder: " + userPath);
			} else {
				classLogger.debug("User folder already exists: " + userPath);
			}
			
			// Check and create images folder
			if (!folderExists(imagePath)) {
				createFolder(imagePath);
				classLogger.info("Created images folder: " + imagePath);
			} else {
				classLogger.debug("Images folder already exists: " + imagePath);
			}
			
			// Check and create audio folder
			if (!folderExists(audioPath)) {
				createFolder(audioPath);
				classLogger.info("Created audio folder: " + audioPath);
			} else {
				classLogger.debug("Audio folder already exists: " + audioPath);
			}
			
			return true;
			
		} catch (Exception e) {
			classLogger.error("Failed to verify/create user folder structure for user: " + userId, e);
			return false;
		}
	}
	
	/**
	 * Checks if a folder (represented as a zero-byte blob ending with '/') exists in GCS
	 * 
	 * @param folderPath the folder path to check (should end with '/')
	 * @return true if folder exists, false otherwise
	 */
	private boolean folderExists(String folderPath) {
		try {
			// Ensure the path ends with '/' for folder representation
			if (!folderPath.endsWith("/")) {
				folderPath += "/";
			}
			
			// Check if there's a blob with this exact name (folder placeholder)
			BlobId blobId = BlobId.of(this.BUCKET, folderPath);
			Blob blob = this.storage.get(blobId);
			
			if (blob != null) {
				return true;
			}
			
			// Also check if there are any files with this prefix (folder with content)
			Page<Blob> blobs = this.bucket.list(Storage.BlobListOption.prefix(folderPath), 
											   Storage.BlobListOption.pageSize(1));
			return blobs.iterateAll().iterator().hasNext();
			
		} catch (Exception e) {
			classLogger.error("Error checking if folder exists: " + folderPath, e);
			return false;
		}
	}
	
	/**
	 * Creates a folder in GCS by creating a zero-byte blob ending with '/'
	 * 
	 * @param folderPath the folder path to create (should end with '/')
	 * @throws Exception if folder creation fails
	 */
	private void createFolder(String folderPath) throws Exception {
		// Ensure the path ends with '/' for folder representation
		if (!folderPath.endsWith("/")) {
			folderPath += "/";
		}
		
		BlobId blobId = BlobId.of(this.BUCKET, folderPath);
		BlobInfo blobInfo = BlobInfo.newBuilder(blobId)
			.setContentType("application/x-directory")
			.build();
		
		// Create an empty blob to represent the folder
		this.storage.create(blobInfo, "".getBytes(StandardCharsets.UTF_8));
		classLogger.info("Successfully created folder: " + folderPath);
	}
	
	private String getUserId(User user) {
		String userEpoch = user.getUserEpoch();
		for (AuthProvider provider : user.getLogins()) {
			String providerName = provider.name();
			AccessToken token = user.getAccessToken(provider);
			
			String userId = token.getId();
			if (userId != null) {
				return userId;
			}
		}
			
		classLogger.error("Could not extract User ID from User");
		return null;
	}
	
	private byte[] decodeBase64(String base64String) throws Exception {
        // Remove data URL prefix if present (e.g., "data:image/png;base64,")
        String cleanBase64 = base64String;
        if (base64String.contains(",")) {
            cleanBase64 = base64String.substring(base64String.indexOf(",") + 1);
        }
        
        // Extract image format from data URL prefix (default to jpg if not found)
        String imageFormat = "jpg";
        if (base64String.startsWith("data:image/")) {
            String prefix = base64String.substring(0, base64String.indexOf(","));
            if (prefix.contains("png")) {
                imageFormat = "png";
            } else if (prefix.contains("gif")) {
                imageFormat = "gif";
            } else if (prefix.contains("webp")) {
                imageFormat = "webp";
            } else if (prefix.contains("jpeg") || prefix.contains("jpg")) {
                imageFormat = "jpg";
            }
        }
        
        try {
            return Base64.getDecoder().decode(cleanBase64);
        } catch (IllegalArgumentException e) {
            classLogger.error("Invalid base64 string format", e);
            return null;
        }
	}
	
	private String getImageFormat(String base64String) throws Exception {
        String imageFormat = "jpg";
        if (base64String.startsWith("data:image/")) {
            String prefix = base64String.substring(0, base64String.indexOf(","));
            if (prefix.contains("png")) {
                imageFormat = "png";
            } else if (prefix.contains("gif")) {
                imageFormat = "gif";
            } else if (prefix.contains("webp")) {
                imageFormat = "webp";
            } else if (prefix.contains("jpeg") || prefix.contains("jpg")) {
                imageFormat = "jpg";
            } else {
            	classLogger.error("Invalid image format found in DefaultStorageGCP");
                throw new Exception("Unsupported image format in base64 string"); 
            }
        }
        return imageFormat;
	}
	
	
	public boolean uploadImage(String base64String, User user) throws Exception {
		
        if (base64String == null || base64String.isEmpty()) {
            classLogger.error("Base64 string is null or empty");
            return false;
        }
        
        if (user == null) {
            classLogger.error("User is null");
            return false;
        }
        
        try {
        	String userId = getUserId(user);
        	
        	// Converting image from base64 
	        byte[] imageBytes = decodeBase64(base64String);
	        if (imageBytes == null) return false;
	        
	        String fileName = UUID.randomUUID().toString();
	        String imageFormat = getImageFormat(base64String);
	        
	        String blobPath = userId + "/images/" + fileName + "." + imageFormat;
	        BlobId blobId = BlobId.of(this.BUCKET, blobPath);
            BlobInfo blobInfo = BlobInfo.newBuilder(blobId)
                    .setContentType("image/" + imageFormat)
                    .build();
            
            this.storage.create(blobInfo, imageBytes);
            classLogger.info("Successfully uploaded image: " + blobPath);
            return true;
        }catch(Exception e){
        	classLogger.error("DefaultStorageEngine failed to convert image from base64.", e);
        	return false;
        }
	}

}
