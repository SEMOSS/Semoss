package prerna.sablecc2.reactor.images;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.io.connector.couch.CouchException;
import prerna.io.connector.couch.CouchUtil;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

/**
 * Reactor to upload the images in the database and project cluster folders to
 * CouchDB. There are optional input keys to indicate if the remote folders
 * should be re-synced, if images should be filtered to existing databases and
 * projects, or if we should check the whether the image is already in CouchDB
 * before upload.
 */
public class MigrateImagesToCouchReactor extends AbstractReactor {

	private static final Logger LOGGER = LogManager.getLogger(MigrateImagesToCouchReactor.class);

	private static final String SELECTOR_TEMPLATE = "{\"selector\": {\"%s\": {\"$eq\": \"%s\"}, \"_attachments\": {\"%s\": {\"digest\": {\"$eq\": \"%s\"}}}}}";
	private static final String VERIFY_MISSING = "verifyMissing";
	private static final String REPULL = "repull";
	private static final String FILTER_INVALID_IDS = "filterInvalidIds";

	private MessageDigest md5Instance;
	private boolean verifyMissing = true;
	private boolean repull = false;
	private boolean filterInvalidIds = true;

	public MigrateImagesToCouchReactor() throws NoSuchAlgorithmException {
		md5Instance = MessageDigest.getInstance("MD5");

		this.keysToGet = new String[] { VERIFY_MISSING, REPULL, FILTER_INVALID_IDS};
		this.keyRequired = new int[] { 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		loadInputs();
		
		List<String> outcomes = new Vector<>();
		uploadRemoteToCouch(ClusterUtil.IMAGES_FOLDER_PATH + DIR_SEPARATOR + "databases",
				SecurityEngineUtils.getAllEngineIds(), CouchUtil.DATABASE, outcomes);
		uploadRemoteToCouch(ClusterUtil.IMAGES_FOLDER_PATH + DIR_SEPARATOR + "storages",
				SecurityEngineUtils.getAllEngineIds(), CouchUtil.STORAGE, outcomes);
		uploadRemoteToCouch(ClusterUtil.IMAGES_FOLDER_PATH + DIR_SEPARATOR + "models",
				SecurityEngineUtils.getAllEngineIds(), CouchUtil.MODEL, outcomes);
		uploadRemoteToCouch(ClusterUtil.IMAGES_FOLDER_PATH + DIR_SEPARATOR + "projects",
				SecurityProjectUtils.getAllProjectIds(), CouchUtil.PROJECT, outcomes);
		
		return new NounMetadata(outcomes, PixelDataType.VECTOR);
	}

	private void loadInputs() {
		this.organizeKeys();
		
		// user must be an admin
		// and couch must be enabled
		
		User user = this.insight.getUser();
		if(!SecurityAdminUtils.userIsAdmin(user)) {
			SemossPixelException err = new SemossPixelException(
					new NounMetadata("User must be an admin to perform this operation", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			err.setContinueThreadOfExecution(false);
			throw err;
		}
		if (!CouchUtil.COUCH_ENABLED || !ClusterUtil.IS_CLUSTER) {
			SemossPixelException err = new SemossPixelException(
					new NounMetadata("CouchDB and Clustering need to be enabled to use this reactor",
							PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			err.setContinueThreadOfExecution(false);
			throw err;
		}

		if (this.keyValue.containsKey(REPULL)) {
			repull = Boolean.parseBoolean(this.keyValue.get(REPULL));
		}
		if (repull) {
			try {
				ClusterUtil.pullDatabaseImageFolder();
				ClusterUtil.pullProjectImageFolder();
			} catch (Exception e) {
				LOGGER.warn("Error pulling cloud image folders: " + e.getMessage(), e);
				SemossPixelException err = new SemossPixelException(
						new NounMetadata("Failed to pull images from cloud storage", PixelDataType.CONST_STRING,
								PixelOperationType.ERROR));
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
		
		if (this.keyValue.containsKey(VERIFY_MISSING)) {
			verifyMissing = Boolean.parseBoolean(this.keyValue.get(VERIFY_MISSING));
		}
		
		if (this.keyValue.containsKey(FILTER_INVALID_IDS)) {
			filterInvalidIds = Boolean.parseBoolean(this.keyValue.get(FILTER_INVALID_IDS));
		}
	}

	private void uploadRemoteToCouch(String imageFolderPath, List<String> ids, String partition,
			List<String> outcomes) {
		File[] images = new File(imageFolderPath).listFiles(new FileFilter() {
			@Override
			public boolean accept(File pathname) {
				String filePath = pathname.getAbsolutePath();
				if (!filterInvalidIds || ids.contains(FilenameUtils.getBaseName(filePath))) {
					String ext = FilenameUtils.getExtension(filePath);
					return (ext.equalsIgnoreCase("png") || ext.equalsIgnoreCase("jpeg") || ext.equalsIgnoreCase("jpg")
							|| ext.equalsIgnoreCase("gif") || ext.equalsIgnoreCase("svg"));
				}
				return false;
			}
		});
		if (images == null || images.length == 0) {
			outcomes.add(String.format("No images found to upload for %ss", partition));
		} else {
			Map<String, String> referenceData = new HashMap<>();
			for (File f : images) {
				String fPath = f.getAbsolutePath();
				String identifier = FilenameUtils.getBaseName(fPath);

				if (verifyMissing) {
					// check if the attachment is already uploaded
					try {
						String attachmentName = "image\\\\." + FilenameUtils.getExtension(fPath);
						String digest = "md5-"
								+ new String(Base64.encodeBase64(md5Instance.digest(Files.readAllBytes(f.toPath()))));
						String searchSelector = String.format(SELECTOR_TEMPLATE, partition, identifier, attachmentName,
								digest);
						if(CouchUtil.documentExists(partition, searchSelector)) {
							outcomes.add(String.format("Image %s already uploaded", fPath));
							continue;
						}
					} catch (CouchException | IOException e) {
						// on search error, we will attempt upload
						LOGGER.warn("Exception encountered while searching for image: " + e.getMessage(), e);
					}
				}

				// upload
				referenceData.put(partition, identifier);
				try {
					CouchUtil.upload(partition, referenceData, f);
					outcomes.add(String.format("Image %s uploaded successfully", fPath));
				} catch (CouchException e) {
					outcomes.add(String.format("Image %s upload failure", fPath));
				}
			}
		}
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(VERIFY_MISSING)) {
			return "Boolean flag indicating if we should check if images are already uploaded to CouchDB (default true)";
		} else if (key.equals(REPULL)) {
			return "Boolean flag indicating if we should re-pull image files from the remote repo (default false)";
		} else if (key.equals(FILTER_INVALID_IDS)) {
			return "Boolean flag indicating if only images with identifiers matching existing databases and projects should be uploaded (default true)";
		}
		return super.getDescriptionForKey(key);
	}

	@Override
	public String getReactorDescription() {
		return "Reactor to migrate images from remote image folders to CouchDB";
	}

}
