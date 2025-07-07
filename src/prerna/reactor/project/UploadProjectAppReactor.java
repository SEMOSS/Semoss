package prerna.reactor.project;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.reflect.TypeToken;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.engine.impl.LegacyToProjectRestructurerHelper;
import prerna.engine.impl.SmssUtilities;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EngineUtility;
import prerna.util.UploadInputUtility;
import prerna.util.UploadUtilities;
import prerna.util.Utility;
import prerna.util.ZipUtils;
import prerna.util.gson.GsonUtility;

public class UploadProjectAppReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(UploadProjectAppReactor.class);

	private static final String CLASS_NAME = UploadProjectAppReactor.class.getName();
	
	private static final Pattern UUID_PATTERN = Pattern.compile("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$");


	public UploadProjectAppReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey(), ReactorKeysEnum.GLOBAL.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		Logger logger = this.getLogger(CLASS_NAME);
		int step = 1;
		String zipFilePath = UploadInputUtility.getFilePath(this.store, this.insight);
		// do we want this project to be accessible to everyone
		boolean global = Boolean.parseBoolean(this.keyValue.get(ReactorKeysEnum.GLOBAL.getKey())+"");
		// check security
		// Need to check this, will the same methods work/enhanced to check the
		// permissions on project?
		User user = this.insight.getUser();
		LegacyToProjectRestructurerHelper legacyToProjectRestructurerHelper = new LegacyToProjectRestructurerHelper();
		if (user == null) {
			NounMetadata noun = new NounMetadata(
					"User must be signed into an account in order to create or upload a project",
					PixelDataType.CONST_STRING, PixelOperationType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR);
			SemossPixelException err = new SemossPixelException(noun);
			err.setContinueThreadOfExecution(false);
			throw err;
		}

		// throw error if user is anonymous
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		// throw error is user doesn't have rights to publish new apps
		if (AbstractSecurityUtils.adminSetPublisher() && !SecurityQueryUtils.userIsPublisher(user)) {
			throwUserNotPublisherError();
		}

		if (AbstractSecurityUtils.adminOnlyProjectAdd() && !SecurityAdminUtils.userIsAdmin(user)) {
			AbstractReactor.throwFunctionalityOnlyExposedForAdminsError();
		}
		
		if (global && 
				(AbstractSecurityUtils.adminOnlyProjectSetPublic() && !SecurityAdminUtils.userIsAdmin(user))) {
			SemossPixelException exception = new SemossPixelException(NounMetadata.getErrorNounMessage("User can upload an app but cannot make the app public"));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}

		// creating a temp folder to unzip project folder and smss
		String randomIdAsDir = UUID.randomUUID().toString();
		String projectFolderPath = EngineUtility.getLocalEngineBaseDirectory(IEngine.CATALOG_TYPE.PROJECT);
		String randomTempUnzipFolderPath = projectFolderPath + DIR_SEPARATOR + randomIdAsDir;
		File randomTempUnzipF = new File(randomTempUnzipFolderPath);

		// gotta keep track of the smssFile and files unzipped
		Map<String, List<String>> filesAdded = new HashMap<>();
		List<String> fileList = new ArrayList<>();
		String smssFileLoc = null;
		File smssFile = null;
		// unzip files to temp project folder
		boolean error = false;
		try {
			logger.info(step + ") Unzipping project");
			filesAdded = ZipUtils.unzip(zipFilePath, randomTempUnzipFolderPath);
			logger.info(step + ") Done");
			step++;

			// look for smss file
			fileList = filesAdded.get("FILE");
			logger.info(step + ") Searching for smss");
			for (String filePath : fileList) {
				if (filePath.endsWith(Constants.SEMOSS_EXTENSION)) {
					smssFileLoc = randomTempUnzipFolderPath + DIR_SEPARATOR + filePath;
					smssFile = new File(Utility.normalizePath(smssFileLoc));
					// check if the file exists
					if (!smssFile.exists()) {
						// invalid file need to delete the files unzipped
						smssFileLoc = null;
					}
					break;
				}
			}
			logger.info(step + ") Done");
			step++;

			// delete the files if we were unable to find the smss file
			if (smssFileLoc == null) {
				throw new SemossPixelException("Unable to find " + Constants.SEMOSS_EXTENSION + " file", false);
			}
		} catch (SemossPixelException e) {
			error = true;
			throw e;
		} catch (Exception e) {
			error = true;
			logger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("Error occurred while unzipping the files", false);
		} finally {
			if (error) {
				cleanUpFolders(randomTempUnzipF);
			}
		}
		
		boolean replace = false;
		String projects = (String) DIHelper.getInstance().getProjectProperty(Constants.PROJECTS);
		String projectId = null;
		String projectName = null;
		File finalProjectSmssF = null;
		File finalProjectFolderF = null;
		File finalProjectVersionF = null;
		File finalProjectAssetF = null;
		try {
			logger.info(step + ") Reading smss");
			Properties prop = Utility.loadProperties(smssFileLoc);
			projectId = prop.getProperty(Constants.PROJECT);
			projectName = Utility.normalizePath(prop.getProperty(Constants.PROJECT_ALIAS));
			
			logger.info(step + ") Done");
			step++;

			finalProjectFolderF = new File(Utility.normalizePath(projectFolderPath + DIR_SEPARATOR + SmssUtilities.getUniqueName(projectName, projectId)));
			finalProjectSmssF = new File(Utility.normalizePath(projectFolderPath + DIR_SEPARATOR + SmssUtilities.getUniqueName(projectName, projectId) + Constants.SEMOSS_EXTENSION));

			// is this an update or first time upload
			if(SecurityProjectUtils.projectExists(projectId)) {
				// this is an update
				// do we allow update?
				// if yes, do you have access to update?
				if(deleteIfExisting()) {
					replace = true;
					if(!SecurityProjectUtils.userIsOwner(user, projectId)) {
						SemossPixelException exception = new SemossPixelException(
								NounMetadata.getErrorNounMessage("User is not an owner to replace the existing project with id = " + projectId));
						exception.setContinueThreadOfExecution(false);
						throw exception;
					} else {
						// make sure we pull the project from cloud
						IProject project = Utility.getProject(projectId);
						project.close();
						// now delete the project folder - we will make a new one when we copy from temp dir
			            FileUtils.deleteDirectory(finalProjectFolderF);
			            // now delete the project smss - we will make a new one when we copy from temp dir
			            FileUtils.delete(finalProjectSmssF);
					}
				} else {
					SemossPixelException exception = new SemossPixelException(
							NounMetadata.getErrorNounMessage("Project id already exists"));
					exception.setContinueThreadOfExecution(false);
					throw exception;
				}
			} else {
				// first time upload
				// need to ignore file watcher
				if (!(projects.startsWith(projectId) || projects.contains(";" + projectId + ";")
						|| projects.endsWith(";" + projectId))) {
					String newProjects = projects + ";" + projectId;
					DIHelper.getInstance().setProjectProperty(Constants.PROJECTS, newProjects);
				}
			}
			
			// create the project folder
			// since this assumes we have only the assets
			// make the project name / app root / version / asset folder path
			finalProjectVersionF = new File(finalProjectFolderF.getAbsolutePath() 
					+ DIR_SEPARATOR + Constants.APP_ROOT_FOLDER 
					+ DIR_SEPARATOR + Constants.VERSION_FOLDER);
			finalProjectAssetF = new File(finalProjectVersionF.getAbsolutePath()
					+ DIR_SEPARATOR + Constants.ASSETS_FOLDER);
			finalProjectAssetF.mkdirs();

			// move project folder
			logger.info(step + ") Moving project folder");
			File[] allFiles = randomTempUnzipF.listFiles();
			for(File f : allFiles) {
				if(f.getName().equals("assets") && f.isDirectory()) {
					// we move the assets folder into the version folder
					FileUtils.copyDirectory(f, finalProjectAssetF);
				} else if(f.isDirectory()) {
					FileUtils.copyDirectory(f, finalProjectVersionF);
				} else if(f.isFile()) {
					// this way the metadata files are in the same location
					// if it is UploadProject or UploadProjectApp
					FileUtils.copyFileToDirectory(f, finalProjectFolderF, true);
				}
			}
			logger.info(step + ") Done");

			step++;
			// move smss file which would have been already copied into the project folder 
			logger.info(step + ") Moving smss file");
			File tempUnzippedSmssF = new File(Utility.normalizePath(
					finalProjectFolderF + DIR_SEPARATOR + SmssUtilities.getUniqueName(projectName, projectId) + Constants.SEMOSS_EXTENSION));
			FileUtils.moveFile(tempUnzippedSmssF, finalProjectSmssF);
			logger.info(step + ") Done");
			step++;

		} catch (Exception e) {
			error = true;
			logger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(e.getMessage(), false);
		} finally {
			if (error) {
				// remove from DIHelper
				UploadUtilities.removeProjectFromDIHelper(projectId);
				cleanUpFolders(randomTempUnzipF, finalProjectSmssF, finalProjectFolderF);
			} else {
				// just delete the temp project folder
				cleanUpFolders(randomTempUnzipF);
			}
		}

		try {
			DIHelper.getInstance().setProjectProperty(projectId + "_" + Constants.STORE, finalProjectSmssF.getAbsolutePath());
			logger.info(step + ") Grabbing project insights");
			if(!replace) {
				SecurityProjectUtils.addProject(projectId, global, user);
			}
			
			// see if we have any dependencies or metadata to load
			{
				File metadataFile = new File(finalProjectFolderF.getAbsolutePath() + "/" + projectName + IEngine.METADATA_FILE_SUFFIX);
				if(metadataFile.exists() && metadataFile.isFile()) {
					Map<String, Object> metadata = (Map<String, Object>) GsonUtility.readJsonFileToObject(metadataFile, new TypeToken<Map<String, Object>>() {}.getType());
					SecurityProjectUtils.updateProjectMetadata(projectId, metadata);
					// delete this file since values can update and file is dynamically generated on export
					metadataFile.delete();
				}
				
				File dependenciesFile = new File(finalProjectFolderF.getAbsolutePath() + "/" + projectName + IProject.DEPENDENCIES_FILE_SUFFIX);
				if(dependenciesFile.exists() && dependenciesFile.isFile()) {
					List<Map<String, Object>> projectDependencies = (List<Map<String, Object>>) GsonUtility.readJsonFileToObject(dependenciesFile, new TypeToken<List<Map<String, Object>>>() {}.getType());
					// List<String> dependentEngineIds = (List<String>) GsonUtility.readJsonFileToObject(dependenciesFile, new TypeToken<List<String>>() {}.getType());
					if(projectDependencies != null && !projectDependencies.isEmpty()) {
						List<String> dependentEngineIds = new ArrayList<>();
						for(Map<String, Object> dep : projectDependencies) {
							dependentEngineIds.add((String) dep.get("engine_id"));
						}
						SecurityProjectUtils.updateProjectDependencies(user, projectId, dependentEngineIds);
					}
					// delete this file since values can update and file is dynamically generated on export
					dependenciesFile.delete();
				}
			}
			
			logger.info(step + ") Done");
		} catch (Exception e) {
			error = true;
			logger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(
					"Error occurred trying to synchronize the metadata and insights for the zip file", false);
		} finally {
			if (error) {
				// delete all the resources
				cleanUpFolders(randomTempUnzipF, finalProjectSmssF, finalProjectFolderF);
				// remove from DIHelper
				UploadUtilities.removeProjectFromDIHelper(projectId);
				// delete from security
				SecurityProjectUtils.deleteProject(projectId);
			} else {
				File[] assetsFilesToDelete = finalProjectAssetF.listFiles(
						(dir, name) -> name.endsWith(IEngine.METADATA_FILE_SUFFIX) 
							|| name.endsWith(IProject.DEPENDENCIES_FILE_SUFFIX) 
							|| name.endsWith(Constants.SEMOSS_EXTENSION));			
				cleanUpFolders(assetsFilesToDelete);
			}
		}

		// add user as engine owner
		if(!replace) {
			List<AuthProvider> logins = user.getLogins();
			for (AuthProvider ap : logins) {
				SecurityProjectUtils.addProjectOwner(user, projectId, user.getAccessToken(ap).getId());
			}
		}
		
		// push new project to cloud
		ClusterUtil.pushProject(projectId);
		
		String[] engineIds = getEngineIdsFromProject(finalProjectFolderF);
		Map<String, Object> engineInfo = processAndSetProjectDependencies(engineIds, projectId);
		
		Map<String, Object> retMap = UploadUtilities.getProjectReturnData(this.insight.getUser(), projectId);
		
		// sending the success and failed list of engineIds to FE
		retMap.put("engineIds", engineInfo);
		return new NounMetadata(retMap, PixelDataType.UPLOAD_RETURN_MAP, PixelOperationType.MARKET_PLACE_ADDITION);
	}
	
	/**
	 * This method is intended to be overriden by other reactors
	 * in case we want to attempt to delete and reupload
	 * @return
	 */
	protected boolean deleteIfExisting() {
		return false;
	}

	/**
	 * 
	 * @param fileToDelete
	 */
	private void cleanUpFolders(File... fileToDelete) {
		for(File f : fileToDelete) {
			if(f != null && f.exists()) {
				try {
					FileUtils.forceDelete(f);
				} catch (IOException e) {
					classLogger.warn("Error on clean up attempting to delete " + f.getAbsolutePath());
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}
	
	// extracts and returns engineIds from the files with the given extensions
	// in the project folder.
	public String[] getEngineIdsFromProject(File finalProjectFolderF) {
        Set<String> engineIds = new HashSet<>();
        String folderPath = finalProjectFolderF.getAbsolutePath();
 
        // extensions you want to look into
        String[] extensions = { ".js", ".jsx", ".java", ".env", ".py", ".ts", ".tsx" };
        
        try (java.util.stream.Stream<java.nio.file.Path> stream = Files.walk(Paths.get(folderPath))) {
        	 stream.filter(Files::isRegularFile)
        	       .filter(path -> {
        	          String fileName = path.getFileName().toString().toLowerCase();
        	          for (String ext : extensions) {
        	              if (fileName.endsWith(ext)) {
        	                  return true;
        	              }
        	          }
        	          return false;
        	    })
                .forEach(path -> {
                    try (java.util.stream.Stream<String> lines = Files.lines(path)) {
                        lines.forEach(line -> {
                            String[] tokens = line.split("[^a-zA-Z0-9-]+");
                            for (String token : tokens) {
                                Matcher matcher = UUID_PATTERN.matcher(token.trim());
                                if (matcher.matches()) {
                                    engineIds.add(token.trim());
                                }
                            }
                        });
                    } catch (IOException e) {
                        System.err.println("Error reading file: " + path);
                        e.printStackTrace();
                    }
                });
 
        } catch (IOException e) {
            classLogger.error("Error reading file: {}", folderPath, e);
        }
 
        return engineIds.toArray(new String[0]);
    }
	
	// check if the extracted engineIds are present in the engine table or not
	// adding the info of the existing engineIds into the projectdependencies table
	// returning a list of added (success) and not added (failed) engineIds
	private Map<String, Object> processAndSetProjectDependencies(String[] engineIds, String projectId) {
	 
	    User user = this.insight.getUser();
	    
	    Map<String, Object> result = new HashMap<>();
	    Map<String, String> success = new HashMap<>();
	    Set<String> failed = new HashSet<>();
	 
	    for (String engineId : engineIds) {
	        if (SecurityEngineUtils.containsEngineId(engineId)) {
	            IEngine.CATALOG_TYPE engineType = SecurityEngineUtils.getEngineType(engineId);
	            success.put(engineId, engineType.toString());
	        } else {
	            failed.add(engineId);
	        }
	    }
	 
	    // update the project dependencies table only with valid engineIds
	    List<String> validEngineIds = new ArrayList<>(success.keySet());
	    SecurityProjectUtils.updateProjectDependencies(user, projectId, validEngineIds);
	 
	    // build final result map to return
	    result.put("success", success);
	    result.put("failed", failed);
	    
	    return result;
	}
	
	@Override
	public String getReactorDescription() {
	    return "Import an app from an exported zip smss-app file";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
	    if(key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
	        return "This is a required value containing the relative file path of the single zip file to be imported";
	    } else if(key.equals(ReactorKeysEnum.SPACE.getKey())) {
	        return "This is an optional field to determine the space in which the relative file path exists (user project space, current insight space, project id space).";
	    } else if(key.equals(ReactorKeysEnum.GLOBAL.getKey())) {
	    	return "This is a required value to determine if the app is public or private";
	    }
	    return super.getDescriptionForKey(key);
	}

}
