package prerna.util.git;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.Vector;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.log4j.Logger;
import org.codehaus.plexus.util.FileUtils;
import org.eclipse.jgit.lib.ProgressMonitor;

import prerna.auth.utils.SecurityQueryUtils;
import prerna.auth.utils.SecurityUpdateUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.SmssUtilities;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.MosfetSyncHelper;
import prerna.util.Utility;

public class GitConsumer {

	/**
	 * This class is not intended to be extended or used outside of its static method
	 */
	private GitConsumer() {

	}

	public static String makeAppFromRemote(String yourName4App, String fullRemoteAppName, Logger logger) {
		String temporaryAppId = UUID.randomUUID().toString();
		// need to get the database folder
		try {
			prerna.security.InstallCertNow.please("github.com", null, null);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String dbFolder = baseFolder + "/db/" + SmssUtilities.getUniqueName(yourName4App, temporaryAppId);
		File db = new File(dbFolder);
		if(!db.exists()) {
			// make the folder
			db.mkdirs();
			//			throw new IllegalArgumentException("You already have a folder named " + yourName4App + " in your directory."
			//					+ " Please delete it or choose a different name for the app");
		}

		String[] appNameSplit = fullRemoteAppName.split("/");
		String appUserName = appNameSplit[0];
		String repoName = appNameSplit[1];

		// initialize the version folder
		logger.info("Start creating local git folder");
		GitRepoUtils.makeLocalAppGitVersionFolder(dbFolder);
		logger.info("Done creating local git folder");

		String versionFolder = dbFolder + "/version";
		// write a random file so we can add/commit
		logger.info("Init local git...");
		GitUtils.semossInit(versionFolder);
		// add/commit all the files
		logger.info("Add local git...");
		GitPushUtils.addAllFiles(versionFolder, false);
		logger.info("Commit local git...");
		GitPushUtils.commitAddedFiles(versionFolder);
		logger.info("Done...");
		// push the ignore file
		String[] filesToIgnore = new String[]{".mv.db", "*.db", "*.jnl"};
		GitUtils.writeIgnoreFile(versionFolder, filesToIgnore);

		List<Map<String, String>> remotes = GitRepoUtils.listConfigRemotes(versionFolder);
		// need to loop through and see if remote exists
		boolean existing = false;
		REMOTE_LOOP : for(Map<String, String> remoteMap : remotes) {
			String existingRemoteAppName = remoteMap.get("name");
			// need to compare combination of name space + app name
			if(existingRemoteAppName.equals(repoName)) {
				existing = true;
				break REMOTE_LOOP ;
			}
		}
		if(!existing) {
			GitUtils.removeAllIgnore(versionFolder);
			GitRepoUtils.addRemote(versionFolder, appUserName, repoName);
		}
		// else leave the ignore
		else {
			// add it
			GitUtils.checkoutIgnore(versionFolder, filesToIgnore);
		}

		// switch to correct remote
		logger.info("Fetch remote git directory");
		ProgressMonitor mon = GitRepoUtils.fetchRemote(versionFolder, repoName, "", "");
		// merge the remote - this will bring in the files from the remote repo
		logger.info("Merge remote git directory");
		GitMergeHelper.merge(versionFolder, "master", repoName + "/master", 0, 2, true);
		// commit
		GitPushUtils.commitAddedFiles(versionFolder);

		// move the smss to the db folder
		logger.info("Initialize new app...");
		
		// so, we used a random id to create the folder
		// but we need to rename to use the correct app id for the engine
		String actualAppId = null;
		
		FileFilter fileFilter = new WildcardFileFilter("*.smss");
		File versionDir = new File(versionFolder);
		File[] files = versionDir.listFiles(fileFilter);
		// this should be size 1...
		for(int i = 0; i < files.length; i++) {
			File smssFile = files[i];
			Properties prop = Utility.loadProperties(smssFile.getAbsolutePath());
			actualAppId = prop.getProperty(Constants.ENGINE);
			
			// need to rename the folder
			File currAppFolder = versionDir.getParentFile();
			currAppFolder.renameTo(new File(currAppFolder.getParent() + "/" + SmssUtilities.getUniqueName(yourName4App, actualAppId)));
		}
		
		if(actualAppId == null) {
			throw new IllegalArgumentException("There is no app id defined within the smss of the new app you are downloading");
		}
		
		if (SecurityQueryUtils.getEngineIds().contains(actualAppId)) {
			throw new IllegalArgumentException("The app you are attempting to copy already exists as " + SecurityQueryUtils.getEngineAliasForId(actualAppId));
		}
		
		// before you do this.. wait for the monitor to finish
		// so this started succeeding even without so leaving it for now
		/*while(!((GitProgressMonitor)mon).complete)
		{
			try{
				Thread.sleep(200);
			}catch(Exception ignored)
			{
				
			}
		}*/
		moveDataFilesToApp(baseFolder, actualAppId, yourName4App, logger);
		return actualAppId;
	}

	public static void moveDataFilesToApp(String baseFolder, String appId, String yourName4App, Logger logger) {
		// need to account for version here
		String appFolder = baseFolder + "/db/" + SmssUtilities.getUniqueName(yourName4App, appId);
		String versionFolder = appFolder + "/version";
		File dir = new File(versionFolder);

		// seems like git pull doesn't complete until this point
		// so it is screwing up
		
		
		// now move the dbs
		List <String> otherStuff = new Vector<String>();
		otherStuff.add("*.db");
		otherStuff.add("*.OWL");
		FileFilter fileFilter = new WildcardFileFilter(otherStuff);
		
		
		File[] files = dir.listFiles(fileFilter);
		File dbFile = new File(appFolder);
		for (int i = 0; i < files.length; i++) {
			try {
				// need to make modification on the engine
				logger.info("Moving database file : " + FilenameUtils.getName(files[i].getAbsolutePath()));
				FileUtils.copyFileToDirectory(files[i], dbFile);
				files[i].delete();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		File gitDataDir = new File(versionFolder + "/data");
		if(gitDataDir.exists()) {
			File appDataDir = new File(appFolder + "/data");
			if(!appDataDir.exists()) {
				appDataDir.mkdir();
			}
			// move everything over
			File [] dataFiles = gitDataDir.listFiles();
			for (int i = 0; i < dataFiles.length; i++) {
				try {
					// move the data files into the app data folder
					logger.info("Moving database file : " + FilenameUtils.getName(files[i].getAbsolutePath()));
					FileUtils.copyFileToDirectory(dataFiles[i], appDataDir);
					files[i].delete();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		// I need to change the file to the app name
		// first move the smss
		fileFilter = new WildcardFileFilter("*.smss");
		files = dir.listFiles(fileFilter);

		String targetDir = baseFolder + "/db";
		File targetFile = new File(targetDir);
		for (int i = 0; i < files.length; i++) {
			try {
				File fileToMove = files[i];
				Properties prop = Utility.loadProperties(fileToMove.getAbsolutePath());
				
				if(!yourName4App.equals(prop.get(Constants.ENGINE_ALIAS))) {
					File origFile = fileToMove;
					fileToMove = changeSmssEngineName(fileToMove, yourName4App);
					origFile.delete();
				}
				
				// load the app
				IEngine app = loadApp(fileToMove.getAbsolutePath(), logger);
				// load the mosfet
				loadMosfetFiles(app, versionFolder, logger);
				// move the smss
				FileUtils.copyFileToDirectory(fileToMove, targetFile);
				// set the engine to the new smss
				app.setPropFile(targetDir + "/" + SmssUtilities.getUniqueName(yourName4App, appId) + ".smss");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
//	private static void updateInsightRdbms(File oldSmssFile, String appFolder, String yourName4App) {
//		String mainDirectory = oldSmssFile.getParentFile().getParent();
//		
//		FileInputStream fis = null;
//		try {
//			fis = new FileInputStream(oldSmssFile);
//			Properties prop = new Properties();
//			prop.load(fis);
//			
//			String origEngineName = prop.getProperty("ENGINE");
//			// this will contian something like db/engine_name/insights_database
//			String extension = prop.getProperty("RDBMS_INSIGHTS");
//			extension = extension.substring(extension.lastIndexOf("/"), extension.length());
//			String location = mainDirectory + extension;
//			
//			AppNameRecipeModifier.renameDatabaseForInsights(location, yourName4App, origEngineName);
//		} catch (IOException e) {
//			e.printStackTrace();
//		} finally {
//			if(fis != null) {
//				try {
//					fis.close();
//				} catch (IOException e) {
//					e.printStackTrace();
//				}
//			}
//		}
//	}

	private static File changeSmssEngineName(File file, String yourName4App) {
		Properties prop = Utility.loadProperties(file.getAbsolutePath());
		String appId = prop.getProperty(Constants.ENGINE);
		
		String mainDirectory = file.getParent();
		String fileName = file.getName();
		
		String oldName = "db/" + fileName.replace(".smss", "");
		String newName = "db/" + SmssUtilities.getUniqueName(yourName4App, appId);
		String newFileName = mainDirectory + "/" + SmssUtilities.getUniqueName(yourName4App, appId) + ".smss";
		File newFile = new File(newFileName);
		if(!newFile.exists()) {
			try {
				newFile.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		FileInputStream fis = null;
		OutputStream fos = null;
		try {
			fos = new FileOutputStream(newFile);
			fis = new FileInputStream(file);
			prop.load(fis);
			prop.put("ENGINE_ALIAS", yourName4App);

			Enumeration <Object> propKeys = prop.keys();
			while(propKeys.hasMoreElements()) {
				String propKey = propKeys.nextElement() + "";
				String propValue = prop.getProperty(propKey);

				if(propValue.contains(oldName)) {
					propValue = propValue.replaceAll(oldName, newName);
					prop.put(propKey, propValue);
				} else {
					prop.put(propKey, propValue);
				}
			}
			prop.store(fos, "Changing File Content for engine");
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if(fos != null) {
				try {
					fos.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if(fis != null) {
				try {
					fis.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		return newFile;
	}

	/**
	 * Load the app
	 * @param smssLocation
	 */
	private static IEngine loadApp(String smssLocation, Logger logger) {
		FileInputStream fileIn = null;
		try{
			Properties prop = new Properties();
			fileIn = new FileInputStream(smssLocation);
			prop.load(fileIn);
			logger.info("Start synchronizing app metadata...");
			IEngine engine = Utility.loadEngine(smssLocation, prop);
			logger.info("Done synchronizing app metadata");
			SecurityUpdateUtils.setEngineCompletelyGlobal(prop.getProperty(Constants.ENGINE));
			return engine;
		} catch(IOException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Error with loading app metadata");
		} finally {
			if(fileIn!=null) {
				try{
					fileIn.close();
				} catch(IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * Load any mosfet files since they might not have been added to the insights rdbms that was pushed
	 * @param versionFolder
	 */
	private static void loadMosfetFiles(IEngine app, String versionFolder, Logger logger) {
		IEngine insightsDb = app.getInsightDatabase();

		List<String> addFilesPath = new Vector<String>();
		List<String> modFilesPath = new Vector<String>();

		// mosfet filter
		List<String> mosfet = new Vector<String>();
		mosfet.add("*.mosfet");
		FileFilter mosfetFilter = new WildcardFileFilter(mosfet);
		
		// grab all the directories
		File vFolder = new File(versionFolder);
		File[] inDir = vFolder.listFiles();
		for(File in : inDir) {
			if(in.isDirectory()) {
				File[] mosfetFiles = in.listFiles(mosfetFilter);
				for(File mosfetF : mosfetFiles) {
					try {
						Map<String, Object> dataMap = MosfetSyncHelper.getMosfitMap(mosfetF);
						String id = dataMap.get(MosfetSyncHelper.RDBMS_ID_KEY) + "";
						// see if it exists or not
						IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(insightsDb, "select id from question_id where id='" + id + "'");
						if(wrapper.hasNext()) {
							modFilesPath.add(mosfetF.getAbsolutePath());
						} else {
							addFilesPath.add(mosfetF.getAbsolutePath());
						}
					} catch(Exception e) {
						logger.info("ERROR!!! " + e.getMessage());
					}
				}
			}
		}
		
		Map<String, List<String>> changedFiles = new HashMap<String, List<String>>();
		if(!addFilesPath.isEmpty()) {
			changedFiles.put(MosfetSyncHelper.ADD, addFilesPath);
		}
		if(!modFilesPath.isEmpty()) {
			changedFiles.put(MosfetSyncHelper.MOD, modFilesPath);
		}
		if(!changedFiles.isEmpty()) {
			logger.info("Start synchronizing mosfet files");
			MosfetSyncHelper.synchronizeInsightChanges(changedFiles, logger);
			logger.info("End synchronizing mosfet files");
		}
	}

}
