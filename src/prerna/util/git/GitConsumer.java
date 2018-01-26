package prerna.util.git;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.codehaus.plexus.util.FileUtils;

import prerna.util.DIHelper;

public class GitConsumer {

	/**
	 * This class is not intended to be extended or used outside of its static method
	 */
	private GitConsumer() {

	}

	public static void makeAppFromRemote(String yourName4App, String fullRemoteAppName) {
		// need to get the database folder
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String dbFolder = baseFolder + "/db/" + yourName4App;
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
		GitRepoUtils.makeLocalAppGitVersionFolder(dbFolder);

		String versionFolder = dbFolder + "/version";
		// write a random file so we can add/commit
		GitUtils.semossInit(versionFolder);
		// add/commit all the files
		GitPushUtils.addAllFiles(versionFolder, false);
		GitPushUtils.commitAddedFiles(versionFolder);
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
		GitRepoUtils.fetchRemote(versionFolder, repoName, "", "");
		// merge
		GitMergeHelper.merge(versionFolder, "master", repoName + "/master", 0, 2, true);
		// commit
		GitPushUtils.commitAddedFiles(versionFolder);

		// move the smss to the db folder
		moveDataFilesToApp(baseFolder, yourName4App);
	}

	public static void moveDataFilesToApp(String baseFolder, String yourName4App) {
		// need to account for version here
		String appFolder = baseFolder + "/db/" + yourName4App ;
		String versionFolder = appFolder + "/version";
		File dir = new File(versionFolder);

		// now move the dbs
		List <String> otherStuff = new Vector<String>();
		otherStuff.add("*.db");
		otherStuff.add("*.OWL");
		FileFilter fileFilter = new WildcardFileFilter(otherStuff);
		File [] files = dir.listFiles(fileFilter);
		File dbFile = new File(appFolder);
		for (int i = 0; i < files.length; i++) {
			try {
				// need to make modification on the engine
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
				// need to make modification on the engine
				File fileToMove = null;
				if(files[i].getName().equalsIgnoreCase(yourName4App)) {
					fileToMove = files[i];
				} else {
					// we have to change the smss file
					File origFile = files[i];
					fileToMove = changeSmssEngineName(origFile, yourName4App);
					// we have to modify the recipe steps
//					updateInsightRdbms(origFile, appFolder, yourName4App);
				}
				FileUtils.copyFileToDirectory(fileToMove, targetFile);
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
		String mainDirectory = file.getParent();
		String fileName = file.getName();
		
		String oldName = "db/" + fileName.replace(".smss", "");
		String newName = "db/" + yourName4App;
		String newFileName = mainDirectory + "/" + yourName4App + ".smss";
		File newFile = new File(newFileName);

		FileInputStream fis = null;
		OutputStream fos = null;
		try {
			fos = new FileOutputStream(newFile);
			fis = new FileInputStream(file);
			Properties prop = new Properties();
			prop.load(fis);
			prop.put("ENGINE", yourName4App);

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
}
