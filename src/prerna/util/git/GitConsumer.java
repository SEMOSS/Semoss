package prerna.util.git;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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
		moveSMSSToDB(baseFolder, yourName4App );
	}

	private static void moveSMSSToDB(String baseFolder, String appName)
	{
		// need to account for version here

		String fileName = baseFolder + "/db/" + appName + "/version";

		String dbName = baseFolder + "/db/" + appName ;

		File dir = new File(fileName);
		String targetDir = baseFolder + "/db";

		// now move the dbs

		List <String> otherStuff = new Vector<String>();
		otherStuff.add("*.db");
		otherStuff.add("*.OWL");
		FileFilter fileFilter = new WildcardFileFilter(otherStuff);
		File [] files = dir.listFiles(fileFilter);
		File dbFile = new File(dbName);
		for (int i = 0; i < files.length; i++) {
			try {
				// need to make modification on the engine
				FileUtils.copyFileToDirectory(files[i], dbFile);
				files[i].delete();
				// in reality there may be other things we need to do
				//files[i].renameTo(new File(targetDir + "/" + appName + ".smss"));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}		
		// I need to change the file to the app name
		// first move the smss
		fileFilter = new WildcardFileFilter("*.smss");
		files = dir.listFiles(fileFilter);
		File targetFile = new File(targetDir);
		for (int i = 0; i < files.length; i++) {
			try {
				// need to make modification on the engine
				File file = changeEngine(files[i], appName);
				FileUtils.copyFileToDirectory(file, targetFile );

				// in reality there may be other things we need to do
				//files[i].renameTo(new File(targetDir + "/" + appName + ".smss"));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private static File changeEngine(File file, String appName)
	{
		String mainDirectory = file.getParent();
		String fileName = file.getName();
		File newFile = null;

		try {
			OutputStream fos = null;

			if((fileName).equalsIgnoreCase(appName + ".smss"))
			{
				newFile = file; // nothing to do here
			}
			else
			{
				String oldName = "db/" + fileName.replace(".smss", "");
				String newName = "db/" + appName;
				String newFileName = mainDirectory + "/" + appName + ".smss";
				newFile = new File(newFileName);
				fos = new FileOutputStream(newFile);

				Properties prop = new Properties();
				prop.load(new FileInputStream(file));

				prop.put("ENGINE", appName);

				// accomodate for old stuff
				Enumeration <Object> propKeys = prop.keys();

				while(propKeys.hasMoreElements())
				{
					String propKey = propKeys.nextElement() + "";
					String propValue = prop.getProperty(propKey);

					if(propValue.contains(oldName))
					{
						propValue = propValue.replaceAll(oldName, newName);
						prop.put(propKey, propValue);
					}
					else
					{
						prop.put(propKey, propValue);
					}
				}

				prop.store(fos, "Changing File Content for engine");
				fos.close();

			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return newFile;
	}
}
