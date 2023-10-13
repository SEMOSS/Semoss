package prerna.util.git.reactors;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.git.GitAssetUtils;

public class BrowseAssetReactor extends AbstractReactor {

	// pulls the latest for this project / asset
	// the asset is basically the folder where it sits
	// this can be used enroute in a pipeline

	private static Logger classLogger = LogManager.getLogger(BrowseAssetReactor.class);

	public BrowseAssetReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(),  ReactorKeysEnum.SPACE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String space = this.keyValue.get(this.keysToGet[1]);
		String assetFolder = AssetUtility.getAssetBasePath(this.insight, space, false);
		String replacer = "";
		

		// specific folder to browse
		String locFolder = assetFolder;
		if (keyValue.containsKey(keysToGet[0])) {
			locFolder = assetFolder + "/" + Utility.normalizePath( keyValue.get(keysToGet[0]));
			locFolder = locFolder.replaceAll("\\\\", "/");
		}
		
		 // set the context here
		if(!keyValue.containsKey(keysToGet[0]) && space != null) {
			try {
				this.insight.setContext(space);
				//if we have a chroot, mount the project for that user.
				if (Boolean.parseBoolean(DIHelper.getInstance().getProperty(Constants.CHROOT_ENABLE))) {
					//get the app_root folder for the project
					this.insight.getUser().getUserMountHelper().mountFolder(assetFolder,assetFolder, false);
				}
			} catch(IllegalArgumentException e) {
				// ignore
			}
		}
		
		File dirFile = new File(assetFolder + "/" + locFolder);
		if(!dirFile.exists()) {
			// if this file doesn't exist.. it has not been cloned yet. so clone and then go into it
			cloneRepo(locFolder, assetFolder);
		}

		List <Map<String, Object>> output = GitAssetUtils.getAssetMetadata(locFolder, assetFolder, replacer, false);
		
		
		// add the files from repository and show it as if those files are there
		if(locFolder.length() == 0) {
			FileInputStream fis = null;
			// try to add all the repository
			try {
				File repoFile = new File(assetFolder + "/version/repoList.txt");
				Properties prop = new Properties();
				fis = new FileInputStream(repoFile);
				prop.load(fis);
				Enumeration <Object> dirs = prop.keys();
				
				while(dirs.hasMoreElements())
				{
					String item = dirs.nextElement() + "";
					// path, name, last modified, type
					Map<String, Object> meta = new HashMap<String, Object>();
					
					meta.put("path", item + "/");
					meta.put("name", item);
					meta.put("type", "directory");
					meta.put("lastModified", GitAssetUtils.getDate(System.currentTimeMillis()));
					
					output.add(meta);
				}
			} catch (FileNotFoundException e) {
				classLogger.error(Constants.STACKTRACE, e);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			} finally {
				if(fis != null) {
					try {
						fis.close();
					} catch (IOException e) {
						classLogger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
		
		// let us not show any hidden folders that start with a "."
		Iterator<Map<String, Object>> dirIterator = output.iterator();
		while(dirIterator.hasNext()) {
			Map<String, Object> fileObj = dirIterator.next();
			String name = fileObj.get("name") + "";
			String type = fileObj.get("type") + "";
			if(name.startsWith(".") && type.equalsIgnoreCase("directory")) {
				// we want to remove this
				dirIterator.remove();
			}
		}

		return new NounMetadata(output, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	}
	
	private void cloneRepo(String repoName, String workingDir) {
		String repo = workingDir + "/version/repoList.txt";
		File repoFile = new File(repo);
		if(repoFile.exists() && repoFile.isFile()) {
			Properties prop = Utility.loadProperties(repo);
			String url = prop.getProperty(repoName);
			insight.getCmdUtil().executeCommand("git clone " + url);
		}
	}

}
