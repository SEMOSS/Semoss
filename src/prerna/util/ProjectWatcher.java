package prerna.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;

public class ProjectWatcher extends AbstractFileWatcher {
	
	private static final Logger classLogger = LogManager.getLogger(ProjectWatcher.class);
	
	/**
	 * Used in the starter class for processing SMSS files.
	 */
	@Override
	public void loadFirst() {
		File dir = new File(folderToWatch);
		String[] fileNames = dir.list(this);
		String[] projectIds = new String[fileNames.length];
		
		// loop through and load all the projects
		for (int fileIdx = 0; fileIdx < fileNames.length; fileIdx++) {
			try {
				String fileName = fileNames[fileIdx];
//				//we need to add projects to security db
				String loadedProject = catalogProject(fileName, folderToWatch);
				projectIds[fileIdx] = loadedProject;
			} catch (RuntimeException ex) {
				classLogger.error(Constants.STACKTRACE, ex);
				classLogger.fatal("Project Failed " + folderToWatch + "/" + fileNames[fileIdx]);
			}
		}
		
		if (!ClusterUtil.IS_CLUSTER) {
			// if projects are removed from the file system
			// remove them
			List<String> projects = SecurityProjectUtils.getAllProjectIds();
			for(String project : projects) {
				if(!ArrayUtilityMethods.arrayContainsValue(projectIds, project)) {
					SecurityProjectUtils.deleteProject(project);
				}
			}
		}
	}
	
	// this is an alternate method.. which will not load the database but would merely keep the name of the engine
	// and the SMSS file
	/**
	 * Loads a new database by setting a specific engine with associated properties.
	 * @param 	Specifies properties to load 
	 */	
	public static String catalogProject(String newFile, String folderToWatch) {
		String projects = DIHelper.getInstance().getProjectProperty(Constants.PROJECTS) + "";
		FileInputStream fileIn = null;
		String projectId = null;
		try{
			Properties prop = new Properties();
			fileIn = new FileInputStream(Utility.normalizePath(folderToWatch) + "/"  +  Utility.normalizePath(newFile));
			prop.load(fileIn);
			
			projectId = prop.getProperty(Constants.PROJECT);
			
			if(projects.startsWith(projectId) || projects.contains(";"+projectId+";") || projects.endsWith(";"+projectId)) {
				classLogger.debug("Project " + folderToWatch + "<>" + newFile + " is already loaded...");
			} else {
				String fileName = folderToWatch + "/" + newFile;
				DIHelper.getInstance().setProjectProperty(projectId + "_" + Constants.STORE, fileName);
				
				String projectNames = (String)DIHelper.getInstance().getProjectProperty(Constants.PROJECTS);
				if(!(projects.startsWith(projectId) || projects.contains(";"+projectId+";") || projects.endsWith(";"+projectId))) {
					projectNames = projectNames + ";" + projectId;
					DIHelper.getInstance().setProjectProperty(Constants.PROJECTS, projectNames);
				}
				
				SecurityProjectUtils.addProject(projectId, null);
			}
		} catch(Exception e){
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			try{
				if(fileIn != null) {
					fileIn.close();
				}
			} catch(IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
		
		return projectId;
	}

	@Override
	public void process(String fileName) {
		catalogProject(fileName, folderToWatch);
	}
	
}
