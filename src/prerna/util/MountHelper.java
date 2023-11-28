package prerna.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLDecoder;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MountHelper {

	private static final Logger classLogger = LogManager.getLogger(MountHelper.class);

	protected static final String FILE_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	// store a mount helper in the user object
	// upon creation of a new mount helper it should
	// 1. set up a folder at <mountLocation>/user-session - mount locatin from DI
	// helper
	// 2. mount from the source - source location from DI helper
	// 3. Create the RDF and mount the R and Py folders
	// 4. save the mount location to unmount in the future

	// targetDirName = user chroot
	private String targetDirName = null;
	private Boolean enableSudo = false;

	//	public static void main(String[] args) {
	//		
	//		//String sourceDirName = "c:/users/pkapaleeswaran/workspacej3";
	//		//String targetDirName = "c:/users/pkapaleeswaran/workspacej3/mysession";
	//		String sourceDirName = "/stable-chroot"; // the mainbootstrap directory
	//		String targetDirName = "/home/";
	//
	//		// we can instrument the maindebootstrap folder through RDF Map here
	//		DIHelper.getInstance().loadCoreProp("/mnt/c/users/pkapaleeswaran/workspacej3/SemossDev/RDF_Map.prop");
	//		
	//		
	//		MountHelper maker = new MountHelper(targetDirName);
	//
	//		//make the target
	//		maker.mountTarget(sourceDirName, "", false, false);
	//		System.out.println("Source Dir: " + sourceDirName);
	//		System.out.println("Target Dir: " + targetDirName);
	//
	////		maker.mountDir("/proc", targetDirName + "/proc", true);
	////		maker.mountDir("/dev", targetDirName + "/dev", true);
	//		
	//		
	//		
	//		// remove the target
	//		maker.unmountTarget();
	//		maker.unmountDir(targetDirName + "/proc", true);
	//		maker.unmountDir(targetDirName + "/dev", true);
	////		// delete the target
	////		maker.deleteTarget(true);
	//		maker.createCustomRDFMap();
	//		
	//	}

	//	public MountHelper(String targetDirName, String user)
	//	{
	//		this.targetDirName = targetDirName;
	//		File targetDir = new File(targetDirName);
	//		if(!targetDir.exists()) {
	//			logger.info("Target folder doesn't exist. Making folder now at: " + targetDirName);
	//			boolean success = targetDir.mkdir(); // make directory
	//			logger.info("Target folder creation " + success);
	//
	//		}
	//
	//		// also create the semoss home folder
	//		String appHome = this.targetDirName + "/" + "semoss";
	//		targetDir = new File(appHome);
	//		if(!targetDir.exists())
	//			targetDir.mkdir(); // make app home directory 
	//		
	//		
	//		// also create the semoss home folder
	//		String appHome2 = this.targetDirName + "/" + user;
	//		targetDir = new File(appHome2);
	//		if(!targetDir.exists())
	//			targetDir.mkdir(); // make app home directory 
	//		
	//	}

	public MountHelper(String targetDirName) {
		// String baseMountPath = DIHelper.getInstance().getProperty("CHROOT_DIR");

		// this.targetDirName = baseMountPath + FILE_SEPARATOR + targetDirName;
		this.targetDirName = targetDirName;
		File targetDir = new File(Utility.normalizePath(targetDirName));
		if (!targetDir.exists()) {
			classLogger.info("Target folder doesn't exist. Making folder now at: " + targetDirName);
			boolean success = targetDir.mkdir(); // make directory
			classLogger.info("Target folder creation at " + targetDirName + " " + success);
		}

		// also create the semoss home folder
		String newSemossHomeFolderPath = this.targetDirName + FILE_SEPARATOR + DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		targetDir = new File(Utility.normalizePath(newSemossHomeFolderPath));
		if (!targetDir.exists()) {
			targetDir.mkdirs(); // make app home directory
		}

		//TODO make a unique mount for the debootstrap - not mount target. change mount target to ignore the second param
		// mount the debootstrap
		String debootstrapLoc = DIHelper.getInstance().getProperty("CHROOT_DEBOOTSTRAP_DIR");
		mountTarget(debootstrapLoc, "", true);

		// create the RDF and py/R folders
		createCustomRDFMap();
	}
	
	public String getTargetDirName() {
		return this.targetDirName;
	}

	// TODO include the files at the top directory level if possible here

	//	make the directories for mount
	//	mountTarget is for recursive mounting of multiple folders but ignoring files in the top level (ex. M
	//	example app_root folder is a target we want to mount because that one folder has all the subfiles/folders we need
	//	for Insight Cache, we want to do that as mount folder because we just want that one folder + the files at that level
	public void mountTarget(String sourceDirName, String subPath, boolean readOnly) {
		if (subPath == null) {
			subPath = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		}
		
		File sourceDir = new File(sourceDirName);
		if (!sourceDir.exists()) {
			classLogger.info("Source directory not available" + sourceDirName);
		}
		
		// list files from source file and make a copy
		String[] allSourceFiles = sourceDir.list();
		for (int sourceFileIndex = 0; sourceFileIndex < allSourceFiles.length; sourceFileIndex++) {
			String srcPath = sourceDirName + FILE_SEPARATOR + allSourceFiles[sourceFileIndex];
			File thisFile = new File(srcPath);
			//Thos is the check where it needs to be a subfolder to get mounted. files at the top directory dont get mounted
			if (thisFile.isDirectory()) {
				String targetPath = targetDirName + FILE_SEPARATOR + subPath + FILE_SEPARATOR
						+ allSourceFiles[sourceFileIndex];
				File thisTargetFile = new File(targetPath);
				boolean success = thisTargetFile.mkdirs();
				classLogger.debug("Making dir at " + targetPath + " was a " + success);
				classLogger.debug("Mounting dir at srcPat:  " + srcPath + " and targetPath: " + targetPath);

				mountDir(srcPath, targetPath, enableSudo, readOnly);
			}
		}

		// completed
	}

	// mount a folder including all files in that folder. 
	public void mountFolder(String sourceDirName, String subPath, boolean readOnly) {
		if (subPath == null) {
			subPath = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		}
		
		File sourceDir = new File(Utility.normalizePath(sourceDirName));
		if (!sourceDir.exists() || !sourceDir.isDirectory()) {
			classLogger.info("Source directory not available" + Utility.cleanLogString(sourceDirName));
		}
		
		String targetPath = targetDirName + FILE_SEPARATOR + subPath;

		File thisTargetFile = new File(Utility.normalizePath(targetPath));
		boolean success = thisTargetFile.mkdirs();
		classLogger.info("Making folder at " + Utility.cleanLogString(targetPath) + " was a " + success);
		classLogger.info("Mounting folder at srcPat:  " + Utility.cleanLogString(sourceDirName) + " and targetPath: " + Utility.cleanLogString(targetPath));

		mountDir(sourceDirName, targetPath, enableSudo, readOnly);
	}

	// mount directory
	private void mountDir(String srcDir, String tgtDir, boolean sudo, boolean readOnly) {
		try {
			ProcessBuilder pb = null;
			if (readOnly) {
				if (sudo) {
					pb = new ProcessBuilder(new String[] { "sudo", "bindfs", "-r", srcDir, tgtDir });
				} else {
					pb = new ProcessBuilder(new String[] { "bindfs", "-r", srcDir, tgtDir });
				}
			} else {
				if (sudo) {
					pb = new ProcessBuilder(new String[] { "sudo", "bindfs", srcDir, tgtDir });
				} else {
					pb = new ProcessBuilder(new String[] { "bindfs", srcDir, tgtDir });
				}
			}

			// Process pb = Runtime.getRuntime().exec(new String[] {"mkdir",
			// "/home/prabhuk/mn1/bin2"});
			classLogger.debug(pb.command().toString());
			Process p = pb.start();
			// Thread.sleep(5000);
			 p.waitFor(10, TimeUnit.SECONDS);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}

	// unmount
	public void unmountDir(String srcDir, boolean sudo) {
		classLogger.info("Unmounting.. " + srcDir);
		try {
			ProcessBuilder pb = null;
			if (sudo) {
				pb = new ProcessBuilder(new String[] { "sudo", "umount", srcDir });
			} else {
				pb = new ProcessBuilder(new String[] { "umount", srcDir });
			}
			// Process pb = Runtime.getRuntime().exec(new String[] {"mkdir",
			// "/home/prabhuk/mn1/bin2"});
			Process p = pb.start();
			// Thread.sleep(5000);
			// p.waitFor(5, TimeUnit.SECONDS);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}

	public void deleteTarget(boolean sudo) {
		File tgtDir = new File(targetDirName);
		try {
			FileUtils.deleteDirectory(tgtDir);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}


	// removes all the mount session folder via grep of /proc/mounts
	public void unmountTargetProc() {
		//try to any extra processes in the mount point
		try {
			ProcessBuilder pb = null;
			String command = "fuser -k " + targetDirName;
			classLogger.debug("Running fuser command: " + command);
			pb = new ProcessBuilder(new String[] { "/bin/sh", "-c", command });
			Process p = pb.start();
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		// try to umount all mounts under the mount point
		try {
			ProcessBuilder pb = null;
			String command = "grep " + targetDirName + " /proc/mounts | cut -f2 -d' ' | xargs -r -n 1 umount";
			classLogger.debug("Running umount command " + command);
			pb = new ProcessBuilder(new String[] { "/bin/sh", "-c", command });
			//pb.redirectOutput(tempFile);
			Process p = pb.start();
			p.waitFor(5L, TimeUnit.SECONDS);
			// Thread.sleep(5000);
			// p.waitFor(5, TimeUnit.SECONDS);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		deleteTarget(enableSudo);
	}


	// removes all the mount session folder recursive
	public void unmountTarget() {
		File tgtDir = new File(targetDirName);
		String[] allSourceFiles = tgtDir.list();
		for (int tgtFileIndex = 0; tgtFileIndex < allSourceFiles.length; tgtFileIndex++) {
			String tgtPath = targetDirName + FILE_SEPARATOR + allSourceFiles[tgtFileIndex];

			File thisFile = new File(tgtPath);
			if (thisFile.isDirectory()) {
				unmountDir(tgtPath, enableSudo);
			}
		}
	}

	// move the R and Py folders
	// create a custom RDF Map
	public void createCustomRDFMap() {
		// properties I need in a string array
		String[] propsNeeded = new String[] { Constants.BASE_FOLDER, Constants.NETTY_R, Constants.NETTY_PYTHON,
				Constants.USE_R, Constants.USE_PYTHON, Constants.R_MEM_LIMIT, Constants.INSIGHT_CACHE_DIR,
				Settings.MVN_HOME, Settings.REPO_HOME, Constants.PY_BASE_FOLDER, Constants.R_CONNECTION_JRI};

		Properties prop = new Properties();
		for (int propIndex = 0; propIndex < propsNeeded.length; propIndex++) {
			String key = propsNeeded[propIndex];
			String value = DIHelper.getInstance().getProperty(key);

			if (key != null && value != null) {
				prop.put(key, value);
			}
			classLogger.debug("Writing Value " + key + " <> " + value);
		}

		// set the base folder
		// write this out as RDF_MAP
		File file = new File(targetDirName + FILE_SEPARATOR + DIHelper.getInstance().getProperty(Constants.BASE_FOLDER)
				+ FILE_SEPARATOR + "RDF_Map.prop");
		try(FileOutputStream fos = new FileOutputStream(file);) {
			prop.store(fos, "Chrooted Output");
			fos.flush();
			fos.close();
		} catch (FileNotFoundException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);

		mountFolder(baseFolder + FILE_SEPARATOR + Constants.R_BASE_FOLDER,
				baseFolder + FILE_SEPARATOR + Constants.R_BASE_FOLDER, true);
		mountFolder(baseFolder + FILE_SEPARATOR + Constants.PY_BASE_FOLDER,
				baseFolder + FILE_SEPARATOR + Constants.PY_BASE_FOLDER, true);
		
		boolean nativePyServer = DIHelper.getInstance().getProperty(Settings.NATIVE_PY_SERVER) != null
				&& DIHelper.getInstance().getProperty(Settings.NATIVE_PY_SERVER).equalsIgnoreCase("true");
		if(!nativePyServer) {
			// MOUNTING CP IS NEEDED FOR TCP with java/jvm
			mountFolder(getCP(), getCP(), true);
		}
		
		String m2Location = DIHelper.getInstance().getProperty(Settings.REPO_HOME);
		File m2LocationF = new File(m2Location);
		if(m2LocationF.exists() && m2LocationF.isDirectory()) {
			mountFolder(m2Location, m2Location, false);
		}

		String mvnLocation = DIHelper.getInstance().getProperty(Settings.MVN_HOME);
		File mvnLocationF = new File(mvnLocation);
		if(mvnLocationF.exists() && mvnLocationF.isDirectory()) {
			mountFolder(mvnLocation, mvnLocation, false);
		}
		
		// TODO add insight cache here too - get users insight cache
		mountTarget(DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR),
				DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR), false);

		//		File thisTargetFile = new File(targetDirName);
		//		boolean success = thisTargetFile.mkdirs();
		//		System.out.println("KUNAL ::::: making dir at " + targetPath + " was a " + success);
		//		System.out.println("KUNAL ::::: mounting dir at srcPat:  " + srcPath + " and targetPath: " + targetPath);

		//		String targetPath = targetDirName + FILE_SEPARATOR + DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR);
		//		File thisTargetFile = new File(targetPath);
		//		boolean success = thisTargetFile.mkdirs();
		//		System.out.println("KUNAL ::::: making dir at " + targetPath + " was a " + success);
		//		System.out.println("KUNAL ::::: mounting dir at srcPat:  " + DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + " and targetPath: " + targetPath);
		//
		//		
		//		mountDir(DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) , targetDirName + DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR), enableSudo, true);
		//		
	}

	// TODO project mounting - create the folder for the project name and then mount
	// r/w the app_root

	public static String getCP() {
		// StringBuffer envClassPath = new StringBuffer();
		// String osName = System.getProperty("os.name").toLowerCase();
		// boolean win = osName.indexOf("win") >= 0;

		// String cp = "jep-3.9.0.jar;classes";
		String webInfPath = null;

		try {
			// StringBuffer retClassPath = new StringBuffer("");
			Class utilClass = Class.forName("prerna.util.Utility");
			ClassLoader cl = utilClass.getClassLoader();

			URL[] urls = ((URLClassLoader) cl).getURLs();

			// boolean webinfTagged = false;

			for (URL url : urls) {
				String jarName = Utility.getInstanceName(url + "");
				String thisURL = URLDecoder.decode((url.getFile()));
				if (thisURL.contains("WEB-INF/lib")) {
					webInfPath = thisURL.split("/lib")[0];
					return webInfPath;
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		return webInfPath;
	}

}
