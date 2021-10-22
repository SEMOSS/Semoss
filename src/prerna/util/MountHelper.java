package prerna.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

public class MountHelper {

	String targetDirName = null;
	
	public static void main(String[] args) {
		
		// TODO Auto-generated method stub
		//String sourceDirName = "c:/users/pkapaleeswaran/workspacej3";
		//String targetDirName = "c:/users/pkapaleeswaran/workspacej3/mysession";
		String sourceDirName = "/stable-chroot"; // the mainbootstrap directory
		String targetDirName = "/home/";

		// we can instrument the maindebootstrap folder through RDF Map here
		DIHelper.getInstance().loadCoreProp("/mnt/c/users/pkapaleeswaran/workspacej3/SemossDev/RDF_Map.prop");
		
		
		MountHelper maker = new MountHelper(targetDirName);

		//make the target
		maker.mountTarget(sourceDirName);
		System.out.println("Source Dir: " + sourceDirName);
		System.out.println("Target Dir: " + targetDirName);

//		maker.mountDir("/proc", targetDirName + "/proc", true);
//		maker.mountDir("/dev", targetDirName + "/dev", true);
		
		
		
		// remove the target
		maker.unmountTarget();
		maker.unmountDir(targetDirName + "/proc", true);
		maker.unmountDir(targetDirName + "/dev", true);
//		// delete the target
//		maker.deleteTarget(true);
		maker.createCustomRDFMap();
		
	}
	
	public MountHelper(String targetDirName, String user)
	{
		this.targetDirName = targetDirName;
		File targetDir = new File(targetDirName);
		if(!targetDir.exists()) {
			System.out.println("Target folder doesn't exist. Making folder now at: " + targetDirName);

			boolean success = targetDir.mkdir(); // make directory
			
			System.out.println("Target folder creation " + success);

		}

		// also create the semoss home folder
		String appHome = this.targetDirName + "/" + "semoss";
		targetDir = new File(appHome);
		if(!targetDir.exists())
			targetDir.mkdir(); // make app home directory 
		
		
		// also create the semoss home folder
		String appHome2 = this.targetDirName + "/" + user;
		targetDir = new File(appHome2);
		if(!targetDir.exists())
			targetDir.mkdir(); // make app home directory 
		
	}
	
	public MountHelper(String targetDirName)
	{
		this.targetDirName = targetDirName;
		File targetDir = new File(targetDirName);
		if(!targetDir.exists()) {
			System.out.println("Target folder doesn't exist. Making folder now at: " + targetDirName);

			boolean success = targetDir.mkdir(); // make directory
			
			System.out.println("Target folder creation " + success);

		}

		// also create the semoss home folder
		String appHome = this.targetDirName + "/" + "semoss";
		targetDir = new File(appHome);
		if(!targetDir.exists())
			targetDir.mkdir(); // make app home directory 
		
	}
	
	// make the directories for mount
	public void mountTarget(String sourceDirName)
	{
		File sourceDir = new File(sourceDirName);
		if(!sourceDir.exists())
			System.out.println("Source directory not available" + sourceDirName);
				
		// list files from source file and make a copy
		String [] allSourceFiles = sourceDir.list();
		
		for(int sourceFileIndex = 0;sourceFileIndex < allSourceFiles.length;sourceFileIndex++)
		{
			String srcPath = sourceDirName + "/" + allSourceFiles[sourceFileIndex];
			
			File thisFile = new File(srcPath);
			if(thisFile.isDirectory())
			{
				String targetPath = targetDirName + "/"  + allSourceFiles[sourceFileIndex];
				
				File thisTargetFile = new File(targetPath);
				thisTargetFile.mkdir();
				mountDir(srcPath, targetPath, false);
 			}
		}
		
		// completed
	}
	
	// mount directory
	public void mountDir(String srcDir, String tgtDir, boolean sudo)
	{
		try {
			ProcessBuilder pb = null;
			if(sudo)
				pb = new ProcessBuilder(new String[] {"sudo", "bindfs", "-r", srcDir, tgtDir});
			else
				pb = new ProcessBuilder(new String[] {"bindfs", "-r", srcDir, tgtDir});	
			//Process pb = Runtime.getRuntime().exec(new String[] {"mkdir", "/home/prabhuk/mn1/bin2"});
			Process p = pb.start();
			//Thread.sleep(5000);
			//p.waitFor(5, TimeUnit.SECONDS);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	// unmount
	public void unmountDir(String srcDir, boolean sudo)
	{
		System.err.println("Unmounting.. " + srcDir);
		try {
			ProcessBuilder pb = null;
			if(sudo)
				pb = new ProcessBuilder(new String[] {"sudo", "umount", srcDir});
			else
				pb = new ProcessBuilder(new String[] {"umount", srcDir});	
			
			//Process pb = Runtime.getRuntime().exec(new String[] {"mkdir", "/home/prabhuk/mn1/bin2"});
			Process p = pb.start();
			//Thread.sleep(5000);
			//p.waitFor(5, TimeUnit.SECONDS);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void deleteTarget(boolean sudo)
	{
		File tgtDir = new File(targetDirName);

		try {
			ProcessBuilder pb = null;
			if(sudo)
				pb = new ProcessBuilder(new String[] {"sudo", "rm - r", targetDirName});
			else
				pb = new ProcessBuilder(new String[] {"umount", targetDirName});	
			
			//Process pb = Runtime.getRuntime().exec(new String[] {"mkdir", "/home/prabhuk/mn1/bin2"});
			Process p = pb.start();
			//Thread.sleep(5000);
			//p.waitFor(5, TimeUnit.SECONDS);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	// removes all the mount session folder
	public void unmountTarget()
	{
		File tgtDir = new File(targetDirName);

		String [] allSourceFiles = tgtDir.list();
		
		for(int tgtFileIndex = 0;tgtFileIndex < allSourceFiles.length;tgtFileIndex++)
		{
			String tgtPath = targetDirName + "/" + allSourceFiles[tgtFileIndex];
			
			File thisFile = new File(tgtPath);
			if(thisFile.isDirectory())
			{
				unmountDir(tgtPath, true);
 			}
		}
	}
	
	// move the R and Py folders
	// create a custom RDF Map
	public void createCustomRDFMap()
	{
		// properties I need in a string array
		String [] propsNeeded = new String[] {Constants.BASE_FOLDER, Constants.NETTY_R, Constants.NETTY_PYTHON, Constants.USE_R, Constants.USE_PYTHON, Constants.R_MEM_LIMIT, Settings.MVN_HOME,Settings.REPO_HOME};
		
		Properties prop = new Properties();
		for(int propIndex = 0;propIndex < propsNeeded.length;propIndex++)
		{
			String key = propsNeeded[propIndex];
			String value = DIHelper.getInstance().getProperty(key);
			
			if(key != null && value != null)
				prop.put(key, value);
			System.err.println("Writing Value " + key + " <> " + value);
		}
		
		// set the base folder
		// forcing it to be /semoss
		prop.put(Constants.BASE_FOLDER, "/semoss");
		
		// write this out as RDF_MAP
		File file = new File("/semoss/RDF_MAP.prop");
		try {
			FileOutputStream fos = new FileOutputStream(file);
			prop.store(fos, "Chrooted Output");
			fos.flush();
			fos.close();
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

