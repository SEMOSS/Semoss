package prerna.util;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class MountHelper {

	String targetDirName = null;
	
	public static void main(String[] args) {
		
		// TODO Auto-generated method stub
		//String sourceDirName = "c:/users/pkapaleeswaran/workspacej3";
		//String targetDirName = "c:/users/pkapaleeswaran/workspacej3/mysession";
		String sourceDirName = "/home/prabhuk/db1"; // the mainbootstrap directory
		String targetDirName = "/home/prabhuk/session";

		// we can instrument the maindebootstrap folder through RDF Map here
		
		
		MountHelper maker = new MountHelper(targetDirName);

		//make the target
//		maker.mountTarget(sourceDirName);
//		maker.mountDir("/proc", targetDirName + "/proc", true);
//		maker.mountDir("/dev", targetDirName + "/dev", true);
		
		
		
		// remove the target
		maker.unmountTarget();
		maker.unmountDir(targetDirName + "/proc", true);
		maker.unmountDir(targetDirName + "/dev", true);
//		
//		// delete the target
//		maker.deleteTarget(true);
	}
	
	public MountHelper(String targetDirName)
	{
		this.targetDirName = targetDirName;
		File targetDir = new File(targetDirName);
		if(!targetDir.exists())
			targetDir.mkdir(); // make directory

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
			throw new RuntimeException("Source directory not available" + sourceDirName);
				
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
				mountDir(srcPath, targetPath, true);
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
				pb = new ProcessBuilder(new String[] {"sudo", "mount", "--bind", "-o", "ro", srcDir, tgtDir});
			else
				pb = new ProcessBuilder(new String[] {"mount", "--bind", "-o", "ro", srcDir, tgtDir});	
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
}

