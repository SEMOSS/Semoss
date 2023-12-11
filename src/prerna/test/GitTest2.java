//package prerna.test;
//
//import java.io.BufferedReader;
//import java.io.File;
//import java.io.FileWriter;
//import java.io.InputStreamReader;
//import java.io.PrintWriter;
//import java.util.List;
//
//import prerna.util.Utility;
//import prerna.util.git.GitDiffUtils;
//import prerna.util.git.GitFetchUtils;
//import prerna.util.git.GitRepoUtils;
//import prerna.util.git.GitUtils;
//
//public class GitTest2 {
//	
////	public static void main(String [] args) throws Exception
////	{
////		//
////		// clone from a git
////		// ask user for the file to change - SEMOSS.INIT
////		// browse / print assets in the folder
////		// modify the file
////		// add the file
////		// commit the file
////		// get the last 2 commits
////		// find the difference between 
////		// print the previous version of the file
////		
////		String baseFolder = "c:/users/pkapaleeswaran/workspacej3/git/Trial1";
////		String remoteRepo = "https://github.com/prabhuk12/Mv3";
////		
////		String fileToTouch = "SEMOSS.INIT";
////		
////		
////		// add cert for domain
////		GitRepoUtils.addCertForDomain(remoteRepo);
////		
////		// clone
////		String localFolderName = Utility.getInstanceName(remoteRepo);
////		GitFetchUtils.cloneApp(remoteRepo, baseFolder + "/" + localFolderName);
////		
////		// touch the file
////		String output = GitUtils.getDateMessage("Edited on");
////		File file = new File(baseFolder + "/" + localFolderName + "/" + fileToTouch);
////		PrintWriter pw = new PrintWriter(new FileWriter(file, true), true);
////		pw.write(output);
////		pw.close();
////
////		// add and commit
////		GitRepoUtils.addAllFiles(baseFolder + "/" + localFolderName, true);
////
////		GitRepoUtils.commitAddedFiles(baseFolder + "/" + localFolderName);
////		
////		// get the last 2 commits
////		// this is tricky has to be user driven
////		List commits = GitRepoUtils.listCommits(baseFolder + "/" + localFolderName, null);
////		
////		System.out.println("Commits !!" + commits);
////		
////		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
////		System.out.println("Enter commit id to revert");
////		String commitId = br.readLine();
////		
////		// if you want to revert the commit
////		//gt.revertCommit(gitFolder, comm);
////		
////		System.out.println("Enter another commit for Diff");
////		String commitId2 = br.readLine();
////		
////		String diff = GitDiffUtils.printDiff(baseFolder + "/" + localFolderName, commitId, commitId2, fileToTouch);
////
////		System.out.println("Diff !!" + diff);
////		
////		// show the contents in a particular world
////		String fileOutput = GitRepoUtils.getFile(commitId2, fileToTouch, baseFolder + "/" + localFolderName);
////		
////		System.out.println("File !! " + fileOutput);
////		
////		GitRepoUtils.resetCommit(baseFolder + "/" + localFolderName, commitId2);
////
////		
////	}
//
//}
