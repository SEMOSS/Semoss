package prerna.util.git;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kohsuke.github.GHCommit;
import org.kohsuke.github.GHCommit.File;
import org.kohsuke.github.GHContent;
import org.kohsuke.github.GHContentUpdateResponse;
import org.kohsuke.github.GHRepository;
import org.kohsuke.github.GHUser;
import org.kohsuke.github.GitHub;
import org.kohsuke.github.PagedIterator;

import prerna.om.RemoteItem;
import prerna.util.Constants;


public class GitAssetMaker {

	public static final String assetRepoName = "_ASSETS";
	
	private static final Logger classLogger = LogManager.getLogger(GitAssetMaker.class);

	public static List<RemoteItem> listAssets(String assetFolder, String username, String password) throws Exception
	{
		List <RemoteItem> retList = new Vector<RemoteItem>();
		
		GitHub gh = null;
		
		// should I synchronize first before listing these assets ?
		// https://raw.githubusercontent.com/prabhuk12/Mv2/master/Mv2.smss
		if(password != null)
			gh = GitUtils.login(username, password);
		else
			gh = GitHub.connectAnonymously();
		
		// now get the specific repository
		// GHRepository repo = gh.getRepository("prabhuk12/Mv2");
		
		
		
		GHRepository repo = gh.getRepository(assetFolder);
		
		PagedIterator <GHCommit> refs = repo.listCommits().iterator();
		
		// the first one is the latest
		// usually
		// I am not going to process folder here
		
		GHCommit lastCommit = null;
		if(refs.hasNext())
			lastCommit = refs.next();
		
		
		
		if(lastCommit != null)
		{
			System.out.println(lastCommit.getCommitShortInfo().getMessage());
			System.out.println(lastCommit.getCommitDate());
			
			List <File> fileList = lastCommit.getFiles();
			
			
			for(int fileIndex = 0;fileIndex < fileList.size();fileIndex++)
			{
				File thisFile = fileList.get(fileIndex);
				RemoteItem thisItem = new RemoteItem();
				
				thisItem.setName(thisFile.getFileName());
				thisItem.setId(thisFile.getSha());
				thisItem.setDescription(thisFile.getFileName());
				
				//System.out.println("File is.. " + fileList.get(fileIndex).getFileName());
				//System.out.println("File is.. " + fileList.get(fileIndex).getRawUrl());
				//System.out.println("File is.. " + fileList.get(fileIndex).getBlobUrl());
				
				retList.add(thisItem);
			}
		}
		
		return retList;
	}
	
	// if the repository is not there.. create a repository
	public static void createAssetRepo(String oauthToken)
	{
		try {
			GitHub gh = GitUtils.login(oauthToken);
			
			GHUser user = gh.getMyself();
			String loginName = user.getLogin();
			
			// need something to get hte git profile
			if(!GitRepoUtils.checkRemoteRepositoryO(loginName + "/ " + loginName + assetRepoName,  oauthToken))
				GitRepoUtils.makeRemoteRepository(gh, loginName, loginName + assetRepoName);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		}
		
	}
	
	// get the file content for a given file
	public static void makeAsset(String oauth, String repoName, String content, String fileName)
	{
		try {
			// https://raw.githubusercontent.com/prabhuk12/Trial1/master/Direct.file
			GitHub gh = GitHub.connectUsingOAuth(oauth);
			
			GHRepository repo = gh.getRepository(repoName);
			GHContentUpdateResponse resp = repo.createContent(content.getBytes(), getDateMessage("Created On"), fileName);
			
			//gh.
			//resp.getCommit().getS
		} catch (IOException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		}		
	}
	

	// get the file content for a given file
	public static void updateAsset(String oauth, String repoName, String content, String fileName)
	{
		try {
			// https://raw.githubusercontent.com/prabhuk12/Trial1/master/Direct.file
			GitHub gh = GitHub.connectUsingOAuth(oauth);
			
			GHRepository repo = gh.getRepository(repoName);

			GHContent ghc = repo.getFileContent(fileName);
			
			ghc.update(content, getDateMessage("Updated On"));

		} catch (IOException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		}		
	}


	// get the file content for a given file
	public static void deleteAsset(String oauth, String repoName, String fileName)
	{
		try {
			// https://raw.githubusercontent.com/prabhuk12/Trial1/master/Direct.file
			GitHub gh = GitHub.connectUsingOAuth(oauth);
			
			GHRepository repo = gh.getRepository(repoName);

			GHContent ghc = repo.getFileContent(fileName);

			ghc.delete(getDateMessage("Deleted On"));

		} catch (IOException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		}		
	}

	
	
	public static String getDateMessage(String prefixString) {
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		return prefixString + " : " + dateFormat.format(date);
	}

	
}
