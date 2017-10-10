package prerna.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.codehaus.plexus.util.FileUtils;
import org.eclipse.egit.github.core.client.GitHubClient;
import org.eclipse.egit.github.core.service.RepositoryService;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.AbortedByHookException;
import org.eclipse.jgit.api.errors.CheckoutConflictException;
import org.eclipse.jgit.api.errors.ConcurrentRefUpdateException;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.errors.InvalidRefNameException;
import org.eclipse.jgit.api.errors.InvalidRemoteException;
import org.eclipse.jgit.api.errors.NoFilepatternException;
import org.eclipse.jgit.api.errors.NoHeadException;
import org.eclipse.jgit.api.errors.NoMessageException;
import org.eclipse.jgit.api.errors.RefAlreadyExistsException;
import org.eclipse.jgit.api.errors.RefNotFoundException;
import org.eclipse.jgit.api.errors.TransportException;
import org.eclipse.jgit.api.errors.UnmergedPathsException;
import org.eclipse.jgit.api.errors.WrongRepositoryStateException;
import org.eclipse.jgit.errors.AmbiguousObjectException;
import org.eclipse.jgit.errors.IncorrectObjectTypeException;
import org.eclipse.jgit.errors.NoWorkTreeException;
import org.eclipse.jgit.errors.RevisionSyntaxException;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.lib.StoredConfig;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.transport.CredentialsProvider;
import org.eclipse.jgit.transport.RefSpec;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;
import org.kohsuke.github.GHCreateRepositoryBuilder;
import org.kohsuke.github.GHRepository;
import org.kohsuke.github.GHUser;
import org.kohsuke.github.GitHub;

public class GitHelper {
	
	// this class is primarily responsible for all the git related operations such as
	// creating a local repository
	// creating a remote repository
	// checking to see if a remote repository with a specific name exists
	// adding / removing collaborator
	// search for a given collaborator
	// add an alias to the remote repository
	// 
	
	
	// SEMOSSS database
	// Local Repo | Remote Repo | Remote Repo Alias | (User's Role) Author / Collaborator / Browser | Alias Utilized | Other Collaborator | URL (optional <-- right now assumed to be github)
	// The user can use different Aliases if the user chooses to, and the alias at that point will get recorded
	//
	
	public boolean checkLocalRepository(String repositoryName) throws IOException
	{
		File dirFile = new File(repositoryName);
		// see if such a directory already exists
		return (dirFile.isDirectory() && dirFile.isDirectory());
	}

	public boolean checkRemoteRepository(String repositoryName, String userName, String password) throws IOException
	{
		GitHubClient client = new GitHubClient();
		client = client.createClient("https://github.com");
		if(password != null)
			client.setCredentials("prabhuk12", "g2thub123");
		RepositoryService service = new RepositoryService(client);
/*		for (org.eclipse.egit.github.core.Repository repo : service.getRepositories("prabhuk12"))
		  System.out.println(repo.getName() + " Watchers: " + repo.getWatchers());	
*/		
		/*org.eclipse.egit.github.core.Repository repository=new org.eclipse.egit.github.core.Repository();
		  repository.setOwner(new User().setLogin(client.getUser()));
		  String name = "test-create-" + System.currentTimeMillis();
		  repository.setName(name);
		  repository.setPrivate(false);
		  //Repository created=service.createRepository(repository);
		   * 
		   */
		boolean returnVal = true;
		try
		{
			service.getRepository(userName, repositoryName);
		}catch (Exception ex)
		{
			returnVal = false;
		}
		
		  return returnVal;
	}

	
	public void makeLocalRepository(String repositoryName) throws GitAPIException, IOException
	{
		makeLocalRepository(repositoryName, false, null, null);	
	}
	
	public void deleteLocalRepository(String repositoryName)
	{
		// need to do the subdirectory deletes first
		try {
			File dirFile = new File(repositoryName);
			if(dirFile.exists() && dirFile.isDirectory())
				FileUtils.forceDelete(dirFile);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	public void makeLocalRepository(String repositoryName, boolean sync, String userName, String password) throws GitAPIException, IOException
	{
		try {
			// see if such a directory already exists
			/*if(checkLocalRepository(repositoryName))
			{
				throw new java.io.IOException("The Directory is already present");
			}
			else */
			if(!sync)
			{				
				File dirFile = new File(repositoryName);
				Git.init().setDirectory(dirFile).call();
				Git.open(dirFile).close();
			}
			else
			{
				// make a remote repository and if it is an exception throw it
				makeRemoteRepository(repositoryName, userName, password);
			}
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (GitAPIException e) {
			// TODO Auto-generated catch block
			throw e;
		}
	}

	public void makeRemoteRepository(String repositoryName, String userName, String password) throws IOException 
	{
		if(checkRemoteRepository(repositoryName, userName, password))
		{
			//throw new java.io.IOException("The remote repository " + repositoryName + " is already present");
		}
		else
		{
			GitHub gh = GitHub.connectUsingPassword(userName, password);
			GHCreateRepositoryBuilder ghr = gh.createRepository(repositoryName).description(getDateMessage("Repository created on ") + " By user " + userName);
			ghr.autoInit(true).create();
			System.out.println("Repository created");
		}
	}

	
	public void deleteRemoteRepository(String repositoryName, String userName, String password) throws IOException 
	{
		if(checkRemoteRepository(repositoryName, userName, password))
		{
			//throw new java.io.IOException("The remote repository " + repositoryName + " is already present");
			GitHub gh = GitHub.connectUsingPassword(userName, password);
			GHRepository ghr = gh.getRepository(repositoryName); //.description(getDateMessage("Repository created on ") + " By user " + userName);
			ghr.delete();
		}
	}

	public void addFiles(String localRepository, String [] files)
	{
		try {
			Git thisGit = Git.open(new File(localRepository));

			//thisGit.checkout().setCreateBranch(true).setName("random").call();
			//thisGit.reset().setMode(ResetType.HARD).call();
			//thisGit.checkout().setName("master").call();
			//thisGit.checkout().setName("HEAD").call();
			for(int fileIndex = 0;fileIndex < files.length;fileIndex++)
				thisGit.add().addFilepattern(files[fileIndex]).call();
		} catch (NoFilepatternException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoHeadException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoMessageException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnmergedPathsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ConcurrentRefUpdateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (WrongRepositoryStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (AbortedByHookException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (GitAPIException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void addFile(String localRepository, String file)
	{
		try {
			Git thisGit = Git.open(new File(localRepository));

			//thisGit.checkout().setCreateBranch(true).setName("random").call();
			//thisGit.reset().setMode(ResetType.HARD).call();
			//thisGit.checkout().setName("master").call();
			//thisGit.checkout().setName("HEAD").call();
			thisGit.add().addFilepattern(file).call();
		} catch (NoFilepatternException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoHeadException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoMessageException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnmergedPathsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ConcurrentRefUpdateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (WrongRepositoryStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (AbortedByHookException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (GitAPIException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void syncRepository(String localRepositoryName, String remoteRepositoryName, String userName, String password, boolean owner) throws IOException
	{
		// .this assumes that both local and remote exists
		// first do a pull
		// and then if this user is the owner do a push
		// and the remote has already been added
		// nothing much to do here
		
		// the local repository first
		File file = new File(localRepositoryName);
		Repository thisRepo = Git.open(file).getRepository();

		// this has to be a database call to get the remote alias
		String remoteAlias = remoteRepositoryName;
		// fetch all the changes
		RefSpec spec = new RefSpec("refs/heads/master:refs/remotes/origin/master");
		List <RefSpec> refList = new ArrayList<RefSpec>();
		refList.add(spec);
		fetchRemote(localRepositoryName, remoteRepositoryName, userName, password);
		// now commit the changes
		if(owner)
		{
			pushToRemote(localRepositoryName, remoteRepositoryName, userName, password, true);
		}
		
	}
	
	public void tryPush()
	{
		try {
			System.out.println("Processing Push");
			org.eclipse.jgit.pgm.Main.main(new String[]{"--git-dir", "Trial1/.git", "push", "Trial1", "HEAD:master"});
			System.out.println("Finished");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void addRemote(String localRepository, String repositoryName, String userName) 
	{
		try
		{
			makeLocalRepository(localRepository);
		}catch(IOException ex)
		{
			// repository exists at this point
		} catch (GitAPIException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			StoredConfig config = Git.open(new File(localRepository)).getRepository().getConfig();
			config.setString("remote", repositoryName , "url", "https://github.com/" + userName + "/" + repositoryName);
			config.save();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	
	public void fetchRemote(String localRepo, String remoteRepo, String userName, String password)
	{
		File file = new File(localRepo);
		RefSpec spec = new RefSpec("refs/heads/master:refs/remotes/origin/master");
		List <RefSpec> refList = new ArrayList<RefSpec>();
		refList.add(spec);
		try {
			CredentialsProvider cp = new UsernamePasswordCredentialsProvider(userName, password);
			Git thisGit = Git.open(file);
			//thisGit.checkout().setName("master").call();
			thisGit.fetch().setCredentialsProvider(cp).setRemote(remoteRepo).setRefSpecs(refList).call();
			thisGit.getRepository().resolve("FETCH_HEAD");
			thisGit.clean().call();
			// that is the call to get to the fetch head
			thisGit.checkout().setName("FETCH_HEAD").call();
			//thisGit.checkout().setName("master").call();
			
			//thisGit.checkout().setName("master").call();
			thisGit.close();
		} catch (RevisionSyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoWorkTreeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidRemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TransportException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (AmbiguousObjectException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IncorrectObjectTypeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RefAlreadyExistsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RefNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidRefNameException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (CheckoutConflictException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (GitAPIException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	// I wont clone the repository here but make one add remote and fetch it
	public void cloneRemoteRepo(String userName, String remoteRepositoryName, String password)
	{
		// there are a couple of things here
		// this could be an existing repository
		
		// make a local repository by the same name
		// add this repository as a remote
		// fetch from it

		
		try {
			//makeLocalRepository(remoteRepositoryName);
			//Git thisGit = Git.open(new File(remoteRepositoryName));
			//Git.cloneRepository().setURI("https://github.com/" + userName + "/" + remoteRepositoryName).setDirectory(new File(remoteRepositoryName)).setCloneAllBranches(true).call();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		// make the repo
		addRemote(remoteRepositoryName, remoteRepositoryName, userName);
		
		// fetch from it
		fetchRemote(remoteRepositoryName, remoteRepositoryName, userName, password);
		
	}
	
	public void swapSparseForRemote(String localRepository, String remoteRepository)
	{
		File myFile2 = new File(localRepository + "\\.git\\info\\sparse-checkout-" + remoteRepository);
		File sparseFile = new File(localRepository + "\\.git\\info\\sparse-checkout");
		if(sparseFile.exists())
			sparseFile.delete();
		sparseFile = new File(localRepository + "\\.git\\info\\sparse-checkout");
		if(myFile2.exists())
		{
			//Files.copy
		}
	}
	
	// add files for checkout
	public void addFilesForCheckout(String localRepository, String remoteRepository, String [] files)
	{
		// I also have to employ another tactic here
		// if the remote repository has been added then keep that sparse checkout
		// else people can game this as well
		// right now I am assuming only one repo per repository
		try {
			StoredConfig config = Git.open(new File(localRepository)).getRepository().getConfig();

			config.setString("core", null, "sparseCheckout", "true");
			config.save();
			
			File myNewFile = new File(localRepository + "\\.git\\info"); //\\sparse-checkout");
			if(!myNewFile.exists())
				myNewFile.mkdir();
			File myFile2 = new File(localRepository + "\\.git\\info\\sparse-checkout-" + remoteRepository);
			
			PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(myFile2)));
			for(int fileIndex = 0; fileIndex <= files.length;fileIndex++)
				pw.println(files[fileIndex]);
			pw.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	public void addNewFile(String localRepository)
	{

		try {
			Git thisGit = Git.open(new File(localRepository));
			
			String fileName = "\\my2cents2" + System.currentTimeMillis() +".txt";
			System.out.println("Adding a file " + fileName);
			File myNewFile = new File(localRepository + fileName);
			PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(myNewFile)));
			pw.println("Ok.. this is my 2 cents on this.. push experiment");
			pw.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void commit(String localRepository)
	{
		commit(localRepository, "Commit processed on ");
	}

	
	public void commit(String localRepository, String message)
	{
		try {
			Git thisGit = Git.open(new File(localRepository));
			thisGit.commit().setMessage(getDateMessage(message)).setAll(true).call();
		} catch (NoHeadException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoMessageException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnmergedPathsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ConcurrentRefUpdateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (WrongRepositoryStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (AbortedByHookException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (GitAPIException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	
	// push to remote
	public void pushToRemote(String localRepoName, String remoteRepo, String userName, String password, boolean add)
	{
			try {
				Git thisGit = Git.open(new File(localRepoName));
				CredentialsProvider cp = new UsernamePasswordCredentialsProvider(userName, password);

				//thisGit.checkout().setCreateBranch(true).setName("random").call();
				//thisGit.reset().setMode(ResetType.HARD).call();
				//thisGit.checkout().setName("master").call();
				//thisGit.checkout().setName("HEAD").call();
				if(add)
				{
					addFile(localRepoName, ".");
					commit(localRepoName);
				}
				// using runtime
				Runtime.getRuntime().exec("cmd /c cd " + localRepoName + " && git push " + remoteRepo + " HEAD:master");


				// using jgit
				/*
				RefSpec spec = new RefSpec("+refs/remotes/origin/master:refs/remotes/origin/master");
				//RefSpec spec = new RefSpec("FETCH_HEAD:master");
				//thisGit.merge().include(thisGit.getRepository().exactRef("random")).call();
				thisGit.push().setForce(true).setCredentialsProvider(cp).setRemote(remoteRepo).setRefSpecs(spec).call();

*/			
				thisGit.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}			
	}
	
	// add a collaborator to remote repository
	public void addCollaborator(String remoteRepositoryName, String userName, String password, String collaborator)
	{
		try {
			GitHub gh = GitHub.connectUsingPassword(userName, password);
			
			GHRepository ghr = gh.getRepository(userName + "/" + remoteRepositoryName);
			
			Collection <GHUser> collabs = new Vector<GHUser>();
			collabs.add(gh.getUser(collaborator));
			ghr.addCollaborators(collabs);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//ghr.removeCollaborators(collabs);

	}

	public void removeCollaborator(String remoteRepositoryName, String userName, String password, String collaborator)
	{
		try {
			GitHub gh = GitHub.connectUsingPassword(userName, password);
			
			GHRepository ghr = gh.getRepository(userName + "/" + remoteRepositoryName);
			
			Collection <GHUser> collabs = new Vector<GHUser>();
			collabs.add(gh.getUser(collaborator));
			ghr.removeCollaborators(collabs);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void rebaseRepository(String localRepository, String checkoutPoint)
	{
		// will come to this
	}
	
	// move the file to a particular point
	// if no file send null
	public void checkout(String localRepository, String checkoutPoint, String file)
	{
		try {
			printAllFiles(localRepository);
			Git thisGit = Git.open(new File(localRepository));
			//thisGit.branchRename().setNewName("master");
			//ObjectId prevId = thisGit.getRepository().resolve(checkoutPoint);
			System.out.println("checking out to a checkpoint " + checkoutPoint);
			if(file != null)
				thisGit.checkout().setName(checkoutPoint).addPath(file).call();
			else
				thisGit.checkout().setName(checkoutPoint).call();
			
			printAllFiles(localRepository);
			//thisGit.getRepository().resolve(checkoutPoint);
			//thisGit.reset().setMode(ResetType.HARD).setRef(checkoutPoint);
		}catch(Exception ex)
		{
			
		}
	}
	
	public void printAllFiles(String localRepository)
	{
		File file = new File(localRepository);
		if(file.isDirectory())
		{
			File [] files = file.listFiles();
			for(int fileIndex = 0;fileIndex < files.length;fileIndex++)
				System.out.println(files[fileIndex].getName());
		}

	}

	public Vector <String[]> getLogs(String localRepository, String fileName, boolean date, int count)
	{
		Vector <String[]> retLog = new Vector<String[]>();
		try {
			Git thisGit = Git.open(new File(localRepository));
			Iterator <RevCommit> fileLogs = null;
			if(fileName != null)
				fileLogs = thisGit.log().addPath(fileName).call().iterator();
			else
				fileLogs = thisGit.log().call().iterator();
				
			int logCount = 0;
			
			while(fileLogs.hasNext() && logCount < count)
			{
				RevCommit thisCommit =fileLogs.next(); 
				String thisLog = thisCommit.getShortMessage();
				String id = thisCommit.getId() + "";
				id = id.split(" ")[1];
				
				if(date && thisLog.indexOf("<d>") >= 0)
					thisLog = thisLog.substring(thisLog.indexOf("<d>"));
				String [] dual = new String [2];
				dual[0] = id;
				dual[1] = thisLog;
				retLog.add(dual);
				logCount++;
			}
		} catch (NoHeadException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (GitAPIException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return retLog;
	}
	
	public String getDateMessage(String prefixString)
	{
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		return prefixString + " <d>" + dateFormat.format(date);
	}	
	
	public static void main(String [] args) throws Exception
	{
		GitHelper helper = new GitHelper();
		String repoName = "Trial5";
		String remoteRepoName = "Trial6";
		String userName = "prabhuk12";
		String password = "g2thub123";
		
		// usecase 1 - pull a remote repository into a local repository
		// Usecase 2 - start with local and then commit to the remote
		// usecase 3 - pull from a remote, add changes and then commit back to a different remote
		
		helper.fetchRemote(repoName, remoteRepoName, userName, password);

		
		Vector <String[]> logs = helper.getLogs(repoName, null, false, 20);
		String [] dual = logs.get(logs.size()-1);
		helper.checkout(repoName, dual[0], null);

		helper.addFile(repoName, ".");
		helper.commit(repoName);
		// clone
		//helper.cloneRemoteRepo(userName, remoteRepoName);
		
		helper.pushToRemote(repoName, remoteRepoName, userName, password, true);
		/*
		// make a local repository
		helper.makeLocalRepository(repoName);
		
		// check for a remote repo
		helper.makeRemoteRepository(remoteRepoName, userName, password);
		
		// add remote
		helper.addRemote(repoName, remoteRepoName, userName);		
		
		// fetch it
		// always have to fetch before the commit
		helper.fetchRemote(repoName, remoteRepoName, userName, password);
		
		// make a new file
		helper.addNewFile(repoName);
		
		helper.printAllFiles(repoName);

		// push it back
		helper.pushToRemote(repoName, remoteRepoName, userName, password, true);
		//helper.tryPush();
		
		// synchronize the repository
		//helper.syncRepository(repoName, "Trial1", "prabhuk12", "g2thub123", true);
		*/
	}
	
}
