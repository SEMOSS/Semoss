package prerna.util;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.codehaus.plexus.util.FileUtils;
import org.eclipse.egit.github.core.client.GitHubClient;
import org.eclipse.egit.github.core.service.RepositoryService;
import org.eclipse.jgit.api.CheckoutCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.MergeCommand;
import org.eclipse.jgit.api.MergeResult;
import org.eclipse.jgit.api.PushCommand;
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
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.diff.DiffEntry.ChangeType;
import org.eclipse.jgit.errors.AmbiguousObjectException;
import org.eclipse.jgit.errors.IncorrectObjectTypeException;
import org.eclipse.jgit.errors.NoWorkTreeException;
import org.eclipse.jgit.errors.RevisionSyntaxException;
import org.eclipse.jgit.lib.ObjectReader;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.lib.StoredConfig;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevTree;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.transport.CredentialsProvider;
import org.eclipse.jgit.transport.RefSpec;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;
import org.eclipse.jgit.treewalk.AbstractTreeIterator;
import org.eclipse.jgit.treewalk.CanonicalTreeParser;
import org.kohsuke.github.GHCreateRepositoryBuilder;
import org.kohsuke.github.GHRepository;
import org.kohsuke.github.GHUser;
import org.kohsuke.github.GitHub;
import org.kohsuke.github.PagedIterator;

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

	public static final String DUAL = "DUAL";
	public static final String SUBSCRIBE = "SUBSCRIBE";
	public static final String PUBLISH = "PUBLISH";

	public boolean checkLocalRepository(String repositoryName) throws IOException
	{
		File dirFile = new File(repositoryName);
		// see if such a directory already exists
		return (dirFile.isDirectory() && dirFile.isDirectory());
	}
	
	
	public boolean isCurrent(String repositoryName) throws IOException, IllegalStateException, GitAPIException
	{
		boolean retValue = true;
		
		// I need to update the remote first
		// https://stackoverflow.com/questions/3258243/check-if-pull-needed-in-git
		// git remote update 
		// git status uno
		File dirFile = new File(repositoryName);
		Git thisGit = Git.open(dirFile);
		//Git.open(dirFile).close();
		
		AbstractTreeIterator oldTreeParser = prepareTreeParser(thisGit.getRepository(), "refs/heads/master");
        AbstractTreeIterator newTreeParser = prepareTreeParser(thisGit.getRepository(), "refs/remotes/origin/master");		
     // then the procelain diff-command returns a list of diff entries
        List<DiffEntry> diff = thisGit.diff().setOldTree(oldTreeParser).setNewTree(newTreeParser).call();
        for (DiffEntry entry : diff) {
            System.out.println("Entry: " + entry);
            retValue = false;
        }
 		return retValue;
	}
	
	 private AbstractTreeIterator prepareTreeParser(Repository repository, String ref) throws IOException {
	        // from the commit we can build the tree which allows us to construct the TreeParser
	        Ref head = repository.exactRef(ref);
	        try (RevWalk walk = new RevWalk(repository)) {
	            RevCommit commit = walk.parseCommit(head.getObjectId());
	            RevTree tree = walk.parseTree(commit.getTree().getId());

	            CanonicalTreeParser treeParser = new CanonicalTreeParser();
	            try (ObjectReader reader = repository.newObjectReader()) {
	                treeParser.reset(reader, tree.getId());
	            }

	            walk.dispose();

	            return treeParser;
	        }
	    }
	 
	 public  boolean login(String username, String password)
	 {
		 boolean valid = true;
			try {
				GitHub gh = GitHub.connectUsingPassword(username, password);
				gh.getMyself();
				valid = true;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				valid = false;
			}
			return valid;

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
	
	public Vector<String> listRemotes(String username, String password)
	{
		Vector <String> remoteRepos = new Vector<String>();
		// right now assumes the username is the directory
		GitHubClient client = new GitHubClient();
		client = client.createClient("https://github.com");
		if(password != null)
			client.setCredentials(username, password);
		RepositoryService service = new RepositoryService(client);

		try {
			List<org.eclipse.egit.github.core.Repository> repList = service.getRepositories();
			for(int repIndex = 0;repIndex < repList.size();repIndex++)
			{
				System.out.println(" Name of Repository " + repList.get(repIndex).getName());
				remoteRepos.add(repList.get(repIndex).getName());
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return remoteRepos;
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
	
	public String[] getVersions(String localRepository)
	{
		try {
			Git thisGit = Git.open(new File(localRepository));
			Iterator <RevCommit> commits = thisGit.log().call().iterator();

			String goback = null;
			
			while(commits.hasNext())
			{
				RevCommit thisCommit = commits.next();
				String id = thisCommit.getId().getName();
				String message = thisCommit.getShortMessage();
				String ident = thisCommit.getCommitterIdent().getName() + " : " + thisCommit.getCommitterIdent().getEmailAddress();
				
				if(goback == null)
					goback = id;
				System.out.println(" Commit ... ");
				System.out.println(id + " <> " + message + " <> " + ident);
				
			}
		
			
			// try a random checkout
			
			CheckoutCommand cc = thisGit.checkout();
			cc.setName(goback);
			cc.setCreateBranch(false); // probably not needed, just to make sure
			cc.call(); 
		
		}	catch(Exception ex)
		{
			
		}
		
		return null;
	}

	public void merge(String localRepository, String startPoint, String branchName)
	{
		merge(localRepository, startPoint, branchName, true);
	}

	public void merge(String localRepository, String startPoint, String branchName, boolean autoResolve)
	{
		int curAttempts = 0;
		int maxAttempts = 2;
		if(autoResolve)
			merge(localRepository, startPoint, branchName, curAttempts, maxAttempts, false);
		else
			merge(localRepository, startPoint, branchName, 0,0, false);
	}
	
	
	
	public void merge(String localRepository, String startPoint, String branchName, int numAttempts, int maxAttempts, boolean delete)
	{
		try {
			Git thisGit = Git.open(new File(localRepository));
			
			CheckoutCommand cc = thisGit.checkout();
			Ref startRef = thisGit.getRepository().findRef(startPoint);

			cc.setName(startPoint);
			cc.setCreateBranch(false); // probably not needed, just to make sure
			if(startRef != null)
				cc.call(); 
			
			MergeCommand mc = thisGit.merge();
			Ref ref = thisGit.getRepository().findRef(branchName);
			if(ref != null && startRef != null)
			{
				mc.include(ref);
				
				MergeResult res = mc.call(); 
				boolean retBoolean = true;
				
				if (res.getMergeStatus().equals(MergeResult.MergeStatus.CONFLICTING))
				{
					   System.out.println(res.getConflicts().toString());
					   retBoolean = false;
					   Iterator <String> files = res.getConflicts().keySet().iterator();
					   Vector <String> delFiles = new Vector<String>();
					   while(files.hasNext())
					   {
						   String thisFile = files.next();
						   System.out.println("File is" + thisFile);
						   if(!delFiles.contains(thisFile))
							   delFiles.add(thisFile);
					   }
					   // inform the user he has to handle the conflicts
					   if(numAttempts < maxAttempts && delete)
					   {
							wipeFiles(delFiles);
							commitAll(localRepository, true);
							numAttempts++;
							// I will attempt this just one more time to merge
							merge(localRepository, startPoint, branchName, numAttempts, maxAttempts, delete);
					   }
				}			
			}
			} catch (CheckoutConflictException e) {
				if(delete)
				{
					List<String> delFiles = e.getConflictingPaths();
					wipeFiles(delFiles);
					commitAll(localRepository, true);
					numAttempts++;
					// I will attempt this just one more time to merge
					merge(localRepository, startPoint, branchName, numAttempts, maxAttempts, delete);
				}
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
	public void wipeFiles(List <String> filesToWipe)
	{
		for(int fileIndex = 0;fileIndex < filesToWipe.size();fileIndex++)
		{
			File file = new File(filesToWipe.get(fileIndex));
			if(file.exists())
				file.delete();
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
	
	/**
	 * Get the list of remote configurations associated with an app directory
	 * Get the url
	 * Get the namespace/appName
	 * Get the type -> dual or subscript
	 * @param localRepositoryName
	 * @return
	 */
	public List<Map<String, String>> listConfigRemotes(String localRepositoryName) {
		List<Map<String, String>> returnList = new Vector<Map<String, String>>();
		try {
			File file = new File(localRepositoryName);
			Repository thisRepo = Git.open(file).getRepository();
			
			String[] remNames = thisRepo.getRemoteNames().toArray(new String[]{});
			for(int remIndex = 0; remIndex < remNames.length; remIndex++) {
				String remName = remNames[remIndex] +"";
				String url = thisRepo.getConfig().getString("remote", remName, "url");
				String upstream = thisRepo.getConfig().getString(remName, "upstream",  "url");
				
				Map<String, String> remoteMap = new Hashtable<String, String>();
				remoteMap.put("url", url);
				String appName = Utility.getClassName(url) + "/" + Utility.getInstanceName(url);
				remoteMap.put("name", appName);
				if(upstream != null && upstream.equalsIgnoreCase("DEFUNCT")) {
					remoteMap.put("type", SUBSCRIBE);
				} else {
					remoteMap.put("type", DUAL);
				}
				System.out.println("We have remote with details " + remoteMap);
				returnList.add(remoteMap);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return returnList;
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
	
	public void removeRemote(String localRepository, String repositoryName) 
	{
		try {
			StoredConfig config = Git.open(new File(localRepository)).getRepository().getConfig();
			config.unsetSection("remote", repositoryName);
			config.save();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	
	public void fetchRemote(String localRepo, String remoteRepo, String userName, String password)
	{
		File file = new File(localRepo);
		RefSpec spec = new RefSpec("refs/heads/master:refs/remotes/" + remoteRepo +"/master");
		List <RefSpec> refList = new ArrayList<RefSpec>();
		refList.add(spec);
		try {
			CredentialsProvider cp = new UsernamePasswordCredentialsProvider(userName, password);
			Git thisGit = Git.open(file);
			//thisGit.checkout().setName("master").call();
			thisGit.fetch().setCredentialsProvider(cp).setRemote(remoteRepo).call();
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
	
	public void removeAllIgnore(String localRepository)
	{
		try
		{
			// remove from checkout
			StoredConfig config = Git.open(new File(localRepository)).getRepository().getConfig();
	
			config.setString("core", null, "sparseCheckout", "false");
			config.save();
			File myNewFile2 = new File(localRepository + "/.git/info/sparse-checkout");
			myNewFile2.delete();
			
			// remove from checkin
			File myNewFile = new File(localRepository + "/.gitignore"); //\\sparse-checkout");

			// I have to delete for now
			myNewFile.delete();
			
		}catch(Exception ex)
		{
			
		}
	}
	
	public void checkinIgnore(String localRepository, String [] files)
	{
		// this will go into the checkin ignore
		// a.k.a .gitignore
		try {
			
			// see if the old file is there
			
			File myNewFile = new File(localRepository + "/.gitignore"); //\\sparse-checkout");
			
			PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(myNewFile)));
			//pw.println("/*");
			for(int fileIndex = 0; fileIndex < files.length;fileIndex++)
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
	
	public void removeIgnores(String localRepository)
	{
		
		try {
			StoredConfig config = Git.open(new File(localRepository)).getRepository().getConfig();

			config.setString("core", null, "sparseCheckout", "false");
			config.save();
			

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
		
	}
	
	public void checkoutIgnore(String localRepository, String [] files)
	{
		// this will go into the sparse checkout
		try {
			StoredConfig config = Git.open(new File(localRepository)).getRepository().getConfig();

			config.setString("core", null, "sparseCheckout", "true");
			config.save();
			
			File myFile2 = new File(localRepository + "/.git/info/sparse-checkout");

			// dont create it if it exists
			if(!myFile2.exists())
			{
				File myNewFile = new File(localRepository + "/.git/info"); //\\sparse-checkout");
				if(!myNewFile.exists())
					myNewFile.mkdir();
				
				PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(myFile2)));
				pw.println("/*");
				for(int fileIndex = 0; fileIndex < files.length;fileIndex++)
					pw.println("!"+files[fileIndex]);
				pw.close();
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	public void addFilesToIgnore(String localRepository, String [] files)
	{
		try {
			StoredConfig config = Git.open(new File(localRepository)).getRepository().getConfig();

			config.setString("core", null, "sparseCheckout", "true");
			config.save();
			
			File myNewFile = new File(localRepository + "\\.git\\info"); //\\sparse-checkout");
			if(!myNewFile.exists())
				myNewFile.mkdir();
			File myFile2 = new File(localRepository + "\\.git\\info\\sparse-checkout");
			
			PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(myFile2)));
			pw.println("/*");
			for(int fileIndex = 0; fileIndex <= files.length;fileIndex++)
				pw.println("!"+files[fileIndex]);
			pw.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
	
	public Hashtable <String, String> searchUsers(String query, String userName, String password)
	{
		Hashtable <String, String> userHash = new Hashtable<String, String>();
		try {
			GitHub gh = GitHub.connectUsingPassword(userName, password);
			PagedIterator <GHUser> users = gh.searchUsers().q(query).list().iterator();
			
			for(int userIndex = 0;users.hasNext() && userIndex < 10;userIndex++)
			{
				GHUser user = users.next();
				String id = user.getLogin();
				String name = user.getName();
				name = name + ", Followers : " + user.getFollowersCount();
				name = name + ", Repositories : " + user.getRepositories().size();
				
				userHash.put(id,  name);
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return userHash;
	}
	
	public Vector<String> listCollaborators(String repo, String userName, String password)
	{
		Vector <String> collabVector = new Vector<String>(); 
		try {
			GitHub gh = GitHub.connectUsingPassword(userName, password);
			GHRepository ghr = gh.getRepository(userName + "/" + repo);
			Iterator <String> collabNames = ghr.getCollaboratorNames().iterator();
			
			while(collabNames.hasNext())
			{
				collabVector.add(collabNames.next());
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return collabVector;
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

	public Hashtable <String, List<String>> getFilesToAdd(String dir, String baseRepo, String newRepo)
	{
		// this assumes that you have run a fetch
		Hashtable <String, List<String>> finalHash = new Hashtable();
		
		// I need to update the remote first
		// https://stackoverflow.com/questions/3258243/check-if-pull-needed-in-git
		// git remote update 
		// git status uno
		try {
			File dirFile = new File(dir);
			Git thisGit = Git.open(dirFile);
			//Git.open(dirFile).close();
			
			
			AbstractTreeIterator oldTreeParser = prepareTreeParser(thisGit.getRepository(), baseRepo);
			AbstractTreeIterator newTreeParser = prepareTreeParser(thisGit.getRepository(), newRepo);		
			// then the procelain diff-command returns a list of diff entries
			List<DiffEntry> diff = thisGit.diff().setOldTree(oldTreeParser).setNewTree(newTreeParser).call();
			List <String> addFiles = new Vector<String>();
			List <String> modFiles = new Vector<String>();
			List <String> renFiles = new Vector<String>();
			List <String> delFiles = new Vector<String>();
			for (DiffEntry entry : diff) {
			    String fileName = dir + "/" + entry.getNewPath(); 
			    System.out.println("Entry: " + fileName);
			    System.out.println("File : " + entry.getNewPath());
			    System.out.println("File : " + entry.getOldId());
			    System.out.println("File : " + entry.getNewId());
			    if(entry.getChangeType() == ChangeType.ADD)
			    	addFiles.add(fileName);
			    if(entry.getChangeType() == ChangeType.MODIFY)
			    	modFiles.add(fileName);
			    if(entry.getChangeType() == ChangeType.RENAME)
			    	renFiles.add(fileName);
			    if(entry.getChangeType() == ChangeType.DELETE)
			    	delFiles.add(fileName);
			}
			if(addFiles.size() > 0)
				finalHash.put("ADD", addFiles);
			if(modFiles.size() > 0)
				finalHash.put("MOD", modFiles);
			if(renFiles.size() > 0)
				finalHash.put("REN", renFiles);
			if(delFiles.size() > 0)
				finalHash.put("DEL", delFiles);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (GitAPIException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        return finalHash;
 	}
	
	public void push(String repository, String remoteToPush, String branch)
	{
		try
		{
			File dirFile = new File(repository);
			Git thisGit = Git.open(dirFile);
			
			PushCommand pc = thisGit.push();
			Ref ref = thisGit.getRepository().findRef(branch);
			
			//pc.add(ref);
			RefSpec spec = new RefSpec("+refs/heads/*:refs/remotes/" + remoteToPush+"/*");
			pc.setRefSpecs(spec);
			pc.setRemote(remoteToPush);
			pc.call();
		}catch(Exception ex)
		{
			
		}
	}
	
	// command line starts here - PROCESS Builder
	//***************************************************************
	//***************************************************************
	//***************************************************************
	//***************************************************************
	//***************************************************************
	
	
	
	public void initDir(String dir)
	{
		
		// make the directory if not already there
		File dirFile = new File(dir);
		if(!dirFile.exists())
			dirFile.mkdir();
		
		List <String> commands = new Vector<String>();
		commands.add("git");
		commands.add("init");
		
		runProcess(dir, commands);

		commands = new Vector<String>();
		commands.add("git");
		commands.add("checkout");
		commands.add("master");
		
		runProcess(dir, commands, true);

	}
	
	public void semossInit(String dir)
	{
		String newFile = dir + "/SEMOSS.INIT";
		File myFile = new File(newFile);
		try {
			myFile.createNewFile();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
		
	public void addRemote(String dir, String remoteRepo, boolean dual)
	{
		String repoName = Utility.getInstanceName(remoteRepo);
		addRemote(dir, repoName, remoteRepo, true);		
	}

	public void addRemote(String dir, String repoName, String remoteRepo, boolean dual)
	{
		Vector commands = new Vector<String>();
		commands.add("git");
		commands.add("remote");
		commands.add("add");		
		commands.add(repoName);
		commands.add(remoteRepo);	
		runProcess(dir, commands);
		if(!dual)
		{
			commands = new Vector<String>();
			commands.add("git");
			commands.add("config");
			commands.add(repoName+".upstream.url");
			commands.add("DEFUNCT");	
			runProcess(dir, commands);
		}
		
	}

		
	public void fetchRemote(String dir, String repoName)
	{
		List <String> commands = new Vector<String>();

		// all com
		commands = new Vector<String>();

		commands.add("git");
		commands.add("fetch");
		commands.add(repoName);
		runProcess(dir, commands, true);
		
		
	}

	public void pullRemote(String dir, String repoName)
	{
		List <String> commands = new Vector<String>();

		// all com
		commands = new Vector<String>();

		commands.add("git");
		commands.add("pull");
		commands.add(repoName);
		runProcess(dir, commands, true);
		
		
	}

	public void checkout(String dir, String repoPoint)
	{
		List <String> commands = new Vector<String>();
		// move to that master
		commands = new Vector<String>();
		commands.add("git");
		commands.add("checkout");
		commands.add(repoPoint);
		
		runProcess(dir, commands, true);

	}
	
	public void mergeRepo(String dir, String repoName)
	{
		String [] repos = {repoName};
		mergeRepos(dir, repos);
	}
	
	// this is going to be tricky when they are disconnected pieces
	// https://gist.github.com/msrose/2feacb303035d11d2d05
	public void mergeRepos(String dir, String [] repoNames)
	{
		// need to go to the main master first
		
		// need to merge every repo one by one
		List <String> commands = new Vector<String>();
		commands.add("git");
		commands.add("merge");
		commands.add("--allow-unrelated-histories");
		for(int repoIndex = 0;repoIndex < repoNames.length;repoIndex++)
		{
			String thisRepo = repoNames[repoIndex] + "/master";
			commands.add(thisRepo);
			runProcess(dir, commands, true);
			commands.remove(thisRepo);
		}
	}
	

	public void pushRemote(String dir, String repoName)
	{
		pushRemote(dir, repoName, "master");
	}
	
	public void pushRemote(String dir, String repoName, String branchName)
	{
		
		// make the directory if not already there		
		List <String> commands = new Vector<String>();
		commands.add("git");
		commands.add("push");
		commands.add(repoName);
		commands.add(branchName);

		runProcess(dir, commands, true);
	}
	
	public void pushRemote(String dir, String repoName, String remoteUserName, String branchName, String username, String password) {
		List <String> commands = new Vector<String>();
		commands.add("git");
		commands.add("push");
		commands.add("--set-upstream");
		commands.add("https://" + username + ":" + password + "@github.com/" + remoteUserName + "/" + repoName);
		commands.add(branchName);

		runProcess(dir, commands, true);
	}
	
	public void addAll(String dir) {
		addAll(dir, false);
	}
	
	public void addAll(String dir, boolean reset)
	{
		List <String> commands = new Vector<String>();
		commands.add("git");
		commands.add("add");
		commands.add(".");
		runProcess(dir, commands, true);

		if(reset) {
			commands.clear();
			commands.add("git");
			commands.add("reset");
			commands.add("--");
			commands.add("*.db");
			runProcess(dir, commands, true);
		}
	}
	
	public void addSpecific(String dir, String [] fileNames)
	{
		
		List <String> fileList = new Vector<String>();
		fileList.add("git");
		fileList.add("add");
		
		for(int fileIndex = 0;fileIndex < fileNames.length;fileIndex++)
			fileList.add(fileNames[fileIndex]);

		runProcess(dir, fileList, true);
		
	}
	
	public void commitAll(String dir, boolean add)
	{
		commitAll(dir, add, false);
	}
	
	public void commitAll(String dir, boolean add, boolean reset)
	{
		if(add)
			addAll(dir, reset);
		
		// make the directory if not already there		
		List <String> commands = new Vector<String>();
		commands.add("git");
		commands.add("commit");
		commands.add("-m");
		commands.add("\"" + getDateMessage("Committed ") + "\"");

		runProcess(dir, commands, true);
	}



	public void runProcess(String dir, List <String> commands)
	{
		runProcess(dir, commands, false);
	}
	
	public void runProcess(String dir, List <String> commands, boolean wait)
	{
		wait = true;
		ProcessBuilder pb = new ProcessBuilder();
		pb.directory(new File(dir));
		pb.command(commands);
		try {
			Process p = pb.start();
			if(wait)
			{
				p.waitFor();
/*				while(p.isAlive())
					Thread.sleep(1000);
*/			}
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return;
	}
	
	private void moveSMSSToDB(String baseFolder, String appName)
	{
		
		String fileName = baseFolder + "/db/" + appName;
		
		File dir = new File(fileName);
		String targetDir = baseFolder + "/db";
		
		// I need to change the file to the app name
		
		FileFilter fileFilter = new WildcardFileFilter("*.smss");
		 File[] files = dir.listFiles(fileFilter);
		 for (int i = 0; i < files.length; i++) {
		   try {
			   // need to make modification on the engine
			   File file = changeEngine(files[i], appName);
			   FileUtils.copyFileToDirectory(file, new File(targetDir));
			
			// in reality there may be other things we need to do
			//files[i].renameTo(new File(targetDir + "/" + appName + ".smss"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 }
	}
	
	public static boolean isGit(String localApp)
	{
		File file = new File(localApp + "/" + ".git");
		return file.exists();
	}
	
	private File changeEngine(File file, String appName)
	{
		String mainDirectory = file.getParent();
		String fileName = file.getName();
		File newFile = null;
		
		try {
			OutputStream fos = null;
			
			if((fileName + ".smss").equalsIgnoreCase(appName + ".smss"))
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

	private void moveDBToSMSS(String baseFolder, String appName, String smssName)
	{
		
		String fileName = baseFolder + "/db/" + smssName;

		File srcFile = new File(fileName);
		if(srcFile.exists())
		{
			File dir = new File(fileName);
			String targetDir = baseFolder + "/db/" + appName;
			   try {
				FileUtils.copyFileToDirectory(new File(fileName), new File(targetDir));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	// usecase 1 you are starting from a remote repository app
	// your name for app - will be the db folder
	// appname will be the name of the remote
	public void makeAppFromRemote(String baseFolder, String yourName4App, String appName)
	{
		// get to the base folder
		// create directory one with the your name for app 
		// git init
		// add a remote repo with the app name and remote repo
		// pull the app
		// since this is the first time nothing to diff
		String appInstanceName = Utility.getInstanceName(appName);
		String dbName = baseFolder + "/db/" + yourName4App;
		try {
			if(!checkLocalRepository(dbName))
			{
				makeLocalRepository(dbName);
				// need to have the master
				semossInit(dbName);
				commitAll(dbName, true);
				
			}
			else
			{
				// need to throw exception to say the app name exists, need to give a different app name
				
				// need to see if I am adding a new remote or an old remote
				// if it is a new remote
				// I should figuree out a way to put the ignore files
				
			}
			List<Map<String, String>> remotes = listConfigRemotes(dbName);
			// need to loop through and see if remote exists
			boolean existing = false;
			REMOTE_LOOP : for(Map<String, String> remoteMap : remotes) {
				String existingRemoteAppName = remoteMap.get("name");
				// need to compare combination of name space + app name
				if(existingRemoteAppName.equals(appName)) {
					existing = true;
					break REMOTE_LOOP ;
				}
			}
			if(!existing)
			{
				removeAllIgnore(dbName);
				addRemote(dbName, appName, false);
			}
			// else leave the ignore
			else
			{
				// add it
				String [] filesToIgnore = new String[] {"*.db", "*.jnl"};
				checkoutIgnore(dbName, filesToIgnore);
			}
			// I dont need to do that since I will manually move it 
			// so no ignore files
			/*
			String [] filesToIgnore = {"*.smss"};
			
			addFilesToIgnore(dbName, filesToIgnore);
			
			// get everything
			fetchRemote(dbName, appName);
			
			// now that if we have everything remove ignore and pull again
			removeAllIgnore(dbName);
			*/
			
			// get everything
			fetchRemote(dbName, appInstanceName);

			// merge
			merge(dbName, "master", appInstanceName + "/master");

			// move the smss to the db folder
			moveSMSSToDB(baseFolder, yourName4App );
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (GitAPIException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	// usecase 2 - you are starting locally and then establishing a remote repository for the same
	// this is same as connect DB
	// remote says if I should check the remote to see if I can parallely make one
	public void makeRemoteFromApp(String baseFolder, String appName, String remoteAppName, boolean remote, String userName, String password)
	{
		// get to the base folder
		// create directory one with the your name for app 
		// git init
		// create the remote repository
		String dbName = baseFolder + "/db/" + appName;
		
		String remoteUserName = remoteAppName.split("/")[0];
		remoteAppName = remoteAppName.split("/")[1];
		
		try {
			initDir(dbName);
		} catch (Exception e1) 
		{
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		moveDBToSMSS(baseFolder, appName, appName + ".smss" );
	
		commitAll(dbName, true);
		
		try {
			if(checkRemoteRepository(remoteAppName, userName, password))
			{
				if(!remote)
				{
					//throw an exception
					return;
				}
			}
			else
			{
				removeAllIgnore(dbName); // removes all the ignores so it can go in
				makeRemoteRepository(remoteAppName, userName, password);
			}
			// this will assume that everything is fine
			// as in this is being done for the first time and synchronize to the remote

			
			String appRepo = "https://github.com/" + userName + "/" + remoteAppName;
			// now add the remote
			addRemote(dbName, appRepo, true);

			/*
			String [] filesToIgnore = {"*.smss"};
			
			addFilesToIgnore(dbName, filesToIgnore);
			
			// get everything
			fetchRemote(dbName, appName);
			
			// now that if we have everything remove ignore and pull again
			removeAllIgnore(dbName);

			*/
			// get everything
			fetchRemote(dbName, remoteAppName);
			// check to see if there are conflicts
			
			// merge everything
			merge(dbName, "master", remoteAppName + "/master");
			
			// push it back
			pushRemote(dbName, remoteAppName, remoteUserName, "master", userName, password);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	// gives you back the new files
	// the key is ADD, MOD, REN, DEL
	public Hashtable<String, List<String>> synchronize(String localAppName, String remoteAppName, String username, String password, boolean dual)
	{
		// get everything
		String remoteUserName = remoteAppName.split("/")[0];
		remoteAppName = remoteAppName.split("/")[1]; 
		

		removeAllIgnore(remoteAppName);
		// need something that will process files for SOLR
		String [] filesToIgnore = new String[] {"*.mv.db"};
		checkoutIgnore(localAppName, filesToIgnore);

		// already happened once so I will not do this again
		commitAll(localAppName, true, true);
		
		fetchRemote(localAppName, remoteAppName);
		
		// need to get a list of files to process
		String thisMaster = "refs/heads/master";
		String remoteMaster = "refs/remotes/" + remoteAppName +"/master";
		Hashtable <String, List<String>> files = getFilesToAdd(localAppName, thisMaster, remoteMaster);
		
		
		// check to see if there are conflicts
		// it is now done as part of merge
		// merge everything
		merge(localAppName, "master", remoteAppName + "/master");
		
		// push it back
		if(dual)
		{
			checkinIgnore(localAppName, filesToIgnore);
			pushRemote(localAppName, remoteAppName, remoteUserName, "master", username, password);
			// need something that will process files for SOLR
		}	
		
		return files;
	}
	
	// check if a file is safe to save
	public boolean isWritable(String baseFolder, String localAppName, String fileName, String remoteAppName)
	{
		boolean writable = true;
		
		// need to fetch this specific file
		// and then try to see if the file has changed
		// if there is a valid change i.e. the MOD vector is > 1 then writable becomes false
		// else true
		
		// get everything
		String remoteUserName = remoteAppName.split("/")[0];
		remoteAppName = remoteAppName.split("/")[1]; 
		

		removeAllIgnore(remoteAppName);
		// need something that will process files for SOLR
		String [] filesToIgnore = new String[] {"*.mv.db"};
		checkoutIgnore(localAppName, filesToIgnore);
		
		// hmm.. seems like I cannot get just tone file
		// https://stackoverflow.com/questions/28375418/how-to-pull-a-single-file-from-a-server-repository-in-git
		
		fetchRemote(localAppName, remoteAppName);
		checkout(localAppName, remoteAppName + "/master", fileName);

		Hashtable <String, List<String>> files = getFilesToAdd(localAppName, "master", remoteAppName + "/master");

		// if there is mod.. sorry mate nothing I can do
		writable = files.get("MOD").size() > 0;

		merge(localAppName, "master", remoteAppName + "/master");
		
		
		return writable;
	}
	

	
	public static void main(String [] args) throws Exception
	{
		GitHelper helper = new GitHelper();
		String userName = "prabhuk12";
		String password = "g2thub123";
		
		String dir = "C:\\Users\\pkapaleeswaran\\workspacej3\\git\\NTrial3";
	
		//helper.removeAllIgnore(dir);
		
		helper.checkinIgnore(dir, new String[]{"*.db"});
		helper.checkoutIgnore(dir, new String[]{"*.db"});
		
		//helper.listRemotes(userName, password);
		
		String baseFolder = "C:\\Users\\pkapaleeswaran\\workspacej3\\SemossWeb";
		
		String appName = "https://github.com/prabhuk12/Mv2";
		
		helper.removeRemote(baseFolder + "/db/Mv2Git4", "Mv4");
		//helper.makeRemoteFromApp(baseFolder, "Mv2", "Mv2", true, userName, password);
		//helper.makeAppFromRemote(baseFolder, "MvGit", appName);
		helper.synchronize(baseFolder + "/db/Mv2Git4", "Mv4", userName, password, true);
		
		helper.getVersions(dir);
		//helper.mergeRepo(dir, "Trial2");
		helper.listConfigRemotes(dir);

		helper.initDir(dir);
		
		// I dont need this for now
		helper.semossInit(dir);
		helper.commitAll(dir, true);
		
		//helper.checkout(dir, "master");
		String remoteRepo = "https://github.com/prabhuk12/NTrial1";
		helper.addRemote(dir, remoteRepo, true);
		
		// first time
		// command line
		//helper.fetchRemote(dir,"Trial1", userName, password);
		helper.fetchRemote(dir,"NTrial1");
		
		helper.merge(dir, "master", "NTrial1/master");
		
		// command line
		//helper.mergeRepo(dir, "NTrial1");
		
		// commit this to master
		helper.commitAll(dir, true);

		remoteRepo = "https://github.com/prabhuk12/NTrial3";
		helper.addRemote(dir, remoteRepo, true);

		// second time
		//helper.fetchRemote(dir,"Trial2", userName, password);
		helper.fetchRemote(dir,"NTrial3");

		// Jgit version
		helper.merge(dir, "master", "NTrial3/master");

		// command line
		//helper.mergeRepo(dir, "NTrial3");
		
		// commit everything
		helper.commitAll(dir, true);
		
		// push it back to 
		// command line
		helper.pushRemote(dir, "NTrial3");

		// J Git
		//helper.push(dir, "NTrial3", "master");
		
		helper.getFilesToAdd(dir, "refs/remotes/Trial1/master", "refs/remotes/Trial2/master");
		
		helper.checkout(dir, "Trial1/master");
		
		// merge doesn't seem to do jack shit
		// it cant if there is nothing in there
		// so when it is empty 
		// it is kind of easy to merge
		// once the master comes on it it tricky ?
		helper.mergeRepo(dir, "Trial2");

		helper.checkout(dir, "Trial1/master");

		String repoName = "Trial1";
		String remoteRepoName = "Trial1";
		
		String localFile = "C:\\Users\\pkapaleeswaran\\workspacej3\\semossdev\\Trial2\\" + repoName;

		
		// usecase 1 - pull a remote repository into a local repository
		// Usecase 2 - start with local and then commit to the remote
		// usecase 3 - pull from a remote, add changes and then commit back to a different remote
		
		// try if there are updates from remote
		helper.isCurrent(localFile);
		remoteRepoName = "https://github.com/prabhuk12/Trial1";
		
		helper.fetchRemote(localFile, remoteRepoName, userName, password);
		helper.isCurrent(localFile);

		/*
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
