package prerna.util.git;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.kohsuke.github.GHRepository;
import org.kohsuke.github.GitHub;
import org.kohsuke.github.PagedIterable;
import org.kohsuke.github.PagedIterator;

// this is very specific to github
// need to see for gitlab etc. 
public class GitRepoManager {

	GitHub gh = null;
	String userName = null;
	// jsut a way to manage and clean repository
	
	public void login(String username, String password)
	{
		try {
			gh = GitHub.connectUsingPassword(username, password);
			gh.getMyself();
			this.userName = username;
			System.out.println(">> Logged in" + username);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void deleteRepository(String repoName)
	{
		try {
			gh.getRepository(repoName);
			GHRepository ghr = gh.getRepository(repoName);
			ghr.delete();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	public void listRepository(boolean delete)
	{
		try {
		
		PagedIterator <GHRepository> allrepos = gh.getMyself().listRepositories().iterator();
		
		BufferedReader br = null;
		
		if(delete)
			br = new BufferedReader(new InputStreamReader(System.in));
		
		while(allrepos.hasNext())
		{
				GHRepository repo = allrepos.next();
				printRepo(repo);
				if(delete)
				{
					System.out.println("Delete y/n ? ");
					String input = br.readLine();
					if(input.equalsIgnoreCase("y"))
						deleteRepository(userName + "/" + repo.getName());
				}
		}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void printRepo(GHRepository repo)
	{
		try {
			System.out.println("Name : " + repo.getName());
			System.out.println("Created On : " + repo.getCreatedAt());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String [] args)
	{
		GitRepoManager repoM = new GitRepoManager();
		repoM.login("prabhuk12", "g2thub123");
		repoM.listRepository(true);
	}
	
}
