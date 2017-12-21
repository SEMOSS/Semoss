package prerna.util.git;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.kohsuke.github.GHRepository;
import org.kohsuke.github.GHUser;
import org.kohsuke.github.GitHub;
import org.kohsuke.github.PagedIterator;

public class GitCollaboratorUtils {

	/**
	 * This class is not intended to be extended or used outside of its static method
	 */
	private GitCollaboratorUtils() {

	}

	/**
	 * Add a new user to a repository
	 * @param remoteRepositoryName
	 * @param username
	 * @param password
	 * @param collaborator
	 */
	public static void addCollaborator(String remoteRepositoryName, String username, String password, String collaborator) {
		GitHub gh = GitUtils.login(username, password);
		GHRepository ghr;
		try {
			ghr = gh.getRepository(remoteRepositoryName);
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Could not find repository at " + remoteRepositoryName);
		}

		GHUser newUser = null;
		try {
			newUser = gh.getUser(collaborator);
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Could not find user with name " + collaborator);
		}

		try {
			ghr.addCollaborators(newUser);
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Error adding user to repository");
		}
	}

	public static void removeCollaborator(String remoteRepositoryName, String username, String password, String collaborator)
	{
		GitHub gh = GitUtils.login(username, password);
		GHRepository ghr;
		try {
			ghr = gh.getRepository(remoteRepositoryName);
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Could not find repository at " + remoteRepositoryName);
		}

		GHUser existingUser = null;
		try {
			existingUser = gh.getUser(collaborator);
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Could not find user with name " + collaborator);
		}

		try {
			ghr.removeCollaborators(existingUser);
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Error removing user to repository");
		}
	}
	/**
	 * Return the first 10 users that best match a given username search
	 * @param query
	 * @param username
	 * @param password
	 * @return
	 */
	public static List<Map<String, String>> searchUsers(String query, String username, String password) {
		List<Map<String, String>> userList = new Vector<Map<String, String>>();
		GitHub gh = GitUtils.login(username, password);
		PagedIterator <GHUser> users = gh.searchUsers().q(query).list().iterator();

		boolean error = false;
		for(int userIndex = 0; users.hasNext() && userIndex < 10; userIndex++) {
			try {
				GHUser user = users.next();
				Map<String, String> userMap = new Hashtable<String, String>();
				String id = user.getLogin() + "";
				String name = user.getName() + "";
				String follows = user.getFollowersCount() + "";
				String repos = user.getRepositories().size() + "";
				userMap.put("id", id);
				userMap.put("name", name);
				userMap.put("followers", follows);
				userMap.put("repos", repos);
				userList.add(userMap);
			} catch (IOException e) {
				// ignore
				error = true;
			}
		}
		
		if(error && userList.isEmpty()) {
			throw new IllegalArgumentException("Error occured retrieving user information");
		}
		return userList;
	}

	public static List<String> listCollaborators(String remoteRepositoryName, String username, String password) {
		List<String> collabVector = new Vector<String>(); 
		GitHub gh = GitUtils.login(username, password);
		GHRepository ghr;
		try {
			ghr = gh.getRepository(remoteRepositoryName);
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Could not find repository at " + remoteRepositoryName);
		}

		try {
			Iterator<String>  collabNames = ghr.getCollaboratorNames().iterator();
			while(collabNames.hasNext()) {
				collabVector.add(collabNames.next());
			}
		} catch(org.kohsuke.github.HttpException e) {
			// 403 is forbidden access erro code
			if(e.getResponseCode() == 403) {
				throw new IllegalArgumentException("User " + username + " does not have permission to get list of collaborators from repository " + remoteRepositoryName);
			}
			// if other error code, throw generic error
			throw new IllegalArgumentException("Error getting list of collaborators for repository");
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Error getting list of collaborators for repository");
		}


		return collabVector;
	}

}
