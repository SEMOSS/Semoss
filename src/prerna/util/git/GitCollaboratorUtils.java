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
import org.kohsuke.github.HttpException;
import org.kohsuke.github.PagedIterator;

import prerna.security.InstallCertNow;
import prerna.util.Constants;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class GitCollaboratorUtils {
	protected static final Logger classLogger = LogManager.getLogger(GitCollaboratorUtils.class);

	/**
	 * This class is not intended to be extended or used outside of its static method
	 */
	private GitCollaboratorUtils() {

	}
	
	public static void addCollaborator(String remoteRepositoryName, String username, String password, String collaborator) 
	{
		addCollaborator(remoteRepositoryName, username, password, collaborator, 1);
	}

	/**
	 * Add a new user to a repository
	 * @param remoteRepositoryName
	 * @param username
	 * @param password
	 * @param collaborator
	 */
	public static void addCollaborator(String remoteRepositoryName, String username, String password, String collaborator, int attempt) {
		GitHub gh = GitUtils.login(username, password);
		GHRepository ghr;
		try {
			ghr = gh.getRepository(remoteRepositoryName);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Could not find repository at " + remoteRepositoryName);
		}

		GHUser newUser = null;
		if(attempt < 3)
		{

			try {
				newUser = gh.getUser(collaborator);
			}catch(HttpException ex)
			{
				classLogger.error(Constants.STACKTRACE, ex);
				try {
					InstallCertNow.please("github.com", null, null);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					classLogger.error(Constants.STACKTRACE, e);
				}
				attempt = attempt + 1;
				addCollaborator(remoteRepositoryName, username, password, collaborator, attempt);
			}  catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Could not find user with name " + collaborator);
			}
		}
		
		if(attempt <3)
		{
			
			try {
				ghr.addCollaborators(newUser);
			}catch(HttpException ex)
			{
				classLogger.error(Constants.STACKTRACE, ex);
				try {
					InstallCertNow.please("github.com", null, null);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					classLogger.error(Constants.STACKTRACE, e);
				}
				attempt = attempt + 1;
				addCollaborator(remoteRepositoryName, username, password, collaborator, attempt);
			}  catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Error adding user to repository");
			}
		}

	}

	public static void removeCollaborator(String remoteRepositoryName, String username, String password, String collaborator)
	{
		int attempt = 1;
		removeCollaborator(remoteRepositoryName, username, password, collaborator, attempt);
	}
	
	public static void removeCollaborator(String remoteRepositoryName, String username, String password, String collaborator, int attempt)
	{
		if(attempt < 3)
		{
			GitHub gh = GitUtils.login(username, password);
			GHRepository ghr = null;
			try {
				ghr = gh.getRepository(remoteRepositoryName);
			} catch(HttpException ex)
			{
				classLogger.error(Constants.STACKTRACE, ex);
				try {
					InstallCertNow.please("github.com", null, null);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					classLogger.error(Constants.STACKTRACE, e);
				}
				attempt = attempt + 1;
				removeCollaborator(remoteRepositoryName, username, password, collaborator, attempt);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Could not find repository at " + remoteRepositoryName);
			}
	
			GHUser existingUser = null;
			try {
				existingUser = gh.getUser(collaborator);
			}catch(HttpException ex)
			{
				classLogger.error(Constants.STACKTRACE, ex);
				try {
					InstallCertNow.please("github.com", null, null);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					classLogger.error(Constants.STACKTRACE, e);
				}
				attempt = attempt + 1;
				removeCollaborator(remoteRepositoryName, username, password, collaborator, attempt);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Could not find user with name " + collaborator);
			}
	
			try {
				ghr.removeCollaborators(existingUser);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Error removing user to repository");
			}
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
		int attempt = 1;
		return searchUsers(query, username, password, attempt);
	}

	/**
	 * Return the first 10 users that best match a given username search
	 * @param query
	 * @param username
	 * @param password
	 * @return
	 */
	public static List<Map<String, String>> searchUsers(String query, String username, String password, int attempt) {
		List<Map<String, String>> userList = new Vector<Map<String, String>>();
		GitHub gh = GitUtils.login(username, password);
		PagedIterator <GHUser> users = gh.searchUsers().q(query).list().iterator();

		if(attempt < 3)
		{
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
				}catch(HttpException ex)
				{
					classLogger.error(Constants.STACKTRACE, ex);
					try {
						InstallCertNow.please("github.com", null, null);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						classLogger.error(Constants.STACKTRACE, e);
					}
					attempt = attempt + 1;
					searchUsers(query, username, password, attempt);
				} catch (IOException e) {
					// ignore
					error = true;
				}
			}		
		if(error && userList.isEmpty()) {
			throw new IllegalArgumentException("Error occurred retrieving user information");
		}
		}
		return userList;
	}

	public static List<String> listCollaborators(String remoteRepositoryName, String username, String password) {
		
		int attempt = 1;
		return listCollaborators(remoteRepositoryName, username, password, attempt);
	
	}
	
	public static List<String> listCollaborators(String remoteRepositoryName, String username, String password, int attempt) {
		List<String> collabVector = new Vector<String>(); 
		GitHub gh = GitUtils.login(username, password);
		GHRepository ghr = null;
		
		if(attempt < 3)
		{
			
			try {
				ghr = gh.getRepository(remoteRepositoryName);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
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
				else if(e.getResponseCode() == -1)
				{
					attempt = attempt + 1;
					listCollaborators(remoteRepositoryName, username, password, attempt);				
				}
				// if other error code, throw generic error
				throw new IllegalArgumentException("Error getting list of collaborators for repository");
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Error getting list of collaborators for repository");
			}
		}

		return collabVector;
	}
	
	/*************** OAUTH Overloads Go Here ***********************/
	/***************************************************************/
	
	public static void addCollaborator(String remoteRepositoryName, String collaborator, String token) 
	{
		addCollaborator(remoteRepositoryName, collaborator, token, 1);
	}

	/**
	 * Add a new user to a repository
	 * @param remoteRepositoryName
	 * @param username
	 * @param password
	 * @param collaborator
	 */
	public static void addCollaborator(String remoteRepositoryName, String collaborator, String token, int attempt) {
		GitHub gh = GitUtils.login(token);
		GHRepository ghr;
		try {
			ghr = gh.getRepository(remoteRepositoryName);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Could not find repository at " + remoteRepositoryName);
		}

		GHUser newUser = null;
		if(attempt < 3)
		{

			try {
				newUser = gh.getUser(collaborator);
			}catch(HttpException ex)
			{
				classLogger.error(Constants.STACKTRACE, ex);
				try {
					InstallCertNow.please("github.com", null, null);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					classLogger.error(Constants.STACKTRACE, e);
				}
				attempt = attempt + 1;
				addCollaborator(remoteRepositoryName, collaborator, token, attempt);
			}  catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Could not find user with name " + collaborator);
			}
		}
		
		if(attempt <3)
		{
			
			try {
				ghr.addCollaborators(newUser);
			}catch(HttpException ex)
			{
				classLogger.error(Constants.STACKTRACE, ex);
				try {
					InstallCertNow.please("github.com", null, null);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					classLogger.error(Constants.STACKTRACE, e);
				}
				attempt = attempt + 1;
				addCollaborator(remoteRepositoryName, collaborator, token, attempt);
			}  catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Error adding user to repository");
			}
		}

	}

	public static void removeCollaborator(String remoteRepositoryName,String collaborator, String token)
	{
		int attempt = 1;
		removeCollaborator(remoteRepositoryName, collaborator, token, attempt);
	}
	
	public static void removeCollaborator(String remoteRepositoryName, String collaborator, String token, int attempt)
	{
		if(attempt < 3)
		{
			GitHub gh = GitUtils.login(token);
			GHRepository ghr = null;
			try {
				ghr = gh.getRepository(remoteRepositoryName);
			} catch(HttpException ex)
			{
				classLogger.error(Constants.STACKTRACE, ex);
				try {
					InstallCertNow.please("github.com", null, null);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					classLogger.error(Constants.STACKTRACE, e);
				}
				attempt = attempt + 1;
				removeCollaborator(remoteRepositoryName, collaborator, token, attempt);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Could not find repository at " + remoteRepositoryName);
			}
	
			GHUser existingUser = null;
			try {
				existingUser = gh.getUser(collaborator);
			}catch(HttpException ex)
			{
				classLogger.error(Constants.STACKTRACE, ex);
				try {
					InstallCertNow.please("github.com", null, null);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					classLogger.error(Constants.STACKTRACE, e);
				}
				attempt = attempt + 1;
				removeCollaborator(remoteRepositoryName, collaborator, token, attempt);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Could not find user with name " + collaborator);
			}
	
			try {
				ghr.removeCollaborators(existingUser);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Error removing user to repository");
			}
		}
	}

	public static List<String> listCollaborators(String remoteRepositoryName, String token) {
		
		int attempt = 1;
		return listCollaborators(remoteRepositoryName, token, attempt);
	
	}
	
	public static List<String> listCollaborators(String remoteRepositoryName, String token, int attempt) {
		List<String> collabVector = new Vector<String>(); 
		GitHub gh = GitUtils.login(token);
		GHRepository ghr = null;
		
		if(attempt < 3)
		{
			
			try {
				ghr = gh.getRepository(remoteRepositoryName);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				// if the repository is not found - why not just clean up the remote reference
				//GitRepoUtils.removeRemote(localRepository, repositoryName);
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
					throw new IllegalArgumentException("User " + "" + " does not have permission to get list of collaborators from repository " + remoteRepositoryName);
				}
				else if(e.getResponseCode() == -1)
				{
					attempt = attempt + 1;
					listCollaborators(remoteRepositoryName, token, attempt);				
				}
				// if other error code, throw generic error
				throw new IllegalArgumentException("Error getting list of collaborators for repository");
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Error getting list of collaborators for repository");
			}
		}

		return collabVector;
	}

	
	/**
	 * Return the first 10 users that best match a given username search
	 * @param query
	 * @param username
	 * @param password
	 * @return
	 */
	public static List<Map<String, String>> searchUsers(String query, String token) {
		int attempt = 1;
		return searchUsers(query, token, attempt);
	}

	/**
	 * Return the first 10 users that best match a given username search
	 * @param query
	 * @param username
	 * @param password
	 * @return
	 */
	public static List<Map<String, String>> searchUsers(String query, String token, int attempt) {
		List<Map<String, String>> userList = new Vector<Map<String, String>>();
		GitHub gh = GitUtils.login(token);
		PagedIterator <GHUser> users = gh.searchUsers().q(query).list().iterator();

		if(attempt < 3)
		{
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
				}catch(HttpException ex)
				{
					classLogger.error(Constants.STACKTRACE, ex);
					try {
						InstallCertNow.please("github.com", null, null);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						classLogger.error(Constants.STACKTRACE, e);
					}
					attempt = attempt + 1;
					searchUsers(query, token, attempt);
				} catch (IOException e) {
					// ignore
					error = true;
				}
			}		
		if(error && userList.isEmpty()) {
			throw new IllegalArgumentException("Error occurred retrieving user information");
		}
		}
		return userList;
	}
	
	
}
