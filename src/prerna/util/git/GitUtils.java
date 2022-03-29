package prerna.util.git;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.Status;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.errors.NoWorkTreeException;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.lib.StoredConfig;
import org.kohsuke.github.GitHub;
import org.kohsuke.github.HttpException;

import prerna.security.InstallCertNow;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.MosfetSyncHelper;
import prerna.util.Utility;

public class GitUtils {
	
	private static final Logger logger = LogManager.getLogger(GitUtils.class);

	/**
	 * This class is not intended to be extended or used outside of its static method
	 */
	private GitUtils() {

	}

	/**
	 * Determine if an app is already a Git project
	 * @param localApp
	 * @return
	 */
	public static boolean isGit(String localApp) {
		File file = new File(localApp + "/version/.git");
		return file.exists();
	}

	/**
	 * Login using a username / password
	 * @param username
	 * @param password
	 * @return
	 */
	public static GitHub login(String username, String password) {
		return login(username, password, 1);
	}

	public static GitHub login(String username, String password, int attempt) {
		GitHub gh = null;
		if(attempt < 3) {
			logger.info("Attempting login " + attempt);
			try {
				gh = GitHub.connectUsingPassword(username, password);
				gh.getMyself();
				return gh;
			} catch(HttpException ex) {
				logger.error(Constants.STACKTRACE, ex);
				try {
					InstallCertNow.please("github.com", null, null);
				} catch (Exception e) {
					logger.error(Constants.STACKTRACE, e);
				}
				attempt = attempt + 1;
				login(username, password, attempt);
			} catch (IOException e) {
				logger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Invalid Git Credentials for username = \"" + username + "\"");
			}
		}
		return gh;
	}


	public static GitHub login(String oAuth) {
		return login(oAuth, 1);
	}

	public static GitHub login(String oAuth, int attempt) {
		GitHub gh = null;
		if(attempt < 3) {
			System.out.println("Attempting login " + attempt);
			try {
				gh = GitHub.connectUsingOAuth(oAuth);
				gh.getMyself();
				return gh;
			} catch(HttpException ex) {
				logger.error(Constants.STACKTRACE, ex);
				try {
					InstallCertNow.please("github.com", null, null);
				} catch (Exception e) {
					logger.error(Constants.STACKTRACE, e);
				}
				attempt = attempt + 1;
				login(oAuth, attempt);
			} catch (IOException e) {
				logger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Invalid Git Credentials for username = \"" + oAuth + "\"");
			}
		}
		return gh;
	}

	
	/**
	 * 
	 * @param prefixString
	 * @return
	 */
	public static String getDateMessage(String prefixString) {
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		return prefixString + " : " + dateFormat.format(date);
	}
	
	public static void semossInit(String dir) {
		String newFile = dir + "/SEMOSS.INIT";
		File myFile = new File(newFile);
		try {
			myFile.createNewFile();
		} catch (IOException e) {
			logger.error(Constants.STACKTRACE, e);
		}
	}

	///////////////////////////////////////////////////////
	///////////////////////////////////////////////////////
	///////////////////// GIT IGNORE //////////////////////
	///////////////////////////////////////////////////////
	///////////////////////////////////////////////////////

	/**
	 * 
	 * @param localRepository
	 */
	public static void removeAllIgnore(String localRepository) {
		Git thisGit = null;
		Repository thisRepo = null;
		try {
			thisGit = Git.open(new File(localRepository));
			thisRepo = thisGit.getRepository();
			// remove from checkout
			StoredConfig config = thisRepo.getConfig();

			config.setString("core", null, "sparseCheckout", "false");
			config.save();
			File myNewFile2 = new File(localRepository + "/.git/info/sparse-checkout");
			myNewFile2.delete();

			// remove from checkin
			File myNewFile = new File(localRepository + "/.gitignore"); //\\sparse-checkout");

			// I have to delete for now
			myNewFile.delete();
		} catch(IOException e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(thisRepo != null) {
				thisRepo.close();
			}
			if(thisGit != null) {
				thisGit.close();
			}
		}
	}
	
	/**
	 * 
	 * @param localRepository
	 * @param files
	 */
	public static void checkoutIgnore(String localRepository, String [] files)
	{
		Git thisGit = null;
		Repository thisRepo = null;
		StoredConfig config = null;
		try {
			thisGit = Git.open(new File(localRepository));
			thisRepo = thisGit.getRepository();
			config = thisRepo.getConfig();
			config.setString("core", null, "sparseCheckout", "true");
			config.save();
		} catch (IOException e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(thisRepo != null) {
				thisRepo.close();
			}
			if(thisGit != null) {
				thisGit.close();
			}
		}

		File myFile2 = new File(localRepository + "/.git/info/sparse-checkout");
		if(!myFile2.exists()) {
			File myNewFile = new File(localRepository + "/.git/info");
			if(!myNewFile.exists()) {
				myNewFile.mkdir();
			}
			FileOutputStream fos = null;
			OutputStreamWriter osw = null;
			PrintWriter pw = null;
			try {
				fos = new FileOutputStream(myFile2);
				osw = new OutputStreamWriter(fos);
				pw = new PrintWriter(osw);
				pw.println("/*");
				for(int fileIndex = 0; fileIndex < files.length; fileIndex++) {
					pw.println("!"+files[fileIndex]);
				}
				pw.close();
			} catch(IOException e) {
				logger.error(Constants.STACKTRACE, e);
			} finally {
				if(pw != null) {
					pw.close();
				}
				if(osw != null) {
					try {
						osw.close();
					} catch (IOException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
				if(fos != null) {
					try {
						fos.close();
					} catch (IOException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
	}

	/**
	 * 
	 * @param localRepository
	 * @param files
	 */
	public static void writeIgnoreFile(String localRepository, String [] files) {
		File myNewFile = new File(localRepository + "/.gitignore");
		try {
			myNewFile.createNewFile();
		} catch (IOException e) {
			logger.error(Constants.STACKTRACE, e);
		}
		FileOutputStream fos = null;
		OutputStreamWriter osw = null;
		PrintWriter pw = null;
		try {
			fos = new FileOutputStream(myNewFile);
			osw = new OutputStreamWriter(fos);
			pw = new PrintWriter(osw);
			for(int fileIndex = 0; fileIndex < files.length;fileIndex++) {
				pw.println("/" + files[fileIndex]);
			}
		} catch (FileNotFoundException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Unable to write .gitignore file");
		} finally {
			if(pw != null) {
				pw.close();
			}
			if(osw != null) {
				try {
					osw.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
			if(fos != null) {
				try {
					fos.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}
	
	/**
	 * Determine if a file is one to ignore
	 * @param fileName
	 * @return
	 */
	public static boolean isIgnore(String fileName) {
		String [] list = new String[]{".db", ".jnl"};
		boolean ignore = false;
		for(int igIndex = 0; igIndex < list.length && !ignore; igIndex++) {
			ignore = fileName.endsWith(list[igIndex]);
		}
		return ignore;
	}
	
	///////////////////////////////////////////////////////
	///////////////////////////////////////////////////////
	///////////////////// GIT STATUS //////////////////////
	///////////////////////////////////////////////////////
	///////////////////////////////////////////////////////

	
	public static List<Map<String, String>> getStatus(String projectId, String ProjectName)
	{
		List<Map<String, String>> output = new Vector<>();
		String location = AssetUtility.getProjectVersionFolder(ProjectName, projectId);; //DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/db/" + SmssUtilities.getUniqueName(ProjectName, projectId) + "/version";
		Git thisGit = null;
		Status status = null;
		try {
			thisGit = Git.open(new File(location));
			status = thisGit.status().call();
		} catch (IOException ioe) {
			logger.error(Constants.STACKTRACE, ioe);
		} catch (NoWorkTreeException nwte) {
			logger.error(Constants.STACKTRACE, nwte);
		} catch (GitAPIException e) {
			logger.error(Constants.STACKTRACE, e);
		}

		if (status != null) {
			output.addAll(getFiles(projectId, ProjectName, "ADD", status.getAdded().iterator()));
			output.addAll(getFiles(projectId, ProjectName, "MOD", status.getModified().iterator()));
			output.addAll(getFiles(projectId, ProjectName, "DEL", status.getRemoved().iterator()));
			output.addAll(getFiles(projectId, ProjectName, "DEL", status.getMissing().iterator()));
			output.addAll(getFiles(projectId, ProjectName, "CON", status.getConflicting().iterator()));
			output.addAll(getFiles(projectId, ProjectName, "NEW", status.getUntracked().iterator()));
		}

		if (thisGit != null) {
			thisGit.close();
		}

		return output;
	}
	
	
	/**
	 * Get the modified files
	 * @param dbName
	 * @param fileType
	 * @param iterator
	 * @return
	 */
	public static List<Map<String, String>> getFiles(String projectId, String projectName, String fileType, Iterator<String> iterator) {
		List<Map<String, String>> retFiles = new Vector<>();
		while(iterator.hasNext()) {
			String daFile = AssetUtility.getProjectVersionFolder(projectName, projectId) + "/" + iterator.next();
			if(!daFile.endsWith(".mosfet")) {
				continue;
			}
			File f = new File(Utility.normalizePath(daFile));
			String fileName = f.getParentFile().getName();
			// f does not exist when the file type is missing or deleted
			if(f.exists()) {
				try {
					fileName = MosfetSyncHelper.getInsightName(new File(daFile));
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
			Map<String, String> fileData = new Hashtable<>();
			fileData.put("fileName", fileName);
			fileData.put("fileLoc", daFile);
			fileData.put("fileType", fileType);
			retFiles.add(fileData);	
		}
		return retFiles;
	}
}
