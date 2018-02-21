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

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.Status;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.errors.NoWorkTreeException;
import org.eclipse.jgit.lib.StoredConfig;
import org.kohsuke.github.GitHub;
import org.kohsuke.github.HttpException;

import prerna.security.InstallCertNow;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.MosfetSyncHelper;

public class GitUtils {

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
		if(attempt < 3)
		{
			System.out.println("Attempting " + attempt);
			try {
				gh = GitHub.connectUsingPassword(username, password);
				gh.getMyself();
				return gh;
			}catch(HttpException ex)
			{
				ex.printStackTrace();
				try {
					InstallCertNow.please("github.com", null, null);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				attempt = attempt + 1;
				login(username, password, attempt);
			} catch (IOException e) {
				e.printStackTrace();
				throw new IllegalArgumentException("Invalid Git Credentials for username = \"" + username + "\"");
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
		return prefixString + " <d>" + dateFormat.format(date);
	}
	
	public static void semossInit(String dir) {
		String newFile = dir + "/SEMOSS.INIT";
		File myFile = new File(newFile);
		try {
			myFile.createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
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
		try {
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
		} catch(IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 
	 * @param localRepository
	 * @param files
	 */
	public static void checkoutIgnore(String localRepository, String [] files)
	{
		StoredConfig config = null;
		try {
			config = Git.open(new File(localRepository)).getRepository().getConfig();
			config.setString("core", null, "sparseCheckout", "true");
			config.save();
		} catch (IOException e) {
			e.printStackTrace();
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
				e.printStackTrace();
			} finally {
				if(pw != null) {
					pw.close();
				}
				if(osw != null) {
					try {
						osw.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				if(fos != null) {
					try {
						fos.close();
					} catch (IOException e) {
						e.printStackTrace();
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
			e.printStackTrace();
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
			e.printStackTrace();
			throw new IllegalArgumentException("Unable to write .gitignore file");
		} finally {
			if(pw != null) {
				pw.close();
			}
			if(osw != null) {
				try {
					osw.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if(fos != null) {
				try {
					fos.close();
				} catch (IOException e) {
					e.printStackTrace();
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

	
	public static List<Map<String, String>> getStatus(String dbName)
	{
		List<Map<String, String>> output = new Vector<Map<String, String>>();
		String location = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/db/" + dbName + "/version";
		Git thisGit = null;
		Status status = null;
		try {
			thisGit = Git.open(new File(location));
			status = thisGit.status().call();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (NoWorkTreeException e) {
			e.printStackTrace();
		} catch (GitAPIException e) {
			e.printStackTrace();
		}

		output.addAll(getFiles(dbName, "ADD", status.getAdded().iterator()));
		output.addAll(getFiles(dbName, "MOD", status.getModified().iterator()));
		output.addAll(getFiles(dbName, "DEL", status.getRemoved().iterator()));
		output.addAll(getFiles(dbName, "DEL", status.getMissing().iterator()));
		output.addAll(getFiles(dbName, "CON", status.getConflicting().iterator()));
		output.addAll(getFiles(dbName, "NEW", status.getUntracked().iterator()));

		thisGit.close();
		return output;
	}
	
	
	/**
	 * Get the modified files
	 * @param dbName
	 * @param fileType
	 * @param iterator
	 * @return
	 */
	public static List<Map<String, String>> getFiles(String dbName, String fileType, Iterator<String> iterator) {
		List<Map<String, String>> retFiles = new Vector<Map<String, String>>();
		while(iterator.hasNext()) {
			String daFile = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/db/" + dbName + "/version/" + iterator.next();
			if(!daFile.endsWith(".mosfet")) {
				continue;
			}
			File f = new File(daFile);
			String fileName = f.getParentFile().getName();
			// f does not exist when the file type is missing or deleted
			if(f.exists()) {
				fileName = MosfetSyncHelper.getInsightName(new File(daFile));
			}
			Map<String, String> fileData = new Hashtable<String, String>();
			fileData.put("fileName", fileName);
			fileData.put("fileLoc", daFile);
			fileData.put("fileType", fileType);
			retFiles.add(fileData);	
		}
		return retFiles;
	}
}
