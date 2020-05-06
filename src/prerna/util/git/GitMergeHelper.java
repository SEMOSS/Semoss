package prerna.util.git;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.eclipse.jgit.api.CheckoutCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.MergeCommand;
import org.eclipse.jgit.api.MergeResult;
import org.eclipse.jgit.api.ResetCommand.ResetType;
import org.eclipse.jgit.api.errors.AbortedByHookException;
import org.eclipse.jgit.api.errors.CheckoutConflictException;
import org.eclipse.jgit.api.errors.ConcurrentRefUpdateException;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.errors.NoFilepatternException;
import org.eclipse.jgit.api.errors.NoHeadException;
import org.eclipse.jgit.api.errors.NoMessageException;
import org.eclipse.jgit.api.errors.UnmergedPathsException;
import org.eclipse.jgit.api.errors.WrongRepositoryStateException;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;

import prerna.util.Utility;

public class GitMergeHelper {
	private static final Logger logger = LogManager.getLogger(GitMergeHelper.class);

	private static final String STACKTRACE = "StackTrace: ";

	private GitMergeHelper() {

	}

	/**
	 * 
	 * @param localRepository
	 * @param startPoint
	 * @param branchName
	 * @param numAttempts
	 * @param maxAttempts
	 * @param delete
	 */
	public static void merge(String localRepository, String startPoint, String branchName, int numAttempts, int maxAttempts, boolean delete)
	{
		Git thisGit = null;
		Repository thisRepo = null;
		try {
			thisGit = Git.open(new File(localRepository));
			thisRepo = thisGit.getRepository();
			Ref startRef = thisRepo.findRef(startPoint);
			
			CheckoutCommand cc = thisGit.checkout();
			cc.setName(startPoint);
			cc.setCreateBranch(false); // probably not needed, just to make sure
			if(startRef != null) {
				cc.call(); 
			}
			MergeCommand mc = thisGit.merge();
			Ref ref = thisGit.getRepository().findRef(branchName);
			if(ref != null && startRef != null) {
				mc.include(ref);
				MergeResult res = mc.call(); 
				boolean retBoolean = true;
				if (res.getMergeStatus().equals(MergeResult.MergeStatus.CONFLICTING)) {
					logger.debug(res.getConflicts().toString());
					retBoolean = false;
					Iterator <String> files = res.getConflicts().keySet().iterator();
					Vector <String> delFiles = new Vector<>();
					while(files.hasNext())
					{
						String thisFile = files.next();
						logger.debug("File is" + thisFile);
						if(!delFiles.contains(thisFile))
							delFiles.add(thisFile);
					}
					// inform the user he has to handle the conflicts
					if(numAttempts < maxAttempts && delete)
					{
						wipeFiles(delFiles);
						GitRepoUtils.addAllFiles(localRepository, false);
						GitRepoUtils.commitAddedFiles(localRepository);
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
				GitPushUtils.addAllFiles(localRepository, false);
				GitPushUtils.commitAddedFiles(localRepository);
				numAttempts++;
				// I will attempt this just one more time to merge
				merge(localRepository, startPoint, branchName, numAttempts, maxAttempts, delete);
			}
		} catch (NoFilepatternException nfpe) {
			logger.error(STACKTRACE, nfpe);
		} catch (NoHeadException nhe) {
			logger.error(STACKTRACE, nhe);
		} catch (NoMessageException nme) {
			logger.error(STACKTRACE, nme);
		} catch (UnmergedPathsException upe) {
			logger.error(STACKTRACE, upe);
		} catch (ConcurrentRefUpdateException crue) {
			logger.error(STACKTRACE, crue);
		} catch (WrongRepositoryStateException wrse) {
			logger.error(STACKTRACE, wrse);
		} catch (AbortedByHookException abhe) {
			logger.error(STACKTRACE, abhe);
		} catch (IOException ioe) {
			logger.error(STACKTRACE, ioe);
		} catch (GitAPIException gae) {
			logger.error(STACKTRACE, gae);
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
	 * Delete files that are conflicting
	 * @param filesToWipe
	 */
	private static void wipeFiles(List <String> filesToWipe) {
		for(int fileIndex = 0;fileIndex < filesToWipe.size();fileIndex++) {
			File file = new File(Utility.normalizePath(filesToWipe.get(fileIndex)));
			if(file.exists()) {
				file.delete();
			}				
		}
	}

	/**
	 * Reset the repo
	 * @param repository
	 */
	public static void abortMerge(String repository) {
		File dirFile = new File(repository);
		Git thisGit = null;
		try {
			thisGit = Git.open(dirFile);
		} catch (IOException e) {
			logger.error(STACKTRACE, e);
		}
		try {
			if (thisGit != null) {
				thisGit.reset().setMode(ResetType.HARD).setRef("HEAD").call();
			}
		} catch (GitAPIException e) {
			logger.error(STACKTRACE, e);
		} finally {
			if(thisGit != null) {
				thisGit.close();
			}
		}
	}

}
