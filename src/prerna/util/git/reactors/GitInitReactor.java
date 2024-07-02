package prerna.util.git.reactors;

import java.io.File;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.git.GitRepoUtils;

public class GitInitReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GitInitReactor.class);

	// pulls the latest for this project / asset
	// the asset is basically the folder where it sits
	// this can be used enroute in a pipeline
	
	public GitInitReactor() {
	}

	@Override
	public NounMetadata execute() {

		String assetFolder = this.insight.getInsightFolder();
		assetFolder = assetFolder.replaceAll("\\\\", "/");

		try {
			Git.init().setDirectory(new File(assetFolder)).call();
			Git.open(new File(assetFolder)).close();
			
			GitRepoUtils.addAllFiles(assetFolder, true);
			GitRepoUtils.commitAddedFiles(assetFolder);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		} catch (GitAPIException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		}
		
		return new NounMetadata("Success !", PixelDataType.VECTOR, PixelOperationType.OPERATION);
	}

}
