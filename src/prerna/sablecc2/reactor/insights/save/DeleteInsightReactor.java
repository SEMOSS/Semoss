package prerna.sablecc2.reactor.insights.save;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAppUtils;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.engine.impl.InsightAdministrator;
import prerna.engine.impl.SmssUtilities;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.MosfetSyncHelper;
import prerna.util.Utility;
import prerna.util.git.GitDestroyer;
import prerna.util.git.GitRepoUtils;
import prerna.util.git.GitUtils;

public class DeleteInsightReactor extends AbstractReactor {

	public DeleteInsightReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.ID.getKey()};
	}

	@Override
	public NounMetadata execute() {
		if(AbstractSecurityUtils.anonymousUsersEnabled() && this.insight.getUser().isAnonymous()) {
			throwAnonymousUserError();
		}
		
		organizeKeys();
		GenRowStruct appGrs = this.store.getNoun(this.keysToGet[0]);
		if(appGrs.isEmpty()) {
			throw new IllegalArgumentException("Must define the app to delete the insights from");
		}
		String appId = appGrs.get(0).toString();
		if(AbstractSecurityUtils.securityEnabled()) {
			appId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), appId);
			if(!SecurityAppUtils.userCanViewEngine(this.insight.getUser(), appId)) {
				throw new IllegalArgumentException("App " + appId + " does not exist or user does not have access to database");
			}
		} else {
			appId = MasterDatabaseUtility.testEngineIdIfAlias(appId);
			if(!MasterDatabaseUtility.getAllEngineIds().contains(appId)) {
				throw new IllegalArgumentException("App " + appId + " does not exist");
			}
		}
		IEngine engine = Utility.getEngine(appId);
		String appName = engine.getEngineName();
		InsightAdministrator admin = new InsightAdministrator(engine.getInsightDatabase());

		GenRowStruct grs = this.store.getNoun(this.keysToGet[1]);
		int size = grs.size();
		for (int i = 0; i < size; i++) {
			String insightId = grs.get(i).toString();
			if(AbstractSecurityUtils.securityEnabled()) {
				if(!SecurityInsightUtils.userCanEditInsight(this.insight.getUser(), appId, insightId)) {
					throw new IllegalArgumentException("User does not have permission to edit this insight");
				}
			}
			
			// delete from insights database
			try {
				admin.dropInsight(insightId);
			} catch (RuntimeException e) {
				e.printStackTrace();
			}
			
			// delete insight folder
			String insightFolderPath = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER)
					+ DIR_SEPARATOR + "db"
					+ DIR_SEPARATOR + SmssUtilities.getUniqueName(appName, appId)
					+ DIR_SEPARATOR + "version" 
					+ DIR_SEPARATOR + insightId;
			File insightFolder = new File(insightFolderPath);
			try {
				// delete insight files from git
				String insightName = MosfetSyncHelper.getInsightName(new File(insightFolderPath + DIR_SEPARATOR + MosfetSyncHelper.RECIPE_FILE));
				String gitFolder = AssetUtility.getAppAssetVersionFolder(appName, appId);
				// grab relative file paths
				Stream<Path> walk = Files.walk(Paths.get(insightFolder.toURI()));
				List<String> files = walk
						.map(x -> insightId + DIR_SEPARATOR
								+ insightFolder.toURI().relativize(new File(x.toString()).toURI()).getPath().toString())
						.collect(Collectors.toList());
				files.remove(""); // removing empty path
				GitDestroyer.removeSpecificFiles(gitFolder, true, files);
				GitRepoUtils.commitAddedFiles(gitFolder,
						GitUtils.getDateMessage("Deleted " + insightName + " insight on : "));
				FileUtils.deleteDirectory(insightFolder);
			} catch (IOException e) {
				e.printStackTrace();
			}

			SecurityInsightUtils.deleteInsight(appId, insightId);
		}
		ClusterUtil.reactorPushApp(appId);
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.DELETE_INSIGHT);
	}

}
