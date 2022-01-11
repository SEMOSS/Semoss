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

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityUserInsightUtils;
import prerna.auth.utils.SecurityUserProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.impl.InsightAdministrator;
import prerna.om.MosfetFile;
import prerna.project.api.IProject;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.AssetUtility;
import prerna.util.MosfetSyncHelper;
import prerna.util.Utility;
import prerna.util.git.GitDestroyer;
import prerna.util.git.GitRepoUtils;
import prerna.util.git.GitUtils;

public class DeleteInsightReactor extends AbstractReactor {

	public DeleteInsightReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.ID.getKey()};
	}

	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		String author = null;
		String email = null;
		if(AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}
		
		organizeKeys();
		GenRowStruct projectGrs = this.store.getNoun(this.keysToGet[0]);
		if(projectGrs.isEmpty()) {
			throw new IllegalArgumentException("Must define the project to delete the insights from");
		}
		String projectId = projectGrs.get(0).toString();
		if(AbstractSecurityUtils.securityEnabled()) {
			projectId = SecurityUserProjectUtils.testUserProjectIdForAlias(user, projectId);
			if(!SecurityUserProjectUtils.userCanViewProject(user, projectId)) {
				throw new IllegalArgumentException("Project " + projectId + " does not exist or user does not have access to the project");
			}
			// Get the user's email
			AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
			email = accessToken.getEmail();
			author = accessToken.getUsername();
		} 
//		else {
//			projectId = MasterDatabaseUtility.testProjectIdIfAlias(projectId);
//			if(!MasterDatabaseUtility.getAllProjectIds().contains(projectId)) {
//				throw new IllegalArgumentException("Project " + projectId + " does not exist");
//			}
//		}
		IProject project = Utility.getProject(projectId);
		String projectName = project.getProjectName();
		
		InsightAdministrator admin = new InsightAdministrator(project.getInsightDatabase());
		ClusterUtil.reactorPullInsightsDB(projectId);
		ClusterUtil.reactorPullProjectFolder(project, AssetUtility.getProjectAssetVersionFolder(projectName, projectId));

		GenRowStruct grs = this.store.getNoun(this.keysToGet[1]);
		int size = grs.size();
		for (int i = 0; i < size; i++) {
			String insightId = grs.get(i).toString();
			if(AbstractSecurityUtils.securityEnabled()) {
				if(!SecurityUserInsightUtils.userCanEditInsight(user, projectId, insightId)) {
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
			String insightFolderPath = AssetUtility.getProjectAssetVersionFolder(projectName, projectId)
					+ DIR_SEPARATOR + insightId;
			File insightFolder = new File(insightFolderPath);
			Stream<Path> walk = null;
			try {
				// delete insight files from git
				File mosfitF = new File(insightFolderPath + DIR_SEPARATOR + MosfetFile.RECIPE_FILE);
				if(mosfitF.exists() && mosfitF.isFile()) {
					String insightName = MosfetSyncHelper.getInsightName(mosfitF);
					String gitFolder = AssetUtility.getProjectAssetVersionFolder(projectName, projectId);
					// grab relative file paths
					walk = Files.walk(Paths.get(insightFolder.toURI()));
					List<String> files = walk
							.map(x -> insightId + DIR_SEPARATOR
									+ insightFolder.toURI().relativize(new File(x.toString()).toURI()).getPath().toString())
							.collect(Collectors.toList());
					files.remove(""); // removing empty path
					GitDestroyer.removeSpecificFiles(gitFolder, true, files);
					GitRepoUtils.commitAddedFiles(gitFolder,GitUtils.getDateMessage("Deleted " + insightName + " insight on"), author, email);
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				if(walk != null) {
					walk.close();
				}
				// delete folder
				try {
					FileUtils.deleteDirectory(insightFolder);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
			// now delete from security db
			SecurityUserInsightUtils.deleteInsight(projectId, insightId);
		}
		
		ClusterUtil.reactorPushInsightDB(projectId);
		ClusterUtil.reactorPushProjectFolder(project, AssetUtility.getProjectAssetVersionFolder(projectName, projectId));
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.DELETE_INSIGHT);
	}

}
