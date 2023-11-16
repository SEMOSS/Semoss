package prerna.reactor.insights;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.tcp.PayloadStruct;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Settings;
import prerna.util.Utility;

public class ReloadInsightClassesReactor extends AbstractReactor {

	public ReloadInsightClassesReactor() {
		this.keysToGet = new String[]{ ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.RELEASE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		this.insight.resetClassCache();
		
		String projectId = this.keyValue.get(this.keysToGet[0]);
		Boolean release = Boolean.parseBoolean(this.keyValue.get(this.keysToGet[1])+"");

		List<String> messages = new ArrayList<>();
		
		if(projectId != null && !projectId.isEmpty()) {
			// make sure valid id for user
			if(!SecurityProjectUtils.userCanViewProject(this.insight.getUser(), projectId)) {
				// you dont have access
				throw new IllegalArgumentException("Project does not exist or user does not have access to the project");
			}
			
			IProject project = Utility.getProject(projectId);
			try {
				clearProjectAssets(project, release);
				messages.add("Compiled reactors for project '" + project.getProjectId() + "'.");
			} catch(IllegalArgumentException e) {
				messages.add(e.getMessage());
			}
		} else {
			// clear the context project
			if(insight.getContextProjectId() != null) {
				IProject project = Utility.getProject(insight.getContextProjectId());
				try {
					clearProjectAssets(project, release);
					messages.add("Compiled reactors for project '" + project.getProjectId() + "'.");
				} catch(IllegalArgumentException e) {
					messages.add(e.getMessage());
				}
			}
			// clear the insight saved reactor
			if(insight.getProjectId() != null) {
				IProject project = Utility.getProject(insight.getProjectId());
				try {
					clearProjectAssets(project, release);
					messages.add("Compiled reactors for project '" + project.getProjectId() + "'.");
				} catch(IllegalArgumentException e) {
					messages.add(e.getMessage());
				}
			}
		}
		
		return new NounMetadata(String.join(" ", messages), PixelDataType.CONST_STRING);
	}
	
	/**
	 * 
	 * @param project
	 */
	private void clearProjectAssets(IProject project, boolean release) {
		project.clearClassCache();
		project.compileReactors(null);
		if(release) {
			User user = this.insight.getUser();
			String projectId = project.getProjectId();
			String projectName = project.getProjectName();
			if(!SecurityProjectUtils.userIsOwner(user, projectId)) {
				throw new IllegalArgumentException("Project '" + project.getProjectId() + "' does not exist or user is not an owner of the project.");
			}
			
			// push the compiled code
			String projectVersionFolder = AssetUtility.getProjectVersionFolder(projectName, projectId);
			ClusterUtil.pushProjectFolder(project, projectVersionFolder, Constants.ASSETS_FOLDER + "/" + "java");
			
			// might need to also push the classes folder
			String projectAssetFolder = AssetUtility.getProjectVersionFolder(projectName, projectId) + "/" + Constants.ASSETS_FOLDER;
			File compiledClasses = new File(projectAssetFolder + DIR_SEPARATOR + "classes");
			if(compiledClasses.exists() && compiledClasses.isDirectory()) {
				ClusterUtil.pushProjectFolder(project, projectVersionFolder, Constants.ASSETS_FOLDER + "/" + "classes");
			}
			
			SecurityProjectUtils.setReactorCompilation(user, projectId);
		}
		
		// if we are doing reactors on socket side
		boolean executeOnSocket = false;
		if(DIHelper.getInstance().getProperty(Settings.CUSTOM_REACTOR_EXECUTION) != null) {
			executeOnSocket = Boolean.parseBoolean(DIHelper.getInstance().getProperty(Settings.CUSTOM_REACTOR_EXECUTION)+"");
		}
		
		if(executeOnSocket && this.insight.getUser() != null && this.insight.getUser().getSocketClient(false) != null) {
			PayloadStruct ps = new PayloadStruct();
			ps.operation = PayloadStruct.OPERATION.PROJECT;
			ps.projectId = insight.getContextProjectId();
			ps.methodName = "clearClassCache";
			ps.hasReturn = false;
			
			this.insight.getUser().getSocketClient(false).executeCommand(ps);
		}
	}
	
	
}
