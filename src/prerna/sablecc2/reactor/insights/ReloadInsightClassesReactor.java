package prerna.sablecc2.reactor.insights;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.tcp.PayloadStruct;
import prerna.util.DIHelper;
import prerna.util.Settings;
import prerna.util.Utility;

public class ReloadInsightClassesReactor extends AbstractReactor {

	public ReloadInsightClassesReactor() {
		this.keysToGet = new String[]{ ReactorKeysEnum.PROJECT.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		this.insight.resetClassCache();
		
		String projectIdToClear = this.keyValue.get(this.keysToGet[0]);
		if(projectIdToClear != null && !projectIdToClear.isEmpty()) {
			if(AbstractSecurityUtils.securityEnabled()) {
				// make sure valid id for user
				if(!SecurityProjectUtils.userCanViewProject(this.insight.getUser(), projectIdToClear)) {
					// you dont have access
					throw new IllegalArgumentException("Project does not exist or user does not have access to the project");
				}
			}
			
			IProject project = Utility.getProject(projectIdToClear);
			clearProjectAssets(project);
		} else {
			// clear the context project
			if(insight.getContextProjectId() != null) {
				IProject project = Utility.getProject(insight.getContextProjectId());
				clearProjectAssets(project);
			}
			// clear the insight saved reactor
			if(insight.getProjectId() != null) {
				IProject project = Utility.getProject(insight.getProjectId());
				clearProjectAssets(project);
			}
		}
		
		return new NounMetadata("Recompile Completed", PixelDataType.CONST_STRING);
	}
	
	/**
	 * 
	 * @param project
	 */
	private void clearProjectAssets(IProject project) {
		project.clearClassCache();
		
		boolean executeOnSocket = false;
		if(DIHelper.getInstance().getProperty(Settings.CUSTOM_REACTOR_EXECUTION) != null) {
			executeOnSocket = Boolean.parseBoolean(DIHelper.getInstance().getProperty(Settings.CUSTOM_REACTOR_EXECUTION)+"");
		}
		
		if(executeOnSocket && this.insight.getUser() != null && this.insight.getUser().getTCPServer(false) != null) {
			PayloadStruct ps = new PayloadStruct();
			ps.operation = PayloadStruct.OPERATION.PROJECT;
			ps.projectId = insight.getContextProjectId();
			ps.methodName = "clearClassCache";
			ps.hasReturn = false;
			
			this.insight.getUser().getTCPServer(false).executeCommand(ps);
		}
	}
	
	
}
