package prerna.reactor.project;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.tcp.client.SocketClient;
import prerna.util.Constants;
import prerna.util.Utility;

public class ProjectReconnectServerReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(ProjectReconnectServerReactor.class);
	
	public ProjectReconnectServerReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.PROJECT.getKey(), "force", "port"};
	}
	
	// reconnects the server
	// execute method - GREEDY translation
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);
		if(projectId == null || (projectId=projectId.trim()).isEmpty()) {
			projectId = this.insight.getContextProjectId();
		}
		if(projectId == null || (projectId=projectId.trim()).isEmpty()) {
			projectId = this.insight.getProjectId();
		}
		
		// make sure valid id for user
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		if(!SecurityProjectUtils.userCanEditProject(this.insight.getUser(), projectId)) {
			// you don't have access
			throw new IllegalArgumentException("Project does not exist or user does not have access to the project");
		}
		
		boolean force = Boolean.parseBoolean(keyValue.get(keysToGet[1])+"");
		int forcePort = -1;
		if(keyValue.containsKey(keysToGet[2])) {
			forcePort = Integer.parseInt(keyValue.get(keysToGet[2]));
		}
		
		IProject project = Utility.getProject(projectId);
		project.getProjectTcpClient();
		
		{
			// are we already connected?
			SocketClient client = project.getProjectTcpClient(false);
			if(client != null && client.isConnected()) {
				if(force) {
					client.stopPyServe(project.getProjectTcpServerDirectory());
					client.disconnect();
					project.setProjectTcpClient(null);
				} else {
					return NounMetadata.getErrorNounMessage("Project '" + projectId + "' TCP Server is already available");
				}
			} else if(client != null && !client.isConnected()) {
				try {
					client.stopPyServe(project.getProjectTcpServerDirectory());
				} catch(Exception e) {
					// just try to close if possible
					classLogger.error(Constants.STACKTRACE, e);
				}
				project.setProjectTcpClient(null);
			}
		}
		
		SocketClient client = project.getProjectTcpClient(true, forcePort);
		if(client.isConnected()) {
			return new NounMetadata("Project '" + projectId + "' TCP Server available and connected", PixelDataType.CONST_STRING);
		}
		
		return new NounMetadata("Unable to connect to project '" + projectId + "' TCP Server", PixelDataType.CONST_STRING);
	}
	
}