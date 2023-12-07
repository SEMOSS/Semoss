package prerna.reactor.project;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.om.ClientProcessWrapper;
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
		this.keysToGet = new String[] {ReactorKeysEnum.PROJECT.getKey()};
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

		IProject project = Utility.getProject(projectId);
		// sadly, the logic right now requires we have a made cpw
		// otherwise the reconnect method does nto 
		project.getProjectTcpClient();
		ClientProcessWrapper cpw = project.getClientProcessWrapper();
		if(cpw == null || cpw.getSocketClient() == null) {
			project.getProjectTcpClient(true);
			return new NounMetadata("TCP Server was not initialized but is now started and connected for project '" + projectId + "'", PixelDataType.CONST_STRING);
		}
		cpw.shutdown(false);
		try {
			cpw.reconnect();
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			return new NounMetadata("Unable to restart TCP Server", PixelDataType.CONST_STRING);
		}
		SocketClient client = project.getProjectTcpClient(false);
		if(client == null || !client.isConnected()) {
			return new NounMetadata("Unable to connect to project '" + projectId + "' TCP Server", PixelDataType.CONST_STRING);
		}
		
		return new NounMetadata("Project '" + projectId + "' TCP Server available and connected", PixelDataType.CONST_STRING);
	}
	
}