package prerna.solr.reactor;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.Utility;

public class GetProjectSMSSReactor extends AbstractReactor {
	private static final Logger classLogger = LogManager.getLogger(GetProjectSMSSReactor.class);

	public GetProjectSMSSReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);
		if(AbstractSecurityUtils.securityEnabled()) {
			User user = this.insight.getUser();
			boolean isAdmin = SecurityAdminUtils.userIsAdmin(user);
			if(!isAdmin) {
				boolean isOwner = SecurityProjectUtils.userIsOwner(user, projectId);
				if(!isOwner) {
					throw new IllegalArgumentException("Project " + projectId + " does not exist or user does not have permissions to update the smss of the project. User must be the owner to perform this function.");
				}
			}	
		}
				
		IProject project = Utility.getProject(projectId);
		String currentSmssFileLocation = project.getProjectSmssFilePath();
		File currentSmssFile = new File(currentSmssFileLocation);
		project.closeProject();
		
		String errorMsg = null;
		if(!currentSmssFile.exists() || !currentSmssFile.isFile()) {
			errorMsg = "Could not find current project smss file";
			classLogger.error(Constants.ERROR_MESSAGE,  errorMsg);
			throw new IllegalArgumentException(errorMsg);
		}
		
		String currentSmssContent = null;
		try {
			currentSmssContent = new String(Files.readAllBytes(Paths.get(currentSmssFile.toURI())));
		} catch (IOException e) {
			errorMsg = "An error occurred reading the current project smss details. Detailed message = " + e.getMessage();
			classLogger.error(Constants.ERROR_MESSAGE, errorMsg);
			throw new IllegalArgumentException(errorMsg);
		}
		
		Map<String, String> outputMap = new HashMap<String, String>();
		String [] smssContent = currentSmssContent.split("\\n");
		for (String smssLine: smssContent) {
			if (smssLine.startsWith("#") || smssLine.startsWith("PASSWORD"))
				continue;
			
			// split each line into an array of items using the tab character as the delimiter, with a maximum of 2 substrings
			String[] keyValue = smssLine.split("\\t",2);
			outputMap.put(keyValue[0], keyValue[1]);
		}
		
		return new NounMetadata(outputMap, PixelDataType.MAP);
	}
}
