package prerna.reactor.project;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class CreatedByMeAppsReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(CreatedByMeAppsReactor.class);

	@Override
	public NounMetadata execute() {
		try {
			organizeKeys();
			User user = this.insight.getUser();
			
			// get all project IDs
	        List<String> allProjectIds = SecurityProjectUtils.getAllProjectIds();
	        Set<String> createdProjectIds = new HashSet<>();
	        
			// check ownership for each project
			for (String projectId : allProjectIds) {
				if (SecurityProjectUtils.userIsOwner(user, projectId)) {
					createdProjectIds.add(projectId);
				}
			}
			
	        Map<String, Object> retMap = new HashMap<>();
	        retMap.put("createdProjects", createdProjectIds);
	 
	        return new NounMetadata(retMap, PixelDataType.MAP, PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.error("Failed to fetch project IDs created by the current user", e);
			throw new SemossPixelException(
					"An error occurred while fetching projects created by the current user. Error message: " + e.getMessage());
		}
	}
	
	@Override
	public String getReactorDescription() {
		return "Fetches the list of project IDs created by the current user.";
	}
}