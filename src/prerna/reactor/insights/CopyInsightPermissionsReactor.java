package prerna.reactor.insights;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityInsightUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class CopyInsightPermissionsReactor extends AbstractReactor {

	private static final Logger logger = LogManager.getLogger(CopyInsightPermissionsReactor.class);
	
	private static String SOURCE_PROJECT = "sourceProject";
	private static String SOURCE_INSIGHT = "sourceInsight";

	private static String TARGET_PROJECT = "targetProject";
	private static String TARGET_INSIGHT = "targetInsight";

	public CopyInsightPermissionsReactor() {
		this.keysToGet = new String[]{ SOURCE_PROJECT, SOURCE_INSIGHT, TARGET_PROJECT, TARGET_INSIGHT };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String sourceProjectId = this.keyValue.get(this.keysToGet[0]);
		String sourceInsightId = this.keyValue.get(this.keysToGet[1]);
		String targetProjectId = this.keyValue.get(this.keysToGet[2]);
		String targetInsightId = this.keyValue.get(this.keysToGet[3]);

		// must be an editor for both to run this
		if(!SecurityProjectUtils.userCanEditProject(this.insight.getUser(), sourceProjectId)) {
			throw new IllegalArgumentException("You do not have edit access to the source project");
		}
		if(!SecurityProjectUtils.userCanEditProject(this.insight.getUser(), targetProjectId)) {
			throw new IllegalArgumentException("You do not have edit access to the target project");
		}
		
		String sourceInsightName = SecurityQueryUtils.getInsightNameForId(sourceProjectId, sourceInsightId);
		String targetInsightName = SecurityQueryUtils.getInsightNameForId(targetProjectId, targetInsightId);
		
		// if the insight name is null, then it doesn't exist
		if(sourceInsightName == null) {
			throw new IllegalArgumentException("Could not find the insight defined in the source app");
		}
		if(targetInsightName == null) {
			throw new IllegalArgumentException("Could not find the insight defined in the target app");
		}
		
		// now perform the operation
		try {
			SecurityInsightUtils.copyInsightPermissions(sourceProjectId, sourceInsightId, targetProjectId, targetInsightId);
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred copying the insight permissions.  Detailed error: " + e.getMessage());
		}

		String sourceProject = SecurityProjectUtils.getProjectAliasForId(sourceProjectId);
		String targetProject = SecurityProjectUtils.getProjectAliasForId(targetProjectId);
		
		return new NounMetadata("Copied permissions from project " + sourceProject + "__" + sourceProjectId 
				+ " insight \"" + sourceInsightName + "\" to " + targetProject + "__" + targetProjectId 
				+ " insight \"" + targetInsightName + "\"", PixelDataType.CONST_STRING);
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(SOURCE_PROJECT)) {
			return "The project id that is used to provide information";
		} else if(key.equals(TARGET_PROJECT)) {
			return "The project id that the operation is applied on";
		} else if(key.equals(SOURCE_INSIGHT)) {
			return "The insight id in the source project to provide information";
		} else if(key.equals(TARGET_INSIGHT)) {
			return "The insight id in the target project that the operation is applied on";
		}
		return ReactorKeysEnum.getDescriptionFromKey(key);
	}
}
