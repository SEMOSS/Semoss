package prerna.auth.utils.reactors.admin;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

import java.util.function.Function;

public class AdminGetProjectAvailableReactorsReactor extends AbstractReactor {
	
	public AdminGetProjectAvailableReactorsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey()};
	}
	private Function<User, SecurityAdminUtils> userSecurityAdminUtilsFunction = SecurityAdminUtils::getInstance;
	
	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		SecurityAdminUtils adminUtils = userSecurityAdminUtilsFunction.apply(user);
		if(adminUtils == null) {
			throw new IllegalArgumentException("User must be an admin to perform this function");
		}
		
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);
		if(projectId == null || projectId.isEmpty()) {
			throw new IllegalArgumentException("Must input an project id");
		}
		
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		IProject project = Utility.getProject(projectId);
		return new NounMetadata(project.getAvailableReactors(), PixelDataType.CONST_STRING);
	}

	/**
	 * Used to change behavior in unit testing without changing source code functionality
	 * @param userSecurityAdminUtilsFunction function to set
	 */
	public void setUserSecurityAdminUtilsFunction(Function<User, SecurityAdminUtils> userSecurityAdminUtilsFunction) {
		this.userSecurityAdminUtilsFunction = userSecurityAdminUtilsFunction;
	}

}
