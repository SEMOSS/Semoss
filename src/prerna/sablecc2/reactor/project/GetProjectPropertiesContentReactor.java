package prerna.sablecc2.reactor.project;

import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.project.impl.ProjectProperties;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Utility;

public class GetProjectPropertiesContentReactor extends AbstractReactor {
	
	public GetProjectPropertiesContentReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);

		if(StringUtils.isBlank(projectId)) {
			throw new IllegalArgumentException("Must input an project id");
		}
		
		if(!SecurityProjectUtils.userIsOwner(this.insight.getUser(), projectId)) {
			throw new IllegalArgumentException("Project does not exist or user is not an owner of the project");
		}
				
		IProject project = Utility.getProject(projectId);
		ProjectProperties props = project.getProjectProperties();
		String content = null;
		try {
			content = FileUtils.readFileToString(props.getSocialProp());
		} catch (IOException e) {
			throw new IllegalArgumentException("Unable to read project properties. Detailed error = " + e.getMessage());
		}
		NounMetadata noun = new NounMetadata(content, PixelDataType.CONST_STRING);
		return noun;
	}
	
}
