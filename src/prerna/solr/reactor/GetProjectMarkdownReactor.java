package prerna.solr.reactor;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class GetProjectMarkdownReactor extends AbstractReactor {
	
	public GetProjectMarkdownReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);
		if(projectId == null) {
			throw new IllegalArgumentException("Need to define the project to get the markdown from");
		}
		
		String projectMarkdown = SecurityProjectUtils.getProjectMarkdown(this.insight.getUser(), projectId);
		return new NounMetadata(projectMarkdown, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.PROJECT_INFO);
	}

}
