package prerna.solr.reactor;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class GetDatabaseMarkdownReactor extends AbstractReactor {
	
	public GetDatabaseMarkdownReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		if(databaseId == null) {
			throw new IllegalArgumentException("Need to define the database to get the markdown from");
		}
		
		String databaseMarkdown = SecurityEngineUtils.getDatabaseMarkdown(this.insight.getUser(), databaseId);
		return new NounMetadata(databaseMarkdown, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DATABASE_INFO);
	}

}
