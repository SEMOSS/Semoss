package prerna.solr.reactor;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class GetEngineMarkdownReactor extends AbstractReactor {
	
	public GetEngineMarkdownReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.ENGINE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		if(engineId == null) {
			throw new IllegalArgumentException("Need to define the engine to get the markdown from");
		}
		
		String engineMarkdown = SecurityEngineUtils.getEngineMarkdown(this.insight.getUser(), engineId);
		return new NounMetadata(engineMarkdown, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.ENGINE_INFO);
	}

}
