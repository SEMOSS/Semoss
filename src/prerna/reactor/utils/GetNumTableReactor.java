package prerna.reactor.utils;

import java.util.HashMap;

import prerna.engine.impl.web.WebScrapeEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetNumTableReactor extends AbstractReactor {

	public GetNumTableReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.URL.getKey(), "aliasMap"};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();

		String url = this.keyValue.get(this.keysToGet[0]);
		HashMap aliasMap = (HashMap)this.getNounStore().getNoun("aliasMap").get(0);

		
		WebScrapeEngine engine = new WebScrapeEngine();
		
		int numTables = engine.getNumTables(url, aliasMap);
				
		
		return new NounMetadata(numTables, PixelDataType.CONST_STRING);
	}

}
