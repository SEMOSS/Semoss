package prerna.reactor.utils;

import java.util.HashMap;

import prerna.engine.impl.web.WebScrapeEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetTableHeader extends AbstractReactor {

	public GetTableHeader() {
		this.keysToGet = new String[]{ReactorKeysEnum.URL.getKey(), "alias_map"};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String url = this.keyValue.get(this.keysToGet[0]);
		HashMap aliasMap = (HashMap)this.getNounStore().getNoun("aliasMap").get(0);
		
		WebScrapeEngine engine = new WebScrapeEngine();
		
		String [] headers = engine.getHeaders(url, aliasMap);
				
		StringBuffer accumulator = new StringBuffer("");
		
		for(int headerIndex = 0;headerIndex < headers.length;headerIndex++)
		{
			if(headerIndex != 0)
				accumulator.append("||");
			accumulator.append(headers[headerIndex]);
		}
		accumulator.append("");
		
		return new NounMetadata(accumulator.toString(), PixelDataType.CONST_STRING);
	}

}
