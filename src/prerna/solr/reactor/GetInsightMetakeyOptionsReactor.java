package prerna.solr.reactor;

import java.util.List;
import java.util.Map;

import prerna.auth.utils.SecurityInsightUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetInsightMetakeyOptionsReactor extends AbstractReactor {

	public GetInsightMetakeyOptionsReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.META_KEYS.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String metakey = this.keyValue.get(this.keysToGet[0]);
		List<Map<String, Object>> ret = SecurityInsightUtils.getMetakeyOptions(metakey);
		NounMetadata noun = new NounMetadata(ret, PixelDataType.PIXEL_OBJECT);
		return noun;
	}
	
}
