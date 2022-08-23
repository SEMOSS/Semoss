package prerna.solr.reactor;

import java.util.List;
import java.util.Map;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class GetProjectMetakeyOptionsReactor extends AbstractReactor {
	
	private static final String META_KEYS = "metakey";

	public GetProjectMetakeyOptionsReactor() {
		this.keysToGet = new String[] {META_KEYS};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String metakey = this.keyValue.get(this.keysToGet[0]);
		List<Map<String, Object>> ret = SecurityProjectUtils.getMetakeyOptions(metakey);
		NounMetadata noun = new NounMetadata(ret, PixelDataType.PIXEL_OBJECT);
		return noun;
	}
	
}
