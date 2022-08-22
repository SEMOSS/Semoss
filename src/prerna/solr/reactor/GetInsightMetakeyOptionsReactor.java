package prerna.solr.reactor;

import java.util.List;
import java.util.Map;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityDatabaseUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;

public class GetInsightMetaKeyOptionsReactor extends AbstractReactor {
	
	private static final String META_KEYS = "metakey";

	public GetInsightMetaKeyOptionsReactor() {
		this.keysToGet = new String[] {META_KEYS};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String metakey = this.keyValue.get(this.keysToGet[0]);
		List<Map<String, Object>> ret;
		if(AbstractSecurityUtils.securityEnabled()) {
			ret = SecurityDatabaseUtils.getMetakeyOptions(Constants.INSIGHT_METAKEYS, metakey);
		} else {
			return null;
		}
		NounMetadata noun = new NounMetadata(ret, PixelDataType.PIXEL_OBJECT);
		return noun;
	}
	
}
