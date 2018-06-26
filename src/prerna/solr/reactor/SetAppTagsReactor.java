package prerna.solr.reactor;

import java.util.List;
import java.util.Vector;

import prerna.auth.SecurityUtils;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class SetAppTagsReactor extends AbstractReactor {
	
	public static final String TAGS = "tags";

	public SetAppTagsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), TAGS};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String appName = this.keyValue.get(this.keysToGet[0]);
		List<String> tags = getTags();
		if(this.securityEnabled()) {
			if(this.getUserAppFilters().contains(appName)) {
				SecurityUtils.setEngineMeta(appName, "tags", tags);
			} else {
				throw new IllegalArgumentException("App does not exist or user does not have access to database");
			}
		} else {
			SecurityUtils.setEngineMeta(appName, "tags", tags);
		}
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.APP_INFO);
	}
	
	public List<String> getTags() {
		List<String> tags = new Vector<String>();
		
		// see if added as key
		GenRowStruct grs = this.store.getNoun(this.keysToGet[1]);
		if(grs != null && !grs.isEmpty()) {
			int size = grs.size();
			for(int i = 0; i < size; i++) {
				tags.add(grs.get(i).toString());
			}
			return tags;
		}
		
		// start at index 1 and see if in cur row
		int size = this.curRow.size();
		for(int i = 1; i < size; i++) {
			tags.add(this.curRow.get(i).toString());
		}
		return tags;
	}

}
