package prerna.usertracking.reactors;

import java.util.ArrayList;
import java.util.List;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.EngineUsageUtils;
import prerna.util.Utility;

public class TrendingDatabasesReactor extends AbstractReactor {
	
	public TrendingDatabasesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.NUM_DISPLAY.getKey()};
	}

	@Override 
	public NounMetadata execute() {
		organizeKeys();
		if (Utility.isUserTrackingDisabled()) {
			return new NounMetadata(false, PixelDataType.BOOLEAN, PixelOperationType.USER_TRACKING_DISABLED);
		}
		
		List<String> eTypes = new ArrayList<>();
		eTypes.add(IEngine.CATALOG_TYPE.DATABASE.toString());
		
		String numDisplay = this.keyValue.get(this.keysToGet[0]);
		if (numDisplay == null) {
			numDisplay = "5";
		}
		Integer nd = Integer.valueOf(numDisplay);
		
		List<String> accessibleDbs = SecurityEngineUtils.getUserEngineIdList(this.insight.getUser(), eTypes, true, true, true);
		
		List<String> dbs = EngineUsageUtils.getTrendingDatabases(nd, accessibleDbs);
		
		// just fill out the rest of the trending databases for consistency.
		// Netflix has terrible recommendations too :)
		if (dbs.size() < nd) {
			accessibleDbs.removeAll(dbs);
			int size = accessibleDbs.size();
			int toAdd = Math.min(size, nd - dbs.size());
			for (int i = 0; i < toAdd; i++) {
				dbs.add(accessibleDbs.get(i));
			}
		}
		
		return new NounMetadata(dbs, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DATABASE_LIST);
	}

}