package prerna.sablecc2.reactor.masterdatabase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityDatabaseUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.AbstractFrameReactor;

public class GetFrameDatabaseJoinsReactor extends AbstractFrameReactor {
	
	public GetFrameDatabaseJoinsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.FRAME.getKey(), ReactorKeysEnum.DATABASE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		Map<String, Map<String, List<String>>> connections = new HashMap<String, Map<String, List<String>>>();
		
		List<String> appFilters = null;
		if(AbstractSecurityUtils.securityEnabled()) {
			String specificAppFilter = getApp();
			if(specificAppFilter != null) {
				if(!SecurityDatabaseUtils.userCanViewDatabase(this.insight.getUser(), specificAppFilter)) {
					throw new IllegalArgumentException("Database " + specificAppFilter + " does not exist or user does not have access to database");
				}
				appFilters = new Vector<String>();
				appFilters.add(specificAppFilter);
			} else {
				appFilters = SecurityQueryUtils.getVisibleUserDatabaseIds(this.insight.getUser());
			}
		}
		
		ITableDataFrame frame = getFrame();
		OwlTemporalEngineMeta meta = frame.getMetaData();
		Map<String, List<String[]>> dbInfo = meta.getDatabaseInformation();
		
		for(String uniqueFrameHeader : dbInfo.keySet()) {
			// keep a list of the physical name ids
			List<String> physicalNameIds = new Vector<String>();
			List<String> unknowns = new Vector<String>();
			
			// grab a list of the physical names
			List<String[]> qsData = dbInfo.get(uniqueFrameHeader);
			
			for(String[] info : qsData) {
				if(info.length == 2) {
					String physicalNameId = MasterDatabaseUtility.getPhysicalConceptIdFromPixelName(info[0], info[1]);
					physicalNameIds.add(physicalNameId);
				} else {
					unknowns.add(info[0]);
				}
			}

			// now query to find all related things to this frame header
			List<String> conceptualNames = MasterDatabaseUtility.getConceptualNamesFromPhysicalIds(physicalNameIds);
			List<String[]> headerConnections = MasterDatabaseUtility.getConceptualConnections(conceptualNames, appFilters);
			
			// in the connections map
			// i want to put
			// {appId -> {app_pixel_selector -> [frame_unique_id1, frame_unique_id2] } }
			for(String[] hConn : headerConnections) {
				String appId = hConn[0];
				String appSelector = hConn[1];
				
				Map<String, List<String>> appMap = null;
				if(connections.containsKey(appId)) {
					appMap = connections.get(appId);
				} else {
					appMap = new HashMap<String, List<String>>();
					connections.put(appId, appMap);
				}
				
				List<String> frameHeaderList = null;
				if(appMap.containsKey(appSelector)) {
					frameHeaderList = appMap.get(appSelector);
				} else {
					frameHeaderList = new Vector<String>();
					appMap.put(appSelector, frameHeaderList);
				}
				
				frameHeaderList.add(uniqueFrameHeader);
			}
		}
		
		return new NounMetadata(connections, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DATABASE_TRAVERSE_OPTIONS);
	}
	
	private String getApp() {
		GenRowStruct grs = this.store.getNoun(this.keysToGet[1]);
		if(grs != null && !grs.isEmpty()) {
			return grs.get(0).toString();
		}
		return null;
	}
}
