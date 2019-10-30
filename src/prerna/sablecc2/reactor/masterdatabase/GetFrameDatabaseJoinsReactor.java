package prerna.sablecc2.reactor.masterdatabase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.algorithm.api.ITableDataFrame;
import prerna.auth.utils.AbstractSecurityUtils;
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
		this.keysToGet = new String[]{ReactorKeysEnum.FRAME.getKey(), ReactorKeysEnum.APP.getKey()};
	}

	@Override
	public NounMetadata execute() {
		Map<String, Map<String, List<String>>> connections = new HashMap<String, Map<String, List<String>>>();
		
		List<String> appFilters = null;
		if(AbstractSecurityUtils.securityEnabled()) {
			appFilters = SecurityQueryUtils.getVisibleUserEngineIds(this.insight.getUser());
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
		
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		System.out.println(gson.toJson(connections));
		
		return new NounMetadata(connections, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DATABASE_TRAVERSE_OPTIONS);
		
//		String engineId = getApp();
//		if(engineId != null) {
//			engineId = MasterDatabaseUtility.testEngineIdIfAlias(engineId);
//		}
//		
//		List<String> appliedAppFilters = new Vector<String>();
//		
//		// account for security
//		// TODO: THIS WILL NEED TO ACCOUNT FOR COLUMNS AS WELL!!!
//		List<String> appFilters = null;
//		if(AbstractSecurityUtils.securityEnabled()) {
//			appFilters = SecurityQueryUtils.getUserEngineIds(this.insight.getUser());
//			if(!appFilters.isEmpty()) {
//				if(engineId != null) {
//					// need to make sure it is a valid engine id
//					if(!appFilters.contains(engineId)) {
//						throw new IllegalArgumentException("Database does not exist or user does not have access to database");
//					}
//					// we are good
//					appliedAppFilters.add(engineId);
//				} else {
//					// set default as filters
//					appliedAppFilters = appFilters;
//				}
//			} else {
//				if(engineId != null) {
//					appliedAppFilters.add(engineId);
//				}
//			}
//		} else if(engineId != null) {
//			appliedAppFilters.add(engineId);
//		}
//		
//		List<String> inputColumnValues = getColumns();
//		List<String> localConceptIds = MasterDatabaseUtility.getLocalConceptIdsFromPixelName(inputColumnValues);
//		
//		//TODO: this is giving weirder options than expected ... come back to this
//		//TODO: this is giving weirder options than expected ... come back to this
//		//TODO: this is giving weirder options than expected ... come back to this
//		//TODO: this is giving weirder options than expected ... come back to this
////		localConceptIds.addAll(MasterDatabaseUtility.getLocalConceptIdsFromSimilarLogicalNames(inputColumnValues));
//		
//		List<Map<String, Object>> data = MasterDatabaseUtility.getDatabaseConnections(localConceptIds, appliedAppFilters);
//		return new NounMetadata(data, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DATABASE_TRAVERSE_OPTIONS);
	}
	
	/**
	 * Getter for the list
	 * @return
	 */
	private List<String> getColumns() {
		// is it defined within store
		{
			GenRowStruct cGrs = this.store.getNoun(this.keysToGet[0]);
			if(cGrs != null && !cGrs.isEmpty()) {
				List<String> columns = new Vector<String>();
				for(int i = 0; i < cGrs.size(); i++) {
					String value = cGrs.get(0).toString();
					if(value.contains("__")) {
						columns.add(value.split("__")[1].replaceAll("\\s+", "_"));
					} else {
						columns.add(value.replaceAll("\\s+", "_"));
					}
				}
				return columns;
			}
		}
		
		// is it inline w/ currow
		List<String> columns = new Vector<String>();
		for(int i = 0; i < this.curRow.size(); i++) {
			String value = this.curRow.get(i).toString();
			if(value.contains("__")) {
				columns.add(value.split("__")[1].replaceAll("\\s+", "_"));
			} else {
				columns.add(value.replaceAll("\\s+", "_"));
			}
		}
		return columns;
	}
	
	private String getApp() {
		GenRowStruct grs = this.store.getNoun(this.keysToGet[1]);
		if(grs != null && !grs.isEmpty()) {
			return grs.get(0).toString();
		}
		return null;
	}
}
