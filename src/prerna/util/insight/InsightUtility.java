package prerna.util.insight;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.h2.H2Frame;
import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.InMemStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.PixelPlanner;

public class InsightUtility {

	private InsightUtility() {
		
	}
	
	/**
	 * 	
	 * @param planner
	 * @param key
	 * @return
	 */
	public static NounMetadata removeVaraible(PixelPlanner planner, String key) {
		InMemStore<String, NounMetadata> varStore = planner.getVarStore();
		return removeVaraible(varStore, key);
	}

	/**
	 * 
	 * @param varStore
	 * @param key
	 * @return
	 */
	public static NounMetadata removeVaraible(InMemStore<String, NounMetadata> varStore, String key) {
		NounMetadata noun = varStore.remove(key);
		if(noun == null) {
			return new NounMetadata(false, PixelDataType.BOOLEAN);
		}
		
		PixelDataType nType = noun.getNounType();
		if(nType == PixelDataType.FRAME) {
			ITableDataFrame dm = (ITableDataFrame) noun.getValue();
			//TODO: expose a delete on the frame to hide this crap
			// drop the existing tables/connections if present
			if(dm instanceof H2Frame) {
				H2Frame frame = (H2Frame)dm;
				frame.dropTable();
				if(!frame.isInMem()) {
					frame.dropOnDiskTemporalSchema();
				}
			} else if(dm instanceof RDataTable) {
				RDataTable frame = (RDataTable)dm;
				frame.executeRScript("gc(" + frame.getTableName() + ");");
			}
		}
		
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}
	
	
	

}
