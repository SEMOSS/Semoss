package prerna.sablecc;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.QueryStruct;
import prerna.ds.TableDataFrameFactory;
import prerna.ds.H2.H2Frame;
import prerna.ds.nativeframe.NativeFrame;
import prerna.engine.api.IEngine;
import prerna.engine.api.IScriptReactor;
import prerna.rdf.query.builder.IQueryInterpreter;
import prerna.sablecc.PKQLRunner.STATUS;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class NativeImportDataReactor extends ImportDataReactor {

	public Iterator process() {
		
		super.process();
			
		if(myStore.containsKey(PKQLEnum.API)) {	
			NativeFrame frame = (NativeFrame)myStore.get("G");
//			Map<String, Set<String>> edgeHash = (Map<String, Set<String>>) this.getValue(PKQLEnum.API + "_EDGE_HASH");
			IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp((this.getValue(PKQLEnum.API + "_ENGINE")+"").trim());
//			String query  = (String) myStore.get(PKQLEnum.API);

			String frameEngine = frame.getEngineName();
			if(frameEngine != null && engine != null) {
				
				//if we have data coming from a different engine, i.e. traversing across dbs
				if(!frameEngine.equals(engine.getEngineName())) {
					//need to convert from native frame to h2 frame
					H2Frame newFrame = TableDataFrameFactory.convertToH2FrameFromNativeFrame(frame);
					myStore.put(PKQLEnum.G, newFrame);
					
					String reactorName = newFrame.getScriptReactors().get(PKQLEnum.IMPORT_DATA);
					try {
						IScriptReactor importReactor = (IScriptReactor)Class.forName(reactorName).newInstance();
						return processScriptReactor(importReactor);
					} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
						e.printStackTrace();
					}
				}
			}
			
//			frame.createView(query);
			// 4 & 5) if engine is not null, merge the data into the frame
			// the engine is null when the api is actually a csv file
//			if(engine != null) {
				// put the edge hash and the logicalToValue maps within the myStore
				// will be used when the data is actually imported
//				Map[] mergedMaps = frame.mergeQSEdgeHash(edgeHash, engine, joinCols);
//				myStore.put("edgeHash", mergedMaps[0]);
//				myStore.put("logicalToValue", mergedMaps[1]);
//			} 
		}
		
		myStore.put("STATUS", STATUS.SUCCESS);
		return null;
	}
	
	private Iterator processScriptReactor(IScriptReactor reactor) {
		//set data into the reactor
		for(String key : myStore.keySet()) {
			reactor.put(key, myStore.get(key));
		}
		
		if(reactor instanceof H2ImportDataReactor) {
			((H2ImportDataReactor)reactor).process();
		}
		
		//set my data with data coming from the reactor
		for(String retKey : reactor.getKeys()) {
			myStore.put(retKey, reactor.getValue(retKey));
		}
		
		return null;
	}
}
