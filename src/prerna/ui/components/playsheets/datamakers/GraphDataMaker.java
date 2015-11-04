//package prerna.ui.components.playsheets.datamakers;
//
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
//
//import prerna.engine.api.IEngine;
//import prerna.om.GraphDataModel;
//
//public class GraphDataMaker implements IDataMaker {
//
//	private static final Logger LOGGER = LogManager.getLogger(GraphDataMaker.class.getName());
//	
//	GraphDataModel gdm = new GraphDataModel();
//
//	@Override
//	public void processDataMakerComponent(DataMakerComponent component) {
////		setPropSudowlSearch();
//		String query = component.getQuery();
//		IEngine engine = component.getEngine();
//		
//		gdm.createModel(query, engine);  // instrumented this call
//
//		LOGGER.info("Creating the base Graph");
//		gdm.fillStoresFromModel(engine); // < This is where the gen base graph models
//		
//		//empty incremental stores indicate no new data is available, which means the traversal is invalid. this will set the traversal back one step and remove the invalid traversal from rc and model stores.
//		if(gdm.getIncrementalVertStore() != null && gdm.getIncrementalEdgeStore() != null && gdm.getIncrementalVertStore().isEmpty() && gdm.getIncrementalEdgeStore().isEmpty() && gdm.getIncrementalVertPropStore() != null && gdm.getIncrementalVertPropStore().isEmpty() ) {
//			gdm.setUndo(true);
//			gdm.undoData();
//			gdm.fillStoresFromModel(engine);
//			gdm.rcStore.remove(gdm.rcStore.size()-1);
//			gdm.modelStore.remove(gdm.modelStore.size()-1);
//		}
//		
//	}
//
//	@Override
//	public Object getDMData() {
//		return gdm;
//	}
//
//}
