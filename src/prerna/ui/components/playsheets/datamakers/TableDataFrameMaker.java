//package prerna.ui.components.playsheets.datamakers;
//
//import java.util.List;
//
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
//
//import prerna.algorithm.api.ITableDataFrame;
//import prerna.engine.api.IEngine;
//import prerna.engine.api.ISelectWrapper;
//import prerna.rdf.engine.wrappers.WrapperManager;
//
//public class TableDataFrameMaker implements IDataMaker {
//
//	private static final Logger LOGGER = LogManager.getLogger(TableDataFrameMaker.class.getName());
//	
//	ITableDataFrame dataFrame;
//
//	@Override
//	public void processDataMakerComponent(DataMakerComponent component) {
//		IEngine engine = component.getEngine();
//		String query = component.getQuery();
//		
//		processPreTransformations(component.getPreTrans());
//		
//		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);
//
//		ITableDataFrame newDataFrame = wrapper.getTableDataFrame();
//		
//		if(dataFrame == null){
//			this.dataFrame = newDataFrame;
//		}
//		
//		// old code to fill data frame
////		String[] names = wrapper.getVariables();
////		dataFrame = new BTreeDataFrame(names);
//		
////		// now get the bindings and generate the data
////		try {
////			while(wrapper.hasNext()) {
////				ISelectStatement sjss = wrapper.next();
////				dataFrame.addRow(sjss);
////			}
////		} catch (RuntimeException e) {
////			logger.fatal(e);
////		}
//		
//		processPostTransformations(component.getPostTrans(), dataFrame);
//		
//	}
//	
//	private void processPreTransformations(List<ISEMOSSTransformation> transforms){
//		
//	}
//	
//	private void processPostTransformations(List<ISEMOSSTransformation> transforms, ITableDataFrame dataFrame){
//		
//	}
//
//	@Override
//	public Object getDMData() {
//		return this.dataFrame;
//	}
//
//}
