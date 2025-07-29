package prerna.reactor.frame;

import java.util.List;
import java.util.Vector;

import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.TinkerFrame;
import prerna.ds.nativeframe.NativeFrame;
import prerna.ds.py.PandasFrame;
import prerna.engine.impl.tinker.iGraphUtilities;
import prerna.reactor.frame.r.AbstractRFrameReactor;
import prerna.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class FrameToGraphReactor extends AbstractRFrameReactor {
	
	public FrameToGraphReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FRAME.getKey(), ReactorKeysEnum.MODEL.getKey(), "userInput" };
	}
	
	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		
		GenRowStruct frameGrs = this.store.getNoun(this.keysToGet[0]);
		ITableDataFrame sourceFrame = getFrame();
		String sql = "SELECT * FROM " + sourceFrame.getName();  
		sourceFrame.getColumnHeaders();
		sourceFrame.getQsHeaders();
		for (String header : sourceFrame.getColumnHeaders()) {
			System.out.println("Normal Frame Header" + header);
		}
		for (String header : sourceFrame.getQsHeaders()) {
			System.out.println("Normal Frame Header" + header);
		}
		
		
		String[] packages = new String[] {"igraph"};
		this.rJavaTranslator.checkPackages(packages);
		this.rJavaTranslator.executeEmptyR("library(igraph)");

		
		// 5 types of Frames: Native, Python, Grid (SQL), R and Tinker Frames
		if (sourceFrame instanceof NativeFrame) {
			
			NativeFrame sourceNFrame = (NativeFrame) sourceFrame;
			sourceFrame.querySQL(sql);
			
		} else if(sourceFrame instanceof PandasFrame) {
			
		} else if (sourceFrame instanceof TinkerFrame) {
			System.out.println("TODO: Implement Grid, R, and Tinker Frame instances");
//		} else if (sourceFrame instanceof ) {
			
		} else {
			
		}

//		TinkerFrame graph = (TinkerFrame) frame;
//		AbstractRJavaTranslator rJavaTranslator = this.insight.getRJavaTranslator(CLASS_NAME);
		String wd = this.insight.getInsightFolder();
		
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.OPERATION);
	}
	
	protected ITableDataFrame getFrame() {
		GenRowStruct frameGrs = this.store.getNoun(this.keysToGet[0]);
		if(frameGrs != null && !frameGrs.isEmpty()) {
			return (ITableDataFrame) frameGrs.get(0);
		}
		
		List<NounMetadata> frameCur = this.curRow.getNounsOfType(PixelDataType.FRAME);
		if(frameCur != null && !frameCur.isEmpty()) {
			return (ITableDataFrame) frameCur.get(0).getValue();
		}
		
		throw new IllegalArgumentException("Must define the frame frame");
	}
	
	protected List<String> getFrameColumns() {
		List<String> columns = new Vector<String>();

		GenRowStruct sourceColGrs = this.store.getNoun(this.keysToGet[2]);
		if (sourceColGrs != null && !sourceColGrs.isEmpty()) {
			for (int selectIndex = 0; selectIndex < sourceColGrs.size(); selectIndex++) {
				String column = sourceColGrs.get(selectIndex) + "";
				columns.add(column);
			}
			return columns;
		}
		throw new IllegalArgumentException("Must define the frame columns");
	}
	
	public String getName()
	{
		return "FrameToGraph";
	}
}
