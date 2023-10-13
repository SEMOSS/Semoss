package prerna.reactor.frame.graph.r;

import java.util.List;

import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.TinkerFrame;
import prerna.engine.impl.tinker.iGraphUtilities;
import prerna.reactor.frame.r.AbstractRFrameReactor;
import prerna.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class RemoveNodeReactor extends AbstractRFrameReactor {

	private static final String CLASS_NAME = RemoveNodeReactor.class.getName();

	public RemoveNodeReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.FRAME.getKey(), ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.VALUE.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		String[] packages = new String[] { "igraph" };
		this.rJavaTranslator.checkPackages(packages);
		this.rJavaTranslator.executeEmptyR("library(igraph)");
		Logger logger = getLogger(CLASS_NAME);

		ITableDataFrame frame = getFrame();
		if(!(frame instanceof TinkerFrame)) {
			throw new IllegalArgumentException("Frame must be a graph frame type");
		}
		TinkerFrame graph = (TinkerFrame) frame;
		AbstractRJavaTranslator rJavaTranslator = this.insight.getRJavaTranslator(CLASS_NAME);
		if(!graph.isIGraphSynched()) {
			String wd = this.insight.getInsightFolder();
			iGraphUtilities.synchronizeGraphToR(graph, rJavaTranslator, graph.getName(), wd, logger);
		}
		
		String type = getColumn();
		List<Object> values = getValues();
		graph.remove(type, values);
		iGraphUtilities.removeNodeFromR(graph.getName(), rJavaTranslator, type, values, logger);
		return new NounMetadata(graph, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}
	
	/**
	 * 
	 * @return
	 */
	private String getColumn() {
		GenRowStruct inputsGRS = this.store.getNoun(this.keysToGet[1]);
		if (inputsGRS == null) {
			inputsGRS = this.getCurRow();
		}
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			// first noun will be the column to update
			NounMetadata noun1 = inputsGRS.getNoun(0);
			String fullUpdateCol = noun1.getValue() + "";
			if (fullUpdateCol.length() == 0) {
				throw new IllegalArgumentException("Need to define column type");
			}
			return fullUpdateCol;
		}
		throw new IllegalArgumentException("Need to define column type");
	}

	private List<Object> getValues() {
		GenRowStruct valuesGrs = this.store.getNoun(keysToGet[2]);
		if (valuesGrs == null || valuesGrs.isEmpty()) {
			throw new IllegalArgumentException("Need to define values");
		}
		return valuesGrs.getAllValues();
	}
}
