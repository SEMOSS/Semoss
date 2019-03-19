package prerna.sablecc2.reactor.frame.py;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.py.PandasFrame;
import prerna.ds.py.PandasSyntaxHelper;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.AbstractFrameReactor;
import prerna.sablecc2.reactor.imports.ImportUtility;
import prerna.util.Utility;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;


public class CollapseReactor extends AbstractFramePyReactor {

	public CollapseReactor() {
		this.keysToGet = new String[] { "groupByColumn", ReactorKeysEnum.VALUE.getKey(),
				ReactorKeysEnum.DELIMITER.getKey(), ReactorKeysEnum.MAINTAIN_COLUMNS.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		PandasFrame frame = (PandasFrame) getFrame();
		String frameName = frame.getName();
		List<String> groupByCol = getGroupByCols();
		String valueCol = ", '" + this.keyValue.get(this.keysToGet[1]) + "'";
		String delim = ", '" + this.keyValue.get(this.keysToGet[2]) + "'";

		StringBuilder rsb = new StringBuilder();
		String groupByColsR = "[";
		
		// group by cols
		for (int i = 0; i < groupByCol.size(); i++) 
		{
			String groupCol = groupByCol.get(i);
			if(i == 0)
				groupByColsR = groupByColsR + "'" + groupCol + "'";
			else
				groupByColsR = groupByColsR + ", '" + groupCol + "'";			
		}
		groupByColsR += "]";
		
		
		// main cols
		// get columns to keep
		// convert to a list
		StringBuilder maintainCols = new StringBuilder("");
		HashSet<String> colsToKeep = getKeepCols();
		if (colsToKeep != null) {
			// merge columns
			maintainCols.append(", [");
			colsToKeep.addAll(groupByCol);
			
			Iterator  <String> maintainIterator = colsToKeep.iterator();
			for(int maintainColIndex = 0;maintainIterator.hasNext();maintainColIndex++)
			{
				String thisCol = maintainIterator.next();
				if(maintainColIndex > 0)
					maintainCols.append(", ");
				maintainCols.append("'").append(thisCol).append("'");
			}
			maintainCols.append("]");			
		}

		String script = frameName + " = " + frameName + "w.collapse(" + groupByColsR + valueCol + delim + maintainCols + ")";
		frame.runScript(script);
						
		frame = (PandasFrame) recreateMetadata(frame);

		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				frame, 
				"Collapse", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		
		NounMetadata retNoun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE, PixelOperationType.FRAME_DATA_CHANGE);
		return retNoun;
	}
	
	private List<String> getGroupByCols() {
		List<String> colInputs = new Vector<String>();
		GenRowStruct colGRS = this.store.getNoun(this.keysToGet[0]);
		if (colGRS != null) {
			int size = colGRS.size();
			if (size > 0) {
				for (int i = 0; i < size; i++) {
					// get each individual column entry and clean
					String column = colGRS.get(i).toString();
					colInputs.add(column);
				}
			}
		}
		return colInputs;
	}

	private HashSet<String> getKeepCols() {
		HashSet<String> colInputs = new HashSet<String>();
		GenRowStruct colGRS = this.store.getNoun(ReactorKeysEnum.MAINTAIN_COLUMNS.getKey());
		if (colGRS != null) {
			int size = colGRS.size();
			if (size > 0) {
				for (int i = 0; i < size; i++) {
					// get each individual column entry and clean
					String column = colGRS.get(i).toString();
					colInputs.add(column);
				}
				return colInputs;
			}
		}
		return null;
	}
	

}
