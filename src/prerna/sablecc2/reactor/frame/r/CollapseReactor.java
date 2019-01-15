package prerna.sablecc2.reactor.frame.r;

import java.util.HashSet;
import java.util.List;
import java.util.Vector;

import prerna.ds.r.RDataTable;
import prerna.ds.r.RSyntaxHelper;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class CollapseReactor extends AbstractRFrameReactor {

	public CollapseReactor() {
		this.keysToGet = new String[] { "groupByColumn", ReactorKeysEnum.VALUE.getKey(),
				ReactorKeysEnum.DELIMITER.getKey(), ReactorKeysEnum.MAINTAIN_COLUMNS.getKey() };
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		RDataTable frame = (RDataTable) getFrame();
		String frameName = frame.getName();
		List<String> groupByCol = getGroupByCols();
		String valueCol = this.keyValue.get(this.keysToGet[1]);
		String delim = this.keyValue.get(this.keysToGet[2]);

		StringBuilder rsb = new StringBuilder();
		String groupByColsR = "list(";
		String[] row = new String[groupByCol.size() + 1];
		Object[] newColNames = new Object[groupByCol.size() + 1];
		String tempFrame = "tempFrame" + Utility.getRandomString(8);
		String collapsedColName = valueCol + "_Collapse";
		for (int i = 0; i < groupByCol.size(); i++) {
			String groupCol = groupByCol.get(i);
			groupByColsR += tempFrame + "$" + groupCol;
			if (i < groupByCol.size() - 1) {
				groupByColsR += " , ";
			}
			row[i] = groupCol;
			newColNames[i] = groupCol;
		}
		groupByColsR += ")";
		row[groupByCol.size()] = valueCol;
		newColNames[groupByCol.size()] = collapsedColName;

		// get subset of frame using columns selected
		rsb.append(RSyntaxHelper.getFrameSubset(tempFrame, frameName, row));
		
		// get unique subset values
		rsb.append(tempFrame + "<-unique(" + tempFrame + ");");

		// aggregate values
		String aggFrame = "aggFrame" + Utility.getRandomString(8);
		String delimR = "'" + delim + "'";
		rsb.append(aggFrame + " <- aggregate(" + tempFrame + "$" + valueCol + ", by = " + groupByColsR + ", paste, collapse=" + delimR + ");");		
		
		// rename columns
		String names = RSyntaxHelper.createStringRColVec(newColNames);
		rsb.append("colnames(" + aggFrame + ") <- " + names + ";");
		
		// get columns to keep
		HashSet<String> colsToKeep = getKeepCols();
		if (colsToKeep != null) {
			// merge columns
			colsToKeep.addAll(groupByCol);
			String mergeFrame = Utility.getRandomString(8);
			// get subset of frame using columns selected
			rsb.append(RSyntaxHelper.getFrameSubset(mergeFrame, frameName, colsToKeep.toArray()));
			rsb.append(aggFrame + "<- merge(" + aggFrame + "," + mergeFrame + ", by = " + RSyntaxHelper.createStringRColVec(groupByCol.toArray()) + ");");
		}
		// replace current frame with agg frame
		rsb.append(RSyntaxHelper.asDataTable(frameName, aggFrame));
		rsb.append("rm(" + aggFrame + ");");
		rsb.append("rm(" + tempFrame + ");");
		rsb.append("gc();");

		this.rJavaTranslator.runR(rsb.toString());
		recreateMetadata(frameName);

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
