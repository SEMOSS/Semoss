package prerna.sablecc2.reactor.frame.r;

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

public class CollapseReactor extends AbstractRFrameReactor {

	public CollapseReactor() {
		this.keysToGet = new String[] { "groupByColumn", ReactorKeysEnum.VALUE.getKey(),
				ReactorKeysEnum.DELIMITER.getKey() };
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		RDataTable frame = (RDataTable) getFrame();
		String frameName = frame.getTableName();
		List<String> groupByCol = getGroupByCols();
		String valueCol = this.keyValue.get(this.keysToGet[1]);
		String delim = this.keyValue.get(this.keysToGet[2]);
		
		StringBuilder rsb = new StringBuilder();
		String groupByColsR = "list(";
		Object[] row = new Object[groupByCol.size() + 1];
		Object[] newColNames = new Object[groupByCol.size() + 1];
		String tempFrame = "tempFrame" + Utility.getRandomString(8);
		String collapsedColName = valueCol+"_Collapse";
		for(int i=0; i < groupByCol.size(); i++) {
			String groupCol = groupByCol.get(i);
			groupByColsR += tempFrame+"$"+groupCol;
			if(i < groupByCol.size() - 1) {
				groupByColsR += " , ";
			}
			row[i] = groupCol;
			newColNames[i] = groupCol;
		}
		groupByColsR += ")";
		row[groupByCol.size()] = valueCol;
		newColNames[groupByCol.size()] = collapsedColName;
		
		// get subset of frame using columns selected
		String test = RSyntaxHelper.createStringRColVec(row);
		rsb.append(tempFrame +"<- subset("+frameName+", select="+ test+");" );
		
		// get unique subset values
		rsb.append(tempFrame +"<- unique("+tempFrame+");" );

		// aggregate values
		String aggFrame = "aggFrame"+ Utility.getRandomString(8);
		String delimR = "'"+delim+"'";
		rsb.append(aggFrame+" <- aggregate("+tempFrame+"$"+valueCol+", by = "+groupByColsR+", paste, collapse="+delimR+");");
		
		// replace current frame with agg frame
		rsb.append(frameName +"<- as.data.table("+aggFrame+");" );
		
		//rename columns
		String names = RSyntaxHelper.createStringRColVec(newColNames);
		rsb.append("colnames("+frameName+") <- "+names );

		this.rJavaTranslator.runR(rsb.toString());
		recreateMetadata(frameName);
		
		// clean up R temp variables
		StringBuilder cleanUpScript = new StringBuilder();
		cleanUpScript.append("rm(" + aggFrame + ");");
		cleanUpScript.append("rm(" + tempFrame + ");");
		cleanUpScript.append("gc();");
		this.rJavaTranslator.runR(cleanUpScript.toString());

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
					//get each individual column entry and clean 
					String column = colGRS.get(i).toString();
					if (column.contains("__")) {
						column = column.split("__")[1];
					}
					colInputs.add(column);
				}
			}
		}
		return colInputs;
	}
}
