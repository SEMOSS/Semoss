package prerna.sablecc2.reactor.frame.r.analytics;

import java.util.List;
import java.util.Vector;

import prerna.ds.r.RDataTable;
import prerna.ds.r.RSyntaxHelper;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.util.Utility;

public class RSimilarityHeatReactor extends AbstractRFrameReactor {

	public RSimilarityHeatReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.INSTANCE_KEY.getKey(), ReactorKeysEnum.ATTRIBUTES.getKey() };
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		// get Pixel inputs
		RDataTable frame = (RDataTable) this.getFrame();
		String frameName = frame.getTableName();
		String instanceCol = this.keyValue.get(this.keysToGet[0]);
		List<String> comparisonColumn = getComparisonColumns();
		
		// create R syntax to get similarity heat value
		StringBuilder rsb = new StringBuilder();
		String tempFrame = Utility.getRandomString(8);
		List<String> temp = getComparisonColumns();
		temp.add(instanceCol);
		// make frame with only used columns
		rsb.append(RSyntaxHelper.getFrameSubset(tempFrame, frameName, temp.toArray()));
		String mergeBy = RSyntaxHelper.createStringRColVec(comparisonColumn.toArray());
		// combine with self
		rsb.append(tempFrame + " <- merge(" + tempFrame + "," + tempFrame + ", by=" + mergeBy + ", all.x = TRUE, all.y = FALSE, allow.cartesian = TRUE);");
		// remove where systems are the same
		rsb.append(tempFrame + " <- " + tempFrame + "[" + tempFrame + "$" + instanceCol + ".x != " + tempFrame + "$" + instanceCol + ".y,];");
		//# drop opposites i.e. a=b and b=a
		rsb.append(tempFrame + " <- " + tempFrame + "[!duplicated(apply(" + tempFrame + ",1,function(x) paste(sort(x),collapse=''))),];");
		rsb.append(tempFrame + " <- count(" + tempFrame + ", c('" + instanceCol + ".x', '" + instanceCol + ".y')); ");
		rsb.append(RSyntaxHelper.alterColumnName(tempFrame, instanceCol + ".x", instanceCol + "_1"));
		rsb.append(RSyntaxHelper.alterColumnName(tempFrame, instanceCol + ".y", instanceCol + "_2"));
		rsb.append(RSyntaxHelper.alterColumnName(tempFrame, "freq", "Heat"));
		rsb.append(RSyntaxHelper.asDataTable(frameName, tempFrame));
		rsb.append("rm(" + tempFrame + ")");
		this.rJavaTranslator.runR(rsb.toString());
		recreateMetadata(frameName);

		// now return this object
		NounMetadata noun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
		noun.addAdditionalReturn(
				new NounMetadata("You've successfully completed running similarity heat and generated a new frame", 
						PixelDataType.CONST_STRING, PixelOperationType.SUCCESS_MESSAGE));
		return noun;
	}
	
	private List<String> getComparisonColumns() {
		GenRowStruct grs = this.store.getNoun(this.keysToGet[1]);
		Vector<String> columns = new Vector<String>();
		NounMetadata noun;
		if (grs != null) {
			for (int i = 0; i < grs.size(); i++) {
				noun = grs.getNoun(i);
				if (noun != null) {
					String column = noun.getValue().toString();
					if (column.length() > 0) {
						columns.add(column);
					}
				}
			}
		}
		return columns;
	}
}
