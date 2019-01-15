package prerna.sablecc2.reactor.frame.r;

import java.util.List;
import java.util.Map;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class UpdateSimilarColumnValuesReactor extends AbstractRFrameReactor {

	public static final String MATCHES = "matches";
	public static final String MATCHES_TABLE = "matchesTable";

	public UpdateSimilarColumnValuesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMN.getKey(), MATCHES_TABLE, MATCHES };
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		String column = this.keyValue.get(this.keysToGet[0]);
		String matchesTable = this.keyValue.get(this.keysToGet[1]);

		// check if packages are installed
		String[] packages = { "stringdist", "data.table" };
		this.rJavaTranslator.checkPackages(packages);
		
		StringBuilder rsb = new StringBuilder();
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String bestMatchScript = "source(\"" + baseFolder + "\\R\\Recommendations\\advanced_federation_blend.r\");";
		bestMatchScript = bestMatchScript.replace("\\", "/");
		rsb.append(bestMatchScript);

		// get single column input
		String linkFrame = "link" + Utility.getRandomString(5);
		RDataTable frame = (RDataTable) getFrame();
		String frameName = frame.getName();
		String col1 = matchesTable + "col1";
		rsb.append(col1 + "<- as.character(" + frameName + "$" + column + ");");

		// iterate matches and create the link frame
		List<String> allMatches = getInputList(MATCHES);
		// add all matches
		if (allMatches != null && !(allMatches.isEmpty())) {
			StringBuilder col1Builder = new StringBuilder();
			StringBuilder col2Builder = new StringBuilder();
			StringBuilder col3Builder = new StringBuilder();
			for (int i = 0; i < allMatches.size(); i++) {
				if (i != 0) {
					col1Builder.append(",");
					col2Builder.append(",");
					col3Builder.append(",");
				}
				String match = (String) allMatches.get(i);
				String[] matchList = match.split(" == ");
				if (matchList.length > 2) {
					throw new IllegalArgumentException("match seperator didnt work");
				}
				String column1 = matchList[0];
				String column2 = matchList[1];
				col1Builder.append("\"" + column1 + "\"");
				col2Builder.append("\"" + column2 + "\"");
				col3Builder.append("1");
			}
			// add all matches provided
			String script = linkFrame + " <- data.table(\"col1\"=c(" + col1Builder + "), \"col2\"=c(" + col2Builder	+ ")); ";
			rsb.append(script);
		}
		// make linkframe unique
		rsb.append(linkFrame + " <- unique(" + linkFrame + ");");

		// get current frame data type
		boolean convertJoinColFromNum = false;
		OwlTemporalEngineMeta metaData = this.getFrame().getMetaData();
		Map<String, SemossDataType> typeMap = metaData.getHeaderToTypeMap();
		SemossDataType dataType = typeMap.get(column);
		if (dataType == SemossDataType.DOUBLE || dataType == SemossDataType.INT) {
			convertJoinColFromNum = true;
		}

		// call the curate script
		String resultFrame = Utility.getRandomString(8);
		rsb.append(resultFrame + "<- curate(" + col1 + "," + linkFrame + ");");

		String tempColHeader = Utility.getRandomString(8);

		// make resultFrame a DT and update the header to a temp name
		rsb.append(resultFrame + " <- as.data.table(" + resultFrame + ");" + "names(" + resultFrame + ")<-\"" + tempColHeader + "\";");
		
		// add new temp name column to frame
		rsb.append(frameName + " <- cbind(" + frameName + "," + resultFrame + ");");
		
		// delete existing column from frame
		rsb.append(frameName + " <- " + frameName + "[,-c(\"" + column + "\")];");
		
		// update temp column name to the original column name
		rsb.append("colnames(" + frameName + ")[colnames(" + frameName + ")==\"" + tempColHeader + "\"] <- \"" + column + "\";");

		// return data type to original state
		if (convertJoinColFromNum) {
			rsb.append(frameName + "$" + column + " <- as.numeric(as.character(" + frameName + "$" + column + "));");
		}

		rsb.append("rm(" + resultFrame + "," + linkFrame + "," + col1 +  "," + matchesTable + ", best_match, best_match_nonzero, best_match_zero, blend, curate, self_match );");
		
		this.rJavaTranslator.runR(rsb.toString());
		
		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				frame, 
				"UpdateSimilarColumnValues", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		NounMetadata retNoun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);

		return retNoun;
	}

	private List<String> getInputList(String key) {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(key);
		if (columnGrs != null) {
			if (columnGrs.size() > 0) {
				List<String> values = columnGrs.getAllStrValues();
				return values;
			}
		}
		// else, we assume it is values in the curRow
		List<String> values = this.curRow.getAllStrValues();
		return values;
	}
}
