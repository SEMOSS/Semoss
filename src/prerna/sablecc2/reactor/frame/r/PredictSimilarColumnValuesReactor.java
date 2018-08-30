package prerna.sablecc2.reactor.frame.r;

import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class PredictSimilarColumnValuesReactor extends AbstractRFrameReactor {

	public PredictSimilarColumnValuesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMN.getKey() };
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		String column = this.keyValue.get(this.keysToGet[0]);
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");

		// check if packages are installed
		String[] packages = { "stringdist", "data.table" };
		this.rJavaTranslator.checkPackages(packages);

		// source script
		String bestMatchScript = "source(\"" + baseFolder + "\\R\\Recommendations\\advanced_federation_blend.r\") ; library(data.table); ";
		bestMatchScript = bestMatchScript.replace("\\", "/");
		this.rJavaTranslator.runR(bestMatchScript);

		// get single column input
		RDataTable frame = (RDataTable) getFrame();
		String frameName = frame.getTableName();
		String matchesTable = Utility.getRandomString(8);
		String col1 = matchesTable + "col1";
		String script = col1 + "<- as.character(" + frameName + "$" + column + ");";

		// run script
		this.rJavaTranslator.runR(script);
		this.rJavaTranslator.runR(matchesTable + " <- self_match(" + col1 + ");");

		// add a unique combined col1 == col2, remove extra columns,
		String combineScript = matchesTable + "$distance <- as.numeric(" + matchesTable + "$dist);" + matchesTable
				+ "<-" + matchesTable + "[,c(\"col1\",\"col2\",\"distance\")]; " + matchesTable + "<-" + matchesTable
				+ "[order(unique(" + matchesTable + ")$distance),] ;";

		this.rJavaTranslator.runR(combineScript + matchesTable + " <- as.data.table(" + matchesTable + ");");

		// remove all garbage
		this.rJavaTranslator.runR("rm(" + col1 + ")");

		RDataTable returnTable = createFrameFromVaraible(matchesTable);
		NounMetadata retNoun = new NounMetadata(returnTable, PixelDataType.FRAME);

		// get count of exact matches
		String exactMatchCount = this.rJavaTranslator
				.getString("as.character(nrow(" + matchesTable + "[" + matchesTable + "$distance == 0,]))");
		if (exactMatchCount != null) {
			int val = Integer.parseInt(exactMatchCount);
			retNoun.addAdditionalReturn(new NounMetadata(val, PixelDataType.CONST_INT));
		} else {
			throw new IllegalArgumentException("No matches found.");
		}

		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				frame, 
				"PredictSimilarColumnValues", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		
		this.insight.getVarStore().put(matchesTable, retNoun);
		return retNoun;
	}

}
