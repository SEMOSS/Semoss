package prerna.sablecc2.reactor.frame.r;

import prerna.ds.r.RDataTable;
import prerna.ds.r.RSyntaxHelper;
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
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder").replace("\\", "/");

		// check if packages are installed
		String[] packages = { "stringdist" };
		this.rJavaTranslator.checkPackages(packages);

		StringBuilder rsb = new StringBuilder();
		// source script
		String bestMatchScript = "source(\"" + baseFolder + "/R/Recommendations/advanced_federation_blend.r\") ; ";
		rsb.append(bestMatchScript);

		// get single column input
		RDataTable frame = (RDataTable) getFrame();
		String frameName = frame.getName();
		String matchesTable = Utility.getRandomString(8);
		String col1 = matchesTable + "col1";

		// run script
		rsb.append(col1 + "<- as.character(" + frameName + "$" + column + ");");
		rsb.append(matchesTable + " <- self_match(" + col1 + ");");
		rsb.append(matchesTable + "<-" + matchesTable + "[order(unique(" + matchesTable + ")$distance),] ;");
//		rsb.append(RSyntaxHelper.asDataTable(matchesTable, matchesTable));
		
		// garbage collection
		rsb.append("rm(" + col1 + ")");
		this.rJavaTranslator.runR(rsb.toString());

		RDataTable returnTable = createFrameFromVariable(matchesTable);
		NounMetadata retNoun = new NounMetadata(returnTable, PixelDataType.FRAME);

		// get count of exact matches
		String exactMatchCount = this.rJavaTranslator.getString("as.character(nrow(" + matchesTable + "[" + matchesTable + "$distance == 0,]))");
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
