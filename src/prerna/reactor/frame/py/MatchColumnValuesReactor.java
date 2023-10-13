package prerna.reactor.frame.py;

import prerna.ds.py.PandasFrame;
import prerna.ds.py.PandasSyntaxHelper;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class MatchColumnValuesReactor extends AbstractPyFrameReactor {

	public MatchColumnValuesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMN.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String column = this.keyValue.get(this.keysToGet[0]);

		// get single column input
		PandasFrame frame = (PandasFrame) getFrame();
		String wrapperName = frame.getWrapperName();
		
		String matchesTable = Utility.getRandomString(8);
		String script = matchesTable + " = " + wrapperName + ".self_match('" + column + "')";
		insight.getPyTranslator().runEmptyPy(script);
		this.addExecutedCode(script);
		
		PandasFrame returnTable = new PandasFrame(matchesTable);
		returnTable.setTranslator(this.insight.getPyTranslator());
		returnTable.setJep(frame.getJep());
		returnTable.getTranslator().runEmptyPy(PandasSyntaxHelper.makeWrapper(returnTable.getWrapperName(), matchesTable));
		returnTable = (PandasFrame) recreateMetadata(returnTable, false);

		NounMetadata retNoun = new NounMetadata(returnTable, PixelDataType.FRAME);

		// get count of exact matches
		Long exactMatchCount = (Long) returnTable.runScript("len(" + matchesTable + "[" + matchesTable + "['distance'] == 100])");
		if (exactMatchCount != null) {
			retNoun.addAdditionalReturn(new NounMetadata(exactMatchCount, PixelDataType.CONST_INT));
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
