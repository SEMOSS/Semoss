package prerna.sablecc2.reactor.frame.py;

import java.util.ArrayList;

import prerna.ds.py.PandasFrame;
import prerna.ds.py.PandasSyntaxHelper;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.AbstractFrameReactor;
import prerna.sablecc2.reactor.imports.ImportUtility;
import prerna.util.Utility;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class MatchColumnValuesReactor extends AbstractFramePyReactor {

	public MatchColumnValuesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMN.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String column = this.keyValue.get(this.keysToGet[0]);

		// get single column input
		PandasFrame frame = (PandasFrame) getFrame();
		String frameName = frame.getName();
		String matchesTable = Utility.getRandomString(8);

		frame.runScript(matchesTable + " = " + frameName + ".match('" + column
				+ "', '" + column + "')");

		PandasFrame returnTable = new PandasFrame(matchesTable+"w");
		returnTable.setJep(frame.getJep());
		String makeWrapper = matchesTable+"w = PyFrame.makefm(" + matchesTable+")";
		returnTable.runScript(makeWrapper);
		returnTable = (PandasFrame) recreateMetadata(returnTable, false);

		NounMetadata retNoun = new NounMetadata(returnTable,
				PixelDataType.FRAME);

		// get count of exact matches
		String exactMatchCount = returnTable.runScript("len(" + matchesTable
				+ "[" + matchesTable + "['distance'] == 100])")
				+ "";
		if (exactMatchCount != null) {
			int val = Integer.parseInt(exactMatchCount);
			retNoun.addAdditionalReturn(new NounMetadata(val,
					PixelDataType.CONST_INT));
		} else {
			throw new IllegalArgumentException("No matches found.");
		}

		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight,
				frame,
				"PredictSimilarColumnValues",
				AnalyticsTrackerHelper
						.getHashInputs(this.store, this.keysToGet));

		this.insight.getVarStore().put(matchesTable, retNoun);
		return retNoun;
	}
}
