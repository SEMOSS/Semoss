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

public class PredictSimilarColumnValuesReactor extends AbstractFrameReactor {

	public PredictSimilarColumnValuesReactor() {
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

		frame.runScript(matchesTable + " = " + frameName + "w.match('" + column + "', '" + column + "')");
		
		
		
		PandasFrame returnTable = new PandasFrame(matchesTable);
		returnTable.setJep(frame.getJep());
		recreateMetadata(returnTable);
		
		NounMetadata retNoun = new NounMetadata(returnTable, PixelDataType.FRAME);

		// get count of exact matches
		String exactMatchCount = returnTable.runScript("len(" + matchesTable + "[" + matchesTable + "['distance'] == 100])") + "";
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
	
////////////////////////Recreation methods //////////////////////////////
	
	
	
private void recreateMetadata(PandasFrame frame)
{

String frameName = frame.getName();

String[] colNames = getColumns(frame);

//I bet this is being done for pixel.. I will keep the same
frame.runScript(PandasSyntaxHelper.cleanFrameHeaders(frameName, colNames));
colNames = getColumns(frame);

String[] colTypes = getColumnTypes(frame);

if(colNames == null || colTypes == null) {
throw new IllegalArgumentException("Please make sure the variable " + frameName + " exists and can be a valid data.table object");
}

//create the pandas frame
//and set up teverything else


ImportUtility.parsePyTableColumnsAndTypesToFlatTable(frame, colNames, colTypes, frameName);

}


public String [] getColumns(PandasFrame frame)
{

String frameName = frame.getName();
//get jep thread and get the names
ArrayList <String> val = (ArrayList<String>)frame.runScript("list(" + frameName + ")");
String [] retString = new String[val.size()];

val.toArray(retString);

return retString;


}

public String [] getColumnTypes(PandasFrame frame)
{
String frameName = frame.getName();

//get jep thread and get the names
ArrayList <String> val = (ArrayList<String>)frame.runScript(PandasSyntaxHelper.getTypes(frameName));
String [] retString = new String[val.size()];

val.toArray(retString);

return retString;
}

}
