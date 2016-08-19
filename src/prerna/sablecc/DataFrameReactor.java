package prerna.sablecc;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.util.Utility;

public class DataFrameReactor extends AbstractReactor {
	
	Map<String, String> dfTranslation = new HashMap<String, String>();
	
	public DataFrameReactor()
	{
		String [] thisReacts = {PKQLEnum.WORD_OR_NUM};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.DATA_FRAME;
		
		// This might be broken out to a prop file
		// The key in this map is what is in the PKQL
		// The value in this map is looked up on RDF Map to get the class
		dfTranslation.put("GRID", "H2Frame");
		dfTranslation.put("GRAPH", "TinkerFrame");
		dfTranslation.put("SPARK", "SparkDataFrame");
		dfTranslation.put("NATIVE", "NativeFrame");
	}

	@Override
	public Iterator process() {
		// All we do here is get the passed name, translate to RDF Map name and instantiate the new frame through utility
		System.out.println("My Store on Data Frame Reactor " + myStore);
		if(myStore.containsKey(PKQLEnum.WORD_OR_NUM))
		{
			String newDfName = ((Vector)myStore.get(PKQLEnum.WORD_OR_NUM)).get(0).toString();

			System.out.println("Creating new datamaker with type : " + newDfName);
			String translatedDf = dfTranslation.get(newDfName.toUpperCase());
			if(translatedDf == null){
				translatedDf = newDfName;
			}

			System.out.println("DataFrameReactor translates this name to  : " + translatedDf);
			ITableDataFrame oldFrame = (ITableDataFrame) myStore.get("G");
			
			ITableDataFrame newFrame = (ITableDataFrame) Utility.getDataMaker(null, translatedDf);
			// set the old id into the new frame
			newFrame.setUserId(oldFrame.getUserId());
			
			myStore.put(PKQLEnum.G, newFrame);
		}
		return null;
	}

}
