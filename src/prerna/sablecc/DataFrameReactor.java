package prerna.sablecc;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import prerna.sablecc.meta.DataframeMetadata;
import prerna.sablecc.meta.IPkqlMetadata;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.util.Utility;

public class DataFrameReactor extends AbstractReactor {
	
	public static final String DATA_FRAME_TYPE = "dataFrameType";
	
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
		dfTranslation.put("RFRAME", "RDataTable");
		dfTranslation.put("MSSQLSERVER", "SqlServerFrame");
		dfTranslation.put("DASHBOARD", "Dashboard");
	}

	@Override
	public Iterator process() {
		// All we do here is get the passed name, translate to RDF Map name and instantiate the new frame through utility
		System.out.println("My Store on Data Frame Reactor " + myStore);
		if(myStore.containsKey(DATA_FRAME_TYPE)) {
			String newDfName = (String) myStore.get(DATA_FRAME_TYPE);

			System.out.println("Creating new datamaker with type : " + newDfName);
			String translatedDf = dfTranslation.get(newDfName.toUpperCase());
			if(translatedDf == null){
				translatedDf = newDfName;
			}

			System.out.println("DataFrameReactor translates this name to  : " + translatedDf);
			IDataMaker oldFrame = (IDataMaker) myStore.get("G");
			
			IDataMaker newFrame = (IDataMaker) Utility.getDataMaker(null, translatedDf);
			// set the old id into the new frame
			if(oldFrame != null) {
				newFrame.setUserId(oldFrame.getUserId());
			}
			myStore.put(PKQLEnum.G, newFrame);
		}
		
		return null;
	}

	public IPkqlMetadata getPkqlMetadata() {
		DataframeMetadata metadata = new DataframeMetadata();
		metadata.setPkqlStr((String) myStore.get(PKQLEnum.DATA_FRAME));
		metadata.setFrameType((String) myStore.get(DATA_FRAME_TYPE));
		return metadata;
	}
}
