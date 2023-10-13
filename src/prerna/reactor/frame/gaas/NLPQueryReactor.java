package prerna.reactor.frame.gaas;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.py.PandasFrame;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.DIHelper;
import prerna.util.Settings;
import prerna.util.Utility;

public class NLPQueryReactor extends AbstractReactor {

	// get a NLP Text
	// starts the environment / sets the model
	// convert text to sql through pipeline
	// plug the pipeline into insight
	
	//
	private static final Logger logger = LogManager.getLogger(NLPQueryReactor.class);

	public NLPQueryReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.COMMAND.getKey(), ReactorKeysEnum.MODEL.getKey()};
	}

	@Override
	public NounMetadata execute() {
		
		organizeKeys();
		String query = keyValue.get(keysToGet[0]);
		
		String model = "tscholak/cxmefzzi";
		// will handle huggingface model
		if(keyValue.containsKey(keysToGet[1]))
			model = keyValue.get(keysToGet[1]);

		if(DIHelper.getInstance().getProperty("HF_SEQ_2_SQL") != null) // force the model
			model = DIHelper.getInstance().getProperty("HF_SEQ_2_SQL");
		
		String modelVarName = Utility.cleanString(model, true);
		modelVarName = modelVarName.replaceAll("-", "_");

		String pipeVar = modelVarName; // setting up so that I dont need to load same model multiple times during the same session
		
		boolean modelLoaded = (Boolean)this.insight.getPyTranslator().runScript("'" + modelVarName +"' in globals()");
		
		if(!modelLoaded) // load the model
		{
			// load the model
			//pipeVar = Utility.getRandomString(5);
			String cacheFolder = DIHelper.getInstance().getProperty(Settings.HF_CACHE_DIR);
			if(cacheFolder == null)
				return NounMetadata.getErrorNounMessage("Hugging Face Cache is not set. Please set it RDF Map HF_CACHE_DIR");
			this.insight.getPyTranslator().runScript(modelVarName + " = smssutil.load_hugging_face_model('" + model + "', 'text2text-generation', '" + cacheFolder + "')");
			logger.info("Loaded the model as " + pipeVar);
		}
		
		// may be we should get all the frames here
		Set <ITableDataFrame> allFrames = this.insight.getVarStore().getFrames();
		
		// iterate through each of the frame
		// get the columns
		// get the dictionary data for these columns
		// create the string
		// set into the frames
		// format - [question] | [db_id] | [table] : [column] ( [content] , [content] ) , [column] ( ... ) , [...] | [table] : ... | ...
		// example - concert_singer | stadium : stadium_id, location, name, capacity, highest, lowest, average | singer : singer_id, name, country, 
		
		StringBuffer finalDbString = new StringBuffer("db");
		
		Iterator <ITableDataFrame> frameIterator = allFrames.iterator();
		while(frameIterator.hasNext())
		{
			ITableDataFrame thisFrame = frameIterator.next();
			logger.info("Processing frame " + thisFrame.getName());
			HashMap columnValues = new HashMap();
			if(thisFrame instanceof PandasFrame)
			{
				Object output = this.insight.getPyTranslator().runScript(thisFrame.getName() + "w.get_categorical_values()");
				if(output instanceof HashMap)
					columnValues = (HashMap)this.insight.getPyTranslator().runScript(thisFrame.getName() + "w.get_categorical_values()");
			}
			
			finalDbString.append(" | ").append(thisFrame.getName()).append(" : ");
			String [] columns = thisFrame.getColumnHeaders();
			
			// if the frame is pandas frame get the data
			// we will get to this shortly
			for(int columnIndex = 0;columnIndex < columns.length;columnIndex++)
			{
				if(columnIndex == 0)
					finalDbString.append(columns[columnIndex]);
				else
					finalDbString.append(" , ").append(columns[columnIndex]);
				//if(columnValues.containsKey(columns[columnIndex]))
				//	finalDbString.append(columnValues.get(columns[columnIndex]));
			}
		}
		
		String pipeQuery = query + " | " + finalDbString;
		logger.info("executing query " + pipeQuery);
		Object output = insight.getPyTranslator().runScript(pipeVar + "(\"" + pipeQuery + "\")");
		StringBuffer outputString = new StringBuffer();
		if(output instanceof ArrayList)
		{
			ArrayList thisList = (ArrayList)output;
			for(int listIndex = 0;listIndex < thisList.size();listIndex++)
			{
				String element = thisList.get(listIndex) + "";
				logger.info(element);
				outputString.append(element);
			}
		}
		// get the string
		// make a frame
		// load the frame into insight
		logger.info("Output query is " + outputString);
		String sqlDFQuery = outputString.toString();
		sqlDFQuery = sqlDFQuery.split("\\|")[1];
		sqlDFQuery = sqlDFQuery.substring(0, sqlDFQuery.length() -1);

		// execute sqlDF to create a frame
		// need to check if the query is right and then feed this into sqldf
		String frameName = Utility.getRandomString(5);
		String frameMaker = frameName + "= pd.DataFrame(sqldf('" + sqlDFQuery + "'))";
		logger.info("Creating frame with query..  " + sqlDFQuery + " <<>> " + frameMaker);
		insight.getPyTranslator().runEmptyPy("from pandasql import sqldf");
		insight.getPyTranslator().runScript(frameMaker); // load the sql df
		
		// check to see if the variable was created
		// if not this is a bad query
		boolean frameCreated = (Boolean)insight.getPyTranslator().runScript("'" + frameName + "' in globals()");
		
		List<NounMetadata> outputs = new Vector<NounMetadata>(4);

				
		if(frameCreated)
		{
			// now we just need to tell the user here is the frame
			String frameType = "Py";
			
			outputs.add(new NounMetadata("Query Generated : " + sqlDFQuery + " Data : " + frameName, PixelDataType.CONST_STRING));
			outputs.add(new NounMetadata(this.insight.getPyTranslator().runSingle(insight.getUser().getVarMap(), frameName + ".head(20)", this.insight), PixelDataType.CONST_STRING));
			outputs.add(new NounMetadata("To start working with this frame  GenerateFrameFrom" + frameType + "Variable('" + frameName + "')", PixelDataType.CONST_STRING));
			
			return new NounMetadata(outputs, PixelDataType.CODE, PixelOperationType.CODE_EXECUTION);
		}
		else
		{
			return new NounMetadata("Could not compute the result / query invalid -- " + sqlDFQuery, PixelDataType.CONST_STRING);
		}
	}

}
