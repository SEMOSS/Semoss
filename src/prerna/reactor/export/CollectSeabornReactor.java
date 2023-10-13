package prerna.reactor.export;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.io.FileUtils;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.py.PyTranslator;
import prerna.om.Insight;
import prerna.query.interpreters.PandasInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.reactor.frame.convert.ConvertReactor;
import prerna.reactor.task.TaskBuilderReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.sablecc2.om.task.ConstantDataTask;
import prerna.sablecc2.om.task.options.TaskOptions;
import prerna.util.Utility;

public class CollectSeabornReactor extends TaskBuilderReactor {

	/**
	 * This class is responsible for collecting data from a task and returning it
	 */

	//sns.relplot(data=plotterframe, x='height', y='weight', kind='scatter')
	
	private int limit = 0;
	
	public CollectSeabornReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.SPLOT.getKey(), ReactorKeysEnum.FORMAT.getKey()};
	}
	
	public NounMetadata execute() {
		organizeKeys();

		PyTranslator pyt = this.insight.getPyTranslator();
		pyt.setLogger(this.getLogger(this.getClass().getName()));
		
		String command = keyValue.get(keysToGet[0]) +"";
		
		this.task = getTask();
		String format = "png";
		
		if(keyValue.containsKey(keysToGet[1]))
			format = keyValue.get(keysToGet[1]);
		String assigner = "";
		String fileName = Utility.getRandomString(6);
		String loadDT = "";
		String adjustTypes = "";
		
		// I neeed to get the basic iterator and then get types from there
		// this is typically what we do on seaborn
		
		// import seaborn as sns
		// daplot = <Whatever the user enters>
		// daplot.savefig(location)
		// del daplot
		// del plotterframe
		// return output

		// need to do a check to see if the frame is in R if not convert to R
		SelectQueryStruct qs = ((BasicIteratorTask)task).getQueryStruct();
		ITableDataFrame thisFrame = qs.getFrame();
//		ITableDataFrame thisFrame = insight.getCurFrame();
		String type = thisFrame.getFrameType().getTypeAsString();
		
		// need to also check if it is already there
		// obviously the issue of synchronization comes but for now

		
		if(!type.equalsIgnoreCase("py")) // && !insight.getVarStore().containsKey("PY_SYNCHRONIZED"))
		{
			// move this to R
			ConvertReactor cr = new ConvertReactor();
			GenRowStruct grs = new GenRowStruct();
			grs.add(new NounMetadata(thisFrame, PixelDataType.FRAME));
			this.getNounStore().addNoun(ReactorKeysEnum.FRAME.getKey(), grs);
			grs = new GenRowStruct();
			grs.add(new NounMetadata("PY", PixelDataType.CONST_STRING));
			this.getNounStore().addNoun(ReactorKeysEnum.FRAME_TYPE.getKey(), grs);
			grs = new GenRowStruct();
			grs.add(new NounMetadata(thisFrame.getName(), PixelDataType.CONST_STRING));
			this.getNounStore().addNoun(ReactorKeysEnum.ALIAS.getKey(), grs);
			cr.setNounStore(getNounStore());
			cr.setInsight(this.insight);
			cr.execute();
			
			insight.getVarStore().put("PY_SYNCHRONIZED", new NounMetadata(true, PixelDataType.BOOLEAN));
			
			// need replace to the frame back
			insight.getVarStore().put(Insight.CUR_FRAME_KEY, new NounMetadata(thisFrame, PixelDataType.FRAME));
			
			/*
			ITableDataFrame newFrame = null;
			try {
				newFrame = FrameFactory.getFrame(this.insight, "R", frame.get);
			} catch (Exception e) {
				throw new IllegalArgumentException("Error occurred trying to create frame of type " + frameType, e);
			}
			// insert the data for the new frame
			IImporter importer = ImportFactory.getImporter(newFrame, qs, it);
			try {
				importer.insertData();
			} catch (Exception e) {
				e.printStackTrace();
				throw new SemossPixelException(e.getMessage());
			}
			*/

		}

		// I can avoid all this to make a selector
//		SelectQueryStruct qs = ((BasicIteratorTask)task).getQueryStruct();
		qs.getRelations().clear();
		
		qs = QSAliasToPhysicalConverter.getPhysicalQs(qs, thisFrame.getMetaData());
		PandasInterpreter interp = new PandasInterpreter();
		interp.setDataTableName(thisFrame.getName(), thisFrame.getName() + "w" + ".cache['data']");
		interp.setDataTypeMap(thisFrame.getMetaData().getHeaderToTypeMap());
		interp.setQueryStruct(qs);
		interp.setKeyCache(new ArrayList());

		StringBuffer columns = new StringBuffer("columns=[");
		// compose the columns string
		try {
			prerna.engine.api.IRawSelectWrapper taskItearator = (((BasicIteratorTask)(task)).getIterator());
			String [] headers = taskItearator.getHeaders();
			for(int headerIndex = 0;headerIndex < headers.length;headerIndex++)
			{
				if(headerIndex != 0)
					columns.append(",");
				columns.append("'").append(headers[headerIndex]).append("'");
			}
			columns.append("]");
		}catch(Exception ex)
		{}
		// pd.DataFrame.from_dict(mv.loc[(mv['Genre'].isin(['Family-Animation'])) ].iloc[0:][['MovieBudget', 'Genre', 'Nominated', 'RevenueDomestic']].to_dict('split'), orient='index')
		// get the composed string and turn it into a data frame
		String subDataTable = "pd.DataFrame(" + interp.composeQuery() + "['data'], " + columns + ")";

		assigner = thisFrame.getName();

		command = command.replaceAll("\\s", ""); // remove all spaces
		assigner = "plotterframe = " + subDataTable;
		if(command.contains("data=" + thisFrame.getName() + ""))
		{
			command = command.replace("data=" + thisFrame.getName() + "", "data=plotterframe");
		}
		
		String ROOT = insight.getInsightFolder();
		ROOT = ROOT.replace("\\", "/");

		String importSeaborn = "import seaborn as sns";
		String importMatPlot = "import matplotlib.pyplot as plt";
		String clearPlot = "plt.clf()";
		//assignPlotter = "plotterFrame = " + fileName;
		//String runPlot = "daplot = sns.relplot(" + splot + ")";
		// making a quick adjustment
		String runPlot =  command ;
		String seabornFile = Utility.getRandomString(6);
		String printFile = "print(saveFile)";
		String saveFileName = "saveFile = '" + ROOT + "/" + seabornFile + "." + format + "'";
		String savePlot = "plt.savefig(saveFile)";
		String removeFrame = ""; //"del(" + fileName + ")";
		String removeSeaborn = "del(sns)"; 
		String removeMatPlot = "del(plt)";
		String removeSaveFile = "del(saveFile)";
		
		seabornFile = (String)pyt.runPyAndReturnOutput(loadDT, adjustTypes, importSeaborn, importMatPlot, clearPlot, assigner, saveFileName, runPlot, savePlot, removeFrame, removeSeaborn, removeMatPlot, printFile, removeSaveFile);

		// get the insight folder
		String IF = insight.getInsightFolder();
		seabornFile = Utility.normalizePath(seabornFile.replace("$IF", IF));
		
		StringWriter sw = new StringWriter();
		try
		{
			// read the file and populate it
			byte [] bytes = FileUtils.readFileToByteArray(new File(seabornFile));
			String encodedString = Base64.getEncoder().encodeToString(bytes);
			String mimeType = "image/png";
			mimeType = Files.probeContentType(new File(seabornFile).toPath());
			sw.write("<img src='data:" + mimeType + ";base64," + encodedString + "'>");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();		
		}

		// remove the csv and the generated jpeg
		new File(seabornFile).delete();
		
		// Need to figure out if I am trying to delete the image and URI encode it at some point.. 
		ConstantDataTask cdt = new ConstantDataTask();
		
		// I need to create the options here
		Map<String, Object> outputMap = new HashMap<String, Object>();

		// need to do all the sets
		cdt.setFormat("TABLE");
		cdt.setTaskOptions(task.getTaskOptions());
		cdt.setHeaderInfo(task.getHeaderInfo());
		cdt.setSortInfo(task.getSortInfo());
		cdt.setId(task.getId());
		Map<String, Object> formatMap = new Hashtable<String, Object>();
		formatMap.put("type", "TABLE");
		cdt.setFormatMap(formatMap);
		
		outputMap.put("headers", new String[] {});
		outputMap.put("rawHeaders", new String[] {});
		outputMap.put("values", new String[]{sw.toString()});
		outputMap.put("splot", command);	
		outputMap.put("format", format);
		
		// set the output so it can give it
		cdt.setOutputData(outputMap);

		// delete the pivot later
		return new NounMetadata(cdt, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA, PixelOperationType.FILE);
	}
	
	// keeping these methods for now.. I am not sure I require them
	@Override
	protected void buildTask() throws Exception {
		// if the task was already passed in
		// we do not need to optimize/recreate the iterator
		if(this.task.isOptimized()) {
			this.task.optimizeQuery(this.limit);
		}
	}
	
	private TaskOptions genOrnamentTaskOptions() {
		if(this.subAdditionalReturn != null && this.subAdditionalReturn.size() == 1) {
			NounMetadata noun = this.subAdditionalReturn.get(0);
			if(noun.getNounType() == PixelDataType.ORNAMENT_MAP) {
				// we will use this map as task options
				TaskOptions options = new TaskOptions((Map<String, Object>) noun.getValue());
				options.setOrnament(true);
				return options;
			}
		}
		return null;
	}
	
	//returns how much do we need to collect
	private int getTotalToCollect() {
		// try the key
		GenRowStruct numGrs = store.getNoun(keysToGet[1]);
		if(numGrs != null && !numGrs.isEmpty()) {
			return ((Number) numGrs.get(0)).intValue();
		}
		
		// try the cur row
		List<Object> allNumericInputs = this.curRow.getAllNumericColumns();
		if(allNumericInputs != null && !allNumericInputs.isEmpty()) {
			return ((Number) allNumericInputs.get(0)).intValue();
		}
		
		// default to 500
		return 500;
	}
	
	@Override
	public List<NounMetadata> getOutputs() {
		List<NounMetadata> outputs = super.getOutputs();
		if(outputs != null && !outputs.isEmpty()) return outputs;
		
		outputs = new Vector<NounMetadata>();
		NounMetadata output = new NounMetadata(this.signature, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA);
		outputs.add(output);
		return outputs;
	}
	
	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.LIMIT.getKey())) {
			return "The number to collect";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
	

	// encoding
	// uses RCurl
	// txt <- base64Encode(readBin(tf1, "raw", file.info(tf1)[1, "size"]), "txt")
	// tf1 - is just the file location
	// https://stackoverflow.com/questions/33409363/convert-r-image-to-base-64/36707831?noredirect=1#comment61003382_36707831
	
	/*
	task.getMetaMap();
	SemossDataType[] sTypes = null;
	String[] headers = null;
	try {
		IRawSelectWrapper taskItearator = (((BasicIteratorTask)(task)).getIterator());
		sTypes = taskItearator.getTypes();
		headers = taskItearator.getHeaders();
	} catch (Exception e) {
		e.printStackTrace();
		throw new SemossPixelException(e.getMessage());
	}
	Map<String, SemossDataType> typeMap = new HashMap<String, SemossDataType>();
	for(int i = 0; i < headers.length; i++) {
		typeMap.put(headers[i],sTypes[i]);
	}
	// I need to see how to get this to temp
	if(headers != null) // if they are using this natively, go with it
	{
		String dir = insight.getUserFolder() + "/Temp";
		dir = dir.replaceAll("\\\\", "/");
		File tempDir = new File(dir);
		if(!tempDir.exists())
			tempDir.mkdir();
		String outputFile = dir + "/" + fileName + ".csv";
		Utility.writeResultToFile(outputFile, this.task, ",");
		// need something here to adjust the types
		// need to move this to utilities 
		// will move it once we have figured it out
		// at some point the encoding etc. needs to be fixed
		loadDT = fileName + " = pd.read_csv(\"" + outputFile + "\");";
		// adjust the types
		adjustTypes = Utility.adjustTypePy(fileName, headers, typeMap);
		// run the job
		//pyt.runEmptyPy(loadDT, adjustTypes);
		// now comes the building part
		// I need to ask kunal if he mauls the path so I cannot load seaborn anymore
		assignPlotter = "plotterframe = " + fileName;
	}
	else
	{
		// give it the full frame name
		ITableDataFrame frame = (ITableDataFrame)insight.getVarStore().get("$CUR_FRAME_KEY").getValue();
		headers = frame.getColumnHeaders();
		assignPlotter = "plotterframe = " + frame.getName();
	}*/

}
