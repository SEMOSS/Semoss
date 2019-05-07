package prerna.sablecc2.reactor.algorithms;

import java.io.File;

import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class RunDocumentSummarizationReactor extends AbstractRFrameReactor {

	/**
	 * Generates pixel to dynamically create insight based on Natural Language
	 * search
	 */

	// CreateFrame ( R ) .as ( [ 'FRAME950929' ] ) | RunDocumentSummarization(desiredOutput = ["Summarize Text"], fileOrigin = ["File Path"], userInput = ["C:/Users/chrilong/Desktop/GoTAppeal.doc"], numSentences = ["5"],numTopics = ["5"],numTopicTerms=["6"])

	protected static final String CLASS_NAME = RunDocumentSummarizationReactor.class.getName();
	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	private static final String FILE_ORIGIN = "fileOrigin";
	private static final String USER_INPUT = "userInput";
	private static final String NUM_SENTENCES = "numSentences";
	private static final String NUM_TOPICS = "numTopics";
	private static final String NUM_TOPIC_TERMS = "numTopicTerms";	

	public RunDocumentSummarizationReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FRAME.getKey(), FILE_ORIGIN, USER_INPUT, NUM_SENTENCES, NUM_TOPICS, NUM_TOPIC_TERMS  };
	}

	@Override
	public NounMetadata execute() {
		// initialize reactor
		init();
		organizeKeys();
		int stepCounter = 1;
		Logger logger = this.getLogger(CLASS_NAME);
		
		// get inputs
		String fileOrigin = this.keyValue.get(FILE_ORIGIN);
		String userInput = this.keyValue.get(USER_INPUT);
		String numSentences = this.keyValue.get(NUM_SENTENCES);
		String numTopics = this.keyValue.get(NUM_TOPICS);
		String numTopicTerms = this.keyValue.get(NUM_TOPIC_TERMS);
		
		// set up frame names
		String summaryFrame = "Summary" + Utility.getRandomString(5);
		String topicsFrame = "Topics" + Utility.getRandomString(5);
		String keywordsFrame = "Keywords" + Utility.getRandomString(5);
		
		// set up the return R frame
		ITableDataFrame table = getFrame();
		if(!(table instanceof RDataTable)) {
			throw new IllegalArgumentException("Frame must be a grid to use DatabaseProfile");
		}
		RDataTable frame = (RDataTable) table;
		String frameName = frame.getName();

		// Check Packages
		logger.info(stepCounter + ". Checking R Packages and Necessary Files");
		stepCounter++;
		String[] packages = new String[] { "readtext", "xml2", "rvest", "lexRankr", "textrank", "udpipe", "textreuse",
				"stringr", "textmineR", "textreadr", "pdftools", "antiword", "dplyr" };
		this.rJavaTranslator.checkPackages(packages);		

		// start the R script
		logger.info(stepCounter + ". Building script to summarize document");
		stepCounter++;
		StringBuilder rsb = new StringBuilder();
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String wd = "wd" + Utility.getRandomString(5);
		rsb.append(frameName + " <- NULL;");
		rsb.append(wd + "<- getwd();");
		rsb.append(("setwd(\"" + baseFolder + DIR_SEPARATOR + "R" + DIR_SEPARATOR + "AnalyticsRoutineScripts\");").replace("\\", "/"));
		rsb.append("source(\"text_summary.R\");");
		
		// if file path, make sure that the file exists
		if(fileOrigin.equals("File Path")) {
			userInput = userInput.replace("\\", "/");
			//String[] accetableFormats = {"txt","doc","docx","pdf"};
			File f = new File(userInput);
			if(!f.exists()) {
				throw new IllegalArgumentException("File does not exist at that location");
			}
		}
	
		// adjust the file origin parameter to the R syntax
		fileOrigin = adjustFileOriginParameter(fileOrigin);
		
		// create the summary, topics, and keywords table
		rsb.append(summaryFrame + " <- summarize_text(" + fileOrigin + " = \"" + userInput + "\", topN = " + numSentences + ");");
		rsb.append(topicsFrame + " <- summarize_topics(" + fileOrigin + " = \"" + userInput + "\" , topTopics = " + numTopics + ", " + "topTerms = " + numTopicTerms + ");");
		rsb.append(keywordsFrame + " <- text_keywords(" + fileOrigin + " = \"" + userInput + "\");");

		// adjust the keyword and freq column naming issue
		rsb.append("setnames(" + topicsFrame + ", old=c(\"freq\",\"keyword\"), new=c(\"freq_Topics\", \"keyword_Topics\"));");
		rsb.append("setnames(" + keywordsFrame + ", old=c(\"freq\",\"keyword\"), new=c(\"freq_Keywords\", \"keyword_Keywords\"));");

		// now lets put it all into one frame for dashboard purposes
		rsb.append(frameName + "<- bind_rows(as.data.frame(" + summaryFrame + ")," + topicsFrame + ");");
		rsb.append(frameName + "<- bind_rows(" + frameName +"," + keywordsFrame + ");");
		
		// change the summary frame column
		rsb.append("setnames(" + frameName + ", old=c(\"" + summaryFrame + "\"), new=c(\"summary\"));");
		
		//reset wd and clean up
		rsb.append("setwd(" + wd + ");");
		rsb.append("rm(" + summaryFrame +"," + topicsFrame + "," + keywordsFrame + ");");
		
		// run the r script
		logger.info(stepCounter + ". Summarizing document");
		stepCounter++;
		this.rJavaTranslator.runR(rsb.toString());
		
		logger.info(stepCounter + ". Visualizing Data");
		stepCounter++;

		// now create the frame and return
		frame = createFrameFromVariable(frameName);
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE);

	}

	private String adjustFileOriginParameter(String fileOrigin) {
		if (fileOrigin.equals("File Path")) {
			fileOrigin = "filename";
		} else if (fileOrigin.equals("URL")) {
			fileOrigin = "page_url";
		} else if (fileOrigin.equals("Enter Manually")) {
			fileOrigin = "content";
		} else {
			throw new IllegalArgumentException("Improper File Origin");
		}
		return fileOrigin;
	}
}