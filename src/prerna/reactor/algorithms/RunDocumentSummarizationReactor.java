package prerna.reactor.algorithms;

import java.io.File;

import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.r.RDataTable;
import prerna.ds.r.RSyntaxHelper;
import prerna.reactor.frame.r.AbstractRFrameReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.upload.UploadInputUtility;

public class RunDocumentSummarizationReactor extends AbstractRFrameReactor {

	/**
	 * Generates a dashboard summarizing a chosen document that is entered by the
	 * user
	 */

	// CreateFrame ( R ) .as ( [ 'FRAME950929' ] ) | RunDocumentSummarization(desiredOutput = ["Summarize Text"], fileOrigin = ["File Path"], userInput = ["C:/Users/chrilong/Desktop/GoTAppeal.doc"],numSentences = ["5"],numTopics = ["5"],numTopicTerms=["6"])

	protected static final String CLASS_NAME = RunDocumentSummarizationReactor.class.getName();
	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	private static final String FILE_ORIGIN = "fileOrigin";
	private static final String USER_INPUT = "userInput";
	private static final String NUM_SENTENCES = "numSentences";
	private static final String NUM_TOPICS = "numTopics";
	private static final String NUM_TOPIC_TERMS = "numTopicTerms";

	public RunDocumentSummarizationReactor() {
		this.keysToGet = new String[] { FILE_ORIGIN, USER_INPUT, NUM_SENTENCES,NUM_TOPICS, NUM_TOPIC_TERMS };
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
		if (!(table instanceof RDataTable)) {
			throw new IllegalArgumentException("Frame must be an R Frame for Document Summarizer");
		}
		RDataTable frame = (RDataTable) table;
		String frameName = frame.getName();
		
		// make sure that user has correct R version
		if(checkRVersion(3,4)) {
			throw new IllegalArgumentException("Document Summary requires at least R 3.4");
		}
		
		// Check Packages
		logger.info(stepCounter + ". Checking R Packages and Necessary Files");
		stepCounter++;
		System.out.println("");
		String[] packages = new String[] { "readtext", "xml2", "rvest", "lexRankr", "textrank", "udpipe", "textreuse",
				"stringr", "textmineR", "textreadr", "pdftools", "antiword", "dplyr" , "tm" };
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
		if (fileOrigin.equals("File Path")) {
			userInput = UploadInputUtility.getFilePath(this.store, this.insight, USER_INPUT);
			userInput = userInput.replace("\\", "/");
			File f = new File(Utility.normalizePath(userInput));
			if (!f.exists()) {
				throw new IllegalArgumentException("File does not exist at that location");
			}
		}

		// adjust the file origin parameter to the R syntax
		fileOrigin = adjustFileOriginParameter(fileOrigin);

		// create the summary, topics, and keywords table
		rsb.append(summaryFrame + " <- summarize_text(" + fileOrigin + " = \"" + userInput + "\", topN = " + numSentences + ");");
		rsb.append(topicsFrame + " <- summarize_topics_text(" + fileOrigin + " = \"" + userInput + "\" , topTopics = " + numTopics + ", " + "topTerms = " + numTopicTerms + ");");
		rsb.append(keywordsFrame + " <- text_keywords(" + fileOrigin + " = \"" + userInput + "\");");

		// adjust the keyword and freq column naming issue
		rsb.append("setnames(" + topicsFrame + ", old=c(\"freq\",\"keyword\", \"text\"), new=c(\"topic_frequency\", \"topic_keywords\", \"topic_summary\"));");
		rsb.append("setnames(" + keywordsFrame + ", old=c(\"freq\"), new=c(\"keyword_frequency\"));");

		// now lets put it all into one frame for dashboard purposes
		rsb.append(frameName + "<- bind_rows(as.data.frame(" + summaryFrame + ")," + topicsFrame + ");");
		rsb.append(frameName + "<- bind_rows(" + frameName + "," + keywordsFrame + ");");

		// change the summary frame column
		rsb.append("setnames(" + frameName + ", old=c(\"" + summaryFrame + "\"), new=c(\"summary\"));");
		
		// need to change the integer columns to numeric
		rsb.append(frameName + "$ngram <- as.numeric("+frameName+"$ngram);");
		rsb.append(frameName + "$keyword_frequency <- as.numeric("+frameName+"$keyword_frequency);");
		rsb.append(frameName + "$topic_frequency <- as.numeric("+frameName+"$topic_frequency);");
		
		// reset wd and clean up
		rsb.append("setwd(" + wd + ");");
		rsb.append("rm(" + summaryFrame + "," + topicsFrame + "," + keywordsFrame + ");");
		
		// make sure that script ran correctly, and throw helpful error if not
		String isError = "documentSummaryError" + Utility.getRandomString(5);
		rsb.append(isError + " <- \"\";");
		
		// run the r script
		logger.info(stepCounter + ". Summarizing document");
		stepCounter++;
		this.rJavaTranslator.runR(rsb.toString());
		this.rJavaTranslator.executeEmptyR(RSyntaxHelper.asDataTable(frameName, frameName));
		logger.info(stepCounter + ". Visualizing Data");
		stepCounter++;
		
		// make sure script ran correctly
		Boolean errorCheck = this.rJavaTranslator.getBoolean("!exists(\"" + isError + "\")");
		if(errorCheck) {
			throw new IllegalArgumentException("Document could not be summarized");
		}

		// now create the frame and return the noun
		RDataTable newTable = createNewFrameFromVariable(frameName);
		this.insight.setDataMaker(newTable);
		NounMetadata noun = new NounMetadata(newTable, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE, PixelOperationType.FRAME_HEADERS_CHANGE);
		this.insight.getVarStore().put(frameName, noun);
		return noun;
	}

	///////////////////////// ORGANIZING PARAMS /////////////////////////
	
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
	
	private boolean checkRVersion(int majorReq, int minorReq) {
		// relies on the following R format (seems like its been this format for > 6 years
		// R version 3.5.0 (2018-04-23)
		String rVersion = this.rJavaTranslator.getString("R.version.string");
		int major = Integer.parseInt(rVersion.substring(10, 11));
		int minor = Integer.parseInt(rVersion.substring(12, 13));
		if(major < 3 || (major <= majorReq && minor < minorReq)) {
			return true;
		} else {
			return false;
		}
	}

	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(FILE_ORIGIN)) {
			return "The format of the document to be summarized (file path, url, or free text)";
		} else if(key.equals(USER_INPUT)) {
			return "The actual file, whether that is a URL, file path, or free text";
		} else if (key.equals(NUM_SENTENCES)) {
			return "The number of sentences to be returned in the summary";
		} else if(key.equals(NUM_TOPICS)) {
			return "The number of major topics to be returned in the summary";
		} else if (key.equals(NUM_TOPIC_TERMS)) {
			return "The number of keywords within each major topic to be returned in the summary";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}