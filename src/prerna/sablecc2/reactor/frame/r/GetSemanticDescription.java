package prerna.sablecc2.reactor.frame.r;

import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class GetSemanticDescription extends AbstractRFrameReactor {

	public GetSemanticDescription() {
		this.keysToGet = new String[] { "input" };
	}

	@Override
	public NounMetadata execute() {
		// TODO Auto-generated method stub
		organizeKeys();
		init();
		String[] packages = { "WikidataR", "data.table"};
		this.rJavaTranslator.checkPackages(packages);
		String input = this.keyValue.get(this.keysToGet[0]);
		StringBuilder rsb = new StringBuilder();

		// r temp variables
		String random = Utility.getRandomString(5);
		String rFindItem = "findItem" + random;
		// resulting frame
		String rFrame = "SemanticMeaning";
		// resulting frame header names
		String url = "Url";
		String semanticMeaning = "SemanticMeaning";
		// do wiki look up
		// remove results
		rsb.append("rm(" + rFrame + ");\n");
		rsb.append("library(WikidataR);\n");
		rsb.append("library(data.table);\n");
		rsb.append(rFindItem + "<-find_item('" + input + "');\n");
		rsb.append(rFrame + "<-data.frame(Reduce('rbind',lapply(" + rFindItem
				+ ",function(x) cbind(x$url,ifelse(length(x$description)==0,NA,x$description)))));\n");
		rsb.append("if(exists('" + rFrame + "')) { \n");
		// rename columns
		rsb.append(rFrame + "<-as.data.table(" + rFrame + ");\n");
		// remove frame if empty
		rsb.append("if(nrow(SemanticMeaning) == 0) {\nrm(SemanticMeaning)\n} else {\n");
		rsb.append("colnames(" + rFrame + ") <- c('" + url + "', '" + semanticMeaning + "'); \n");
		rsb.append(rFrame + "$" + url + "<-gsub('//',''," + rFrame + "$" + url + "); \n");
		rsb.append("}}\n");
		// r temp variable clean up
		rsb.append("rm(" + rFindItem + ")");

		this.rJavaTranslator.runR(rsb.toString());
		String frameExists = "exists('" + rFrame + "')";
		boolean nullResults = this.rJavaTranslator.getBoolean(frameExists);
		if (!nullResults) {
			NounMetadata noun = new NounMetadata("Unable to view your results", PixelDataType.CONST_STRING,
					PixelOperationType.ERROR);
			SemossPixelException exception = new SemossPixelException(noun);
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}

		RDataTable returnTable = createFrameFromVaraible(rFrame);
		this.insight.setDataMaker(returnTable);
		return new NounMetadata(returnTable, PixelDataType.FRAME);

	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(this.keysToGet[0])) {
			return "The input to look up description.";
		} else {
			return super.getDescriptionForKey(key);
		}
	}

}
