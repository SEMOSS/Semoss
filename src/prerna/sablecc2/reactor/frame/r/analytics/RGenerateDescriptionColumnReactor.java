package prerna.sablecc2.reactor.frame.r.analytics;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.AddHeaderNounMetadata;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.util.Utility;

public class RGenerateDescriptionColumnReactor extends AbstractRFrameReactor {

	private static final String CLASS_NAME = RGenerateDescriptionColumnReactor.class.getName();

	public RGenerateDescriptionColumnReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.INSTANCE_KEY.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		init();
		String[] packages = new String[] { "WikidataR" };
		this.rJavaTranslator.checkPackages(packages);
		RDataTable rFrame = (RDataTable) this.getFrame();

		String dataFrame = rFrame.getName();
		String instanceCol = this.keyValue.get(ReactorKeysEnum.INSTANCE_KEY.getKey());
		String descriptionCol = "desc" + Utility.getRandomString(8);
		String descriptionHeader = "description";
		descriptionHeader = this.getCleanNewHeader(dataFrame, descriptionHeader);
		StringBuilder rsb = new StringBuilder();
		String instances = "instances" + Utility.getRandomString(8);
		String correlationScriptFilePath = getBaseFolder() + "\\R\\AnalyticsRoutineScripts\\DescriptionColumn.R";
		correlationScriptFilePath = correlationScriptFilePath.replace("\\", "/");
		rsb.append("source(\"" + correlationScriptFilePath + "\");");
		// grab unique instances to generate descriptions
		rsb.append(instances + "<- unique(" + dataFrame + "$" + instanceCol + ");");
		rsb.append(descriptionCol + "<- generateDescriptionColumn(" + instances + ");");
		this.rJavaTranslator.runR(rsb.toString());
		String checkNull = "is.null(" + descriptionCol + ")";
		boolean nullResults = this.rJavaTranslator.getBoolean(checkNull);
		if (nullResults) {
			// throw error unable to generate results
			NounMetadata noun = new NounMetadata("Unable to obtain descriptions", PixelDataType.CONST_STRING, PixelOperationType.ERROR);
			SemossPixelException exception = new SemossPixelException(noun);
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}

		// merge frame with results
		rsb = new StringBuilder();
		String tempFrame = "tempFrame" + Utility.getRandomString(8);
		rsb.append(tempFrame + "<-data.frame(" + instanceCol + "=" + instances + "," + descriptionHeader + "=" + descriptionCol + ");");
		rsb.append(dataFrame + " <-merge(" + dataFrame + "," + tempFrame + ", by.x=\"" + instanceCol + "\", by.y=\"" + instanceCol + "\");");
		rsb.append("rm(" + descriptionCol + ", "+descriptionCol+","+tempFrame+")");
		this.rJavaTranslator.runR(rsb.toString());
		
		// now add the new header to the frame metadata
		OwlTemporalEngineMeta metaData = rFrame.getMetaData();
		metaData.addProperty(dataFrame, dataFrame + "__" + descriptionHeader);
		metaData.setAliasToProperty(dataFrame + "__" + descriptionHeader, descriptionHeader);
		metaData.setDataTypeToProperty(dataFrame + "__" + descriptionHeader, SemossDataType.STRING.toString());

		NounMetadata retNoun = new NounMetadata(rFrame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE, PixelOperationType.FRAME_DATA_CHANGE);
		retNoun.addAdditionalReturn(new AddHeaderNounMetadata(descriptionHeader));
		return retNoun;
	}
}
