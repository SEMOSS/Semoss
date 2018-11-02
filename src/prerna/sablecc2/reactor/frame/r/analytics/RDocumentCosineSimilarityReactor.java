package prerna.sablecc2.reactor.frame.r.analytics;

import org.rosuda.REngine.Rserve.RConnection;

import prerna.ds.r.RDataTable;
import prerna.ds.r.RSyntaxHelper;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.sablecc2.reactor.frame.r.util.IRJavaTranslator;
import prerna.sablecc2.reactor.imports.ImportUtility;
import prerna.util.Utility;

public class RDocumentCosineSimilarityReactor extends AbstractRFrameReactor {

	private static final String CLASS_NAME = RNumericalCorrelationReactor.class.getName();

	public RDocumentCosineSimilarityReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.INSTANCE_KEY.getKey(), ReactorKeysEnum.DESCRIPTION.getKey(),
				ReactorKeysEnum.OVERRIDE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		init();
		RDataTable rFrame = (RDataTable) this.getFrame();
		String dataFrame = rFrame.getTableName();
		String instanceCol = this.keyValue.get(ReactorKeysEnum.INSTANCE_KEY.getKey());
		String description = this.keyValue.get(ReactorKeysEnum.DESCRIPTION.getKey());
		boolean override = overrideFrame();

		StringBuilder rsb = new StringBuilder();
		// source the r script that will run the numerical correlation routine
		String correlationScriptFilePath = getBaseFolder() + "\\R\\AnalyticsRoutineScripts\\DocumentSimilarity.R";
		correlationScriptFilePath = correlationScriptFilePath.replace("\\", "/");
		rsb.append("source(\"" + correlationScriptFilePath + "\");");

		// create temp frame with column and description
		String tempFrame = "DocSim" + Utility.getRandomString(5);
		rsb.append(tempFrame + " <-  data.frame(description=" + dataFrame + "$" + description + ", column=" + dataFrame + "$" + instanceCol + ");");
		// make columns as character
		rsb.append(tempFrame + "$column<- as.character(" + tempFrame + "$column);");
		rsb.append(tempFrame + "$description<- as.character(" + tempFrame + "$description);");

		String similarityFrame = "SimFrame" + Utility.getRandomString(5);
		rsb.append(similarityFrame + "<- getDocumentCosineSimilarity(" + tempFrame + ");");
		rsb.append(RSyntaxHelper.asDataTable(similarityFrame, similarityFrame));

		// r clean up
		rsb.append("rm(getDocumentCosineSimilarity, " + tempFrame + ");");
		this.rJavaTranslator.runR(rsb.toString());

		// create new R DataTable from results
		VarStore vars = this.insight.getVarStore();
		RDataTable newTable = null;
		if (vars.get(IRJavaTranslator.R_CONN) != null && vars.get(IRJavaTranslator.R_PORT) != null) {
			newTable = new RDataTable(similarityFrame, (RConnection) vars.get(IRJavaTranslator.R_CONN).getValue(),
					(String) vars.get(IRJavaTranslator.R_PORT).getValue());
		} else {
			newTable = new RDataTable(similarityFrame);
		}
		ImportUtility.parserRTableColumnsAndTypesToFlatTable(newTable, this.rJavaTranslator.getColumns(similarityFrame), this.getColumnTypes(similarityFrame), similarityFrame);
		NounMetadata noun = new NounMetadata(newTable, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE, PixelOperationType.FRAME_HEADERS_CHANGE);
		// replace existing frame
		if (override) {
			this.insight.setDataMaker(newTable);
		}
		// add the alias as a noun by default
		else {
			this.insight.getVarStore().put(similarityFrame, noun);
		}

		return noun;
	}

	private boolean overrideFrame() {
		GenRowStruct overrideGrs = this.store.getNoun(ReactorKeysEnum.OVERRIDE.getKey());
		if (overrideGrs != null && !overrideGrs.isEmpty()) {
			return (boolean) overrideGrs.get(0);
		}
		// default is to override
		return true;
	}
}