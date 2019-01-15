package prerna.sablecc2.reactor.frame.r.analytics;

import java.util.ArrayList;
import java.util.List;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.ds.r.RSyntaxHelper;
import prerna.query.interpreters.RInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
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
		String[] packages = new String[] { "lsa", "text2vec" };
		this.rJavaTranslator.checkPackages(packages);
		RDataTable rFrame = (RDataTable) this.getFrame();
		OwlTemporalEngineMeta meta = rFrame.getMetaData();
		String dataFrame = rFrame.getName();
		String instanceCol = this.keyValue.get(ReactorKeysEnum.INSTANCE_KEY.getKey());
		String description = this.keyValue.get(ReactorKeysEnum.DESCRIPTION.getKey());
		boolean override = overrideFrame();

		StringBuilder rsb = new StringBuilder();
		// check if there are filters on the frame. if so then need to run
		// algorithm on subsetted data
		String tempFrame = "DocSim" + Utility.getRandomString(5);
		if (!rFrame.getFrameFilters().isEmpty()) {
			SelectQueryStruct qs = new SelectQueryStruct();
			List<String> selectedCols = new ArrayList<String>();
			selectedCols.add(instanceCol);
			selectedCols.add(description);
			for (String s : selectedCols) {
				qs.addSelector(new QueryColumnSelector(s));
			}
			qs.setImplicitFilters(rFrame.getFrameFilters());
			qs = QSAliasToPhysicalConverter.getPhysicalQs(qs, meta);
			RInterpreter interp = new RInterpreter();
			interp.setQueryStruct(qs);
			interp.setDataTableName(dataFrame);
			interp.setColDataTypes(meta.getHeaderToTypeMap());
			String query = interp.composeQuery();
			rsb.append(tempFrame + "<- {" + query + "};");
		} else {
			rsb.append(tempFrame + "<- " + dataFrame + ";");
		}

		// source the r script that will run the numerical correlation routine
		String correlationScriptFilePath = getBaseFolder() + "\\R\\AnalyticsRoutineScripts\\DocumentSimilarity.R";
		correlationScriptFilePath = correlationScriptFilePath.replace("\\", "/");
		rsb.append("source(\"" + correlationScriptFilePath + "\");");

		// create temp frame with column and description
		rsb.append(tempFrame + " <-  data.frame(description=" + tempFrame + "$" + description + ", column=" + tempFrame
				+ "$" + instanceCol + ");");
		// make columns as character
		rsb.append(tempFrame + "$column<- as.character(" + tempFrame + "$column);");
		rsb.append(tempFrame + "$description<- as.character(" + tempFrame + "$description);");
		String similarityFrame = "SimFrame" + Utility.getRandomString(5);
		if(override) {
			similarityFrame = dataFrame;
		}
		rsb.append(similarityFrame + "<- getDocumentCosineSimilarity(" + tempFrame + ");");
		rsb.append(RSyntaxHelper.asDataTable(similarityFrame, similarityFrame));

		// r clean up
		rsb.append("rm(getDocumentCosineSimilarity, " + tempFrame + ");");
		this.rJavaTranslator.runR(rsb.toString());

		// check if similarity frame exists
		String frameExists = "exists('" + similarityFrame + "')";
		if (!this.rJavaTranslator.getBoolean(frameExists)) {
			String errorMessage = "Unable to generate document similarity";
			NounMetadata error = new NounMetadata(errorMessage, PixelDataType.CONST_STRING, PixelOperationType.ERROR);
			SemossPixelException spe = new SemossPixelException(error);
			spe.setContinueThreadOfExecution(false);
			throw spe;
		}

		// create new R DataTable from results
		RDataTable returnTable = createFrameFromVariable(similarityFrame);
		NounMetadata retNoun = new NounMetadata(returnTable, PixelDataType.FRAME);
		// replace existing frame
		if (override) {
			this.insight.setDataMaker(returnTable);
			retNoun = new NounMetadata(returnTable, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE,
					PixelOperationType.FRAME_HEADERS_CHANGE);
		}

		return retNoun;
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