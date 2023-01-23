package prerna.sablecc2.reactor.frame;

import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.py.PandasFrame;
import prerna.ds.py.PandasSyntaxHelper;
import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.util.Utility;

public class FrameFilterWithSQLReactor extends AbstractFrameReactor {

	private static final String CLASS_NAME = ResetFrameToOriginalNameReactor.class.getName();

	public FrameFilterWithSQLReactor() {
		this.keysToGet = new String[]{ ReactorKeysEnum.FRAME.getKey(), ReactorKeysEnum.QUERY_KEY.getKey() } ;
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		organizeKeys();
		ITableDataFrame frame = getFrameDefaultLast();
		
		if(!(frame instanceof PandasFrame) && !(frame instanceof RDataTable)) {
			return NounMetadata.getErrorNounMessage("This mehtod has only been implemneted for python and r. Please convert your frame type and try again");
		}
		
		String query = Utility.decodeURIComponent(this.keyValue.get(this.keysToGet[1]));
		String newFrameName = Utility.getRandomString(6);
		
		if(frame instanceof PandasFrame) {
			String frameMaker = newFrameName + "= pd.DataFrame(sqldf(\"" + query.replace("\"", "\\\"") + "\"))";
			logger.info("Creating frame with query..  " + query + " <<>> " + frameMaker);
			insight.getPyTranslator().runEmptyPy("from pandasql import sqldf");
			insight.getPyTranslator().runScript(frameMaker); 
			// need to make the wrapper in this instance
			insight.getPyTranslator().runScript(PandasSyntaxHelper.makeWrapper(
					PandasSyntaxHelper.createFrameWrapperName(newFrameName), newFrameName));
		} else if(frame instanceof RDataTable){
			AbstractRJavaTranslator rt = insight.getRJavaTranslator(this.getClass().getName());
			String frameMaker = newFrameName + " <- as.data.table(sqldf(\"" + query.replace("\"", "\\\"") + "\"))";
			logger.info("Creating frame with query..  " + query + " <<>> " + frameMaker);
			rt.runRAndReturnOutput("library(sqldf)");
			rt.runR(frameMaker); // load the sql df			
		}
		
		// reset the name back to the original name
		frame.setName(newFrameName);
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}
}
