package prerna.reactor.frame;

import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.nativeframe.NativeFrame;
import prerna.ds.py.PandasFrame;
import prerna.ds.py.PandasSyntaxHelper;
import prerna.ds.r.RDataTable;
import prerna.ds.rdbms.AbstractRdbmsFrame;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
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
		
		if(!(frame instanceof PandasFrame) && !(frame instanceof RDataTable) 
				&& !(frame instanceof NativeFrame) && !(frame instanceof AbstractRdbmsFrame)) {
			return NounMetadata.getErrorNounMessage("This mehtod has only been implemneted for python, r, grid, and native frame at this point. Please convert your frame and try again");
		}
		
		String query = Utility.decodeURIComponent(this.keyValue.get(this.keysToGet[1]));
		String newFrameName = Utility.getRandomString(6);
		String oldFrameName = frame.getName();
		
		if(frame instanceof PandasFrame) {
			// drop the old frame and old table
			// check to see if this is a new frame
			// if so construct a DataFrame and see
			PandasFrame pFrame = (PandasFrame)frame;
			String sqlite = pFrame.getSQLite();
			//pd.read_sql("select * from diab1 where age > 60", conn)
			
			query = query.replace("\"", "\\\"");
			// drop into sqlite the new name
			String frameMaker = newFrameName + " = pd.read_sql(\"" + query + "\", " + sqlite + ")";
			logger.info("Creating frame with query..  " + query + " <<>> " + frameMaker);
			insight.getPyTranslator().runSingle(insight.getUser().getVarMap(), frameMaker, this.insight); 
			// need to make the wrapper in this instance
			insight.getPyTranslator().runScript(PandasSyntaxHelper.makeWrapper(
					PandasSyntaxHelper.createFrameWrapperName(newFrameName), newFrameName));
			
			// out1.to_sql("diab1", conn, if_exists="replace", index=False)
			String addSqlTable = newFrameName + ".to_sql('" + newFrameName + "', " + sqlite + ", if_exists='replace', index=False)";
			insight.getPyTranslator().runScript(addSqlTable);

			// remove frames
			if(!oldFrameName.equalsIgnoreCase(frame.getOriginalName()))
			{
				//"SELECT name FROM sqlite_master where type='table'"
				String dropTable = sqlite + ".cursor().execute('DROP TABLE " + oldFrameName + "').fetchall()";
				String delete = "del " + oldFrameName + " ," + oldFrameName + "w";
				insight.getPyTranslator().runScript(dropTable);
				insight.getPyTranslator().runScript(delete);				
			}			
		} 
		else if(frame instanceof RDataTable){
			AbstractRJavaTranslator rt = insight.getRJavaTranslator(this.getClass().getName());
			String frameMaker = newFrameName + " <- as.data.table(sqldf(\"" + query.replace("\"", "\\\"") + "\"))";
			logger.info("Creating frame with query..  " + query + " <<>> " + frameMaker);
			rt.runRAndReturnOutput("library(sqldf)");
			rt.runR(frameMaker); // load the sql df			
			if(!oldFrameName.equalsIgnoreCase(frame.getOriginalName()))
			{
				String delete = "rm(" + oldFrameName+ ")";
				rt.runR(delete);				
			}
		}
		else if(frame instanceof AbstractRdbmsFrame){
			String sql = "CREATE TABLE " + newFrameName + " AS " + query;
			try {
				((AbstractRdbmsFrame) frame).getBuilder().runQuery(sql);
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Unable to generate new frame from sql", e);
			}
			if(!oldFrameName.equalsIgnoreCase(frame.getOriginalName()))
			{
				try {
					((AbstractRdbmsFrame) frame).getBuilder().runQuery("DROP TABLE " + oldFrameName);
				} catch (Exception e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
		else if(frame instanceof NativeFrame) {
			SelectQueryStruct qs = new SelectQueryStruct();
			qs.setCustomFrom(query);
			qs.setCustomFromAliasName(newFrameName);
			qs.setEngine( ((NativeFrame)frame).getOriginalQueryStruct().retrieveQueryStructEngine() );
			frame.setName(newFrameName);
			((NativeFrame)frame).setQueryStruct(qs);
		}
		
		// reset the name back to the original name
		frame.setName(newFrameName);
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}
}
