package prerna.sablecc2.reactor.frame.r.util;

import java.io.File;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.DIHelper;

public class RSourceReactor extends AbstractReactor {

	public RSourceReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.FILE_PATH.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String relativePath = this.keyValue.get(this.keysToGet[0]);
		String path = getBaseFolder() + "/R/" + relativePath;
		
		AbstractRJavaTranslator rJavaTranslator = this.insight.getRJavaTranslator(this.getLogger(this.getClass().getName()));
		rJavaTranslator.startR(); 
		
		File file = new File(path);
		
		// if the file is not there try in the insight
		if(!file.exists())
			path = insight.getInsightFolder() + "/" + relativePath;
		path = path.replace("\\", "/");
		
		String env = rJavaTranslator.env;
		
		rJavaTranslator.executeEmptyRDirect("source(\"" + path + "\"," + env + ");");
		
		
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

	/**
     * Get the base folder
     * @return
     */
     protected String getBaseFolder() {
          String baseFolder = null;
          try {
              baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
          } catch (Exception ignored) {
              //logger.info("No BaseFolder detected... most likely running as test...");
          }
          return baseFolder;
     }
}
