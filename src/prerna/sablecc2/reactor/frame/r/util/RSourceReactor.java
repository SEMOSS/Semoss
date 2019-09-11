package prerna.sablecc2.reactor.frame.r.util;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class RSourceReactor extends AbstractReactor {

	public RSourceReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.IN_APP.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String relativePath = this.keyValue.get(this.keysToGet[0]);
		
		AbstractRJavaTranslator rJavaTranslator = this.insight.getRJavaTranslator(this.getLogger(this.getClass().getName()));
		rJavaTranslator.startR(); 
		
		boolean app = (keyValue.containsKey(keysToGet[1]) && keyValue.get(keysToGet[1]).equalsIgnoreCase("app")) ;//|| (keyValue.containsKey(keysToGet[0]) && keyValue.get(keysToGet[0]).startsWith("app_assets"));
		boolean isUser = (keyValue.containsKey(keysToGet[1]) && keyValue.get(keysToGet[1]).equalsIgnoreCase("user")) ;

		String assetFolder = this.insight.getInsightFolder();
		
		if(isUser)
		{
			// do other things
		}
		if (app) {
			assetFolder = this.insight.getAppFolder();
		}

		
		String path = assetFolder + "/" + relativePath;
		path = path.replace('\\', '/');
		
		rJavaTranslator.executeEmptyRDirect("source(\"" + path + "\", " + rJavaTranslator.env + ");");
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}
}
