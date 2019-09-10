package prerna.sablecc2.reactor.frame.r.util;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class RSourceReactor extends AbstractReactor {

	public RSourceReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.FILE_PATH.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String relativePath = this.keyValue.get(this.keysToGet[0]);
		
		AbstractRJavaTranslator rJavaTranslator = this.insight.getRJavaTranslator(this.getLogger(this.getClass().getName()));
		rJavaTranslator.startR(); 
		
		String path = insight.getInsightFolder() + "/" + relativePath;
		path = path.replace('\\', '/');
		
		rJavaTranslator.executeEmptyR("source(\"" + path + "\");");
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}
}
