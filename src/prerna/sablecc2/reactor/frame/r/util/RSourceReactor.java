package prerna.sablecc2.reactor.frame.r.util;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.AssetUtility;

public class RSourceReactor extends AbstractReactor {

	public RSourceReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String relativePath = this.keyValue.get(this.keysToGet[0]);

		AbstractRJavaTranslator rJavaTranslator = this.insight
				.getRJavaTranslator(this.getLogger(this.getClass().getName()));
		rJavaTranslator.startR();

		String space = this.keyValue.get(this.keysToGet[1]);
		String assetFolder = AssetUtility.getAssetBasePath(this.insight, space);

		String path = assetFolder + "/" + relativePath;
		path = path.replace('\\', '/');

		rJavaTranslator.executeEmptyRDirect("source(\"" + path + "\", " + rJavaTranslator.env + ");");
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}
}
