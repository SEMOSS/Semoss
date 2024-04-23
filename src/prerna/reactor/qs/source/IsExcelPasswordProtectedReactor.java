package prerna.reactor.qs.source;

import prerna.poi.main.helper.excel.ExcelUtility;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.UploadInputUtility;
import prerna.util.Utility;

public class IsExcelPasswordProtectedReactor extends AbstractReactor {

	public IsExcelPasswordProtectedReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String fileLocation = Utility.normalizePath(UploadInputUtility.getFilePath(this.store, this.insight));
		boolean isEncrypted = ExcelUtility.isExcelEncrypted(fileLocation);
		return new NounMetadata(isEncrypted, PixelDataType.BOOLEAN);
	}

}
