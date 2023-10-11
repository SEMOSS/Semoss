package prerna.sablecc2.reactor.qs.source;

import prerna.om.FileReference;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class FileReferenceReactor extends AbstractReactor {

	public FileReferenceReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey() };
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String filePath = this.keyValue.get(this.keysToGet[0]);
		String space = this.keyValue.get(this.keysToGet[1]);
		
		FileReference fileRef = new FileReference(filePath, space);
		return new NounMetadata(fileRef, PixelDataType.FILE_REFERENCE);
	}

}