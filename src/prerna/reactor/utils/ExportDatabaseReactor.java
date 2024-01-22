package prerna.reactor.utils;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

@Deprecated
public class ExportDatabaseReactor extends ExportEngineReactor {

	private String keepGit = "keepGit";

	public ExportDatabaseReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DATABASE.getKey(), keepGit };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		if(databaseId != null && !databaseId.isEmpty()) {
			this.store.makeNoun(ReactorKeysEnum.ENGINE.getKey()).add(databaseId, PixelDataType.CONST_STRING);
		}
		return super.execute();
	}

}
