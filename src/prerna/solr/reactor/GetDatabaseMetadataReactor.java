package prerna.solr.reactor;

import java.util.List;
import java.util.Map;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityDatabaseUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class GetDatabaseMetadataReactor extends AbstractReactor {
	
	private static final String META_KEYS = "metakey";
	
	public GetDatabaseMetadataReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey(), META_KEYS};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		
		if(databaseId == null || databaseId.isEmpty()) {
			throw new IllegalArgumentException("Must input an database id");
		}
		
		Map<String, Object> databaseMeta = null;
		if(AbstractSecurityUtils.securityEnabled()) {
			// make sure valid id for user
			databaseId = SecurityQueryUtils.testUserDatabaseIdForAlias(this.insight.getUser(), databaseId);
			if(SecurityDatabaseUtils.userCanViewDatabase(this.insight.getUser(), databaseId)) {
				// user has access!
				databaseMeta = SecurityDatabaseUtils.getAggregateDatabaseMetadata(databaseId, getMetaKeys(), false);
			} else if(SecurityDatabaseUtils.databaseIsDiscoverable(databaseId)) {
				databaseMeta = SecurityDatabaseUtils.getAggregateDatabaseMetadata(databaseId, getMetaKeys(), false);
			} else {
				// you dont have access
				throw new IllegalArgumentException("Database does not exist or user does not have access to the database");
			}
		} else {
			databaseId = MasterDatabaseUtility.testDatabaseIdIfAlias(databaseId);
			// just grab the info
			databaseMeta = SecurityDatabaseUtils.getAggregateDatabaseMetadata(databaseId, getMetaKeys(), false);
		}

		return new NounMetadata(databaseMeta, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DATABASE_INFO);
	}
	
	private List<String> getMetaKeys() {
		GenRowStruct grs = this.store.getNoun(META_KEYS);
		if(grs != null && !grs.isEmpty()) {
			return grs.getAllStrValues();
		}
		
		return null;
	}

}
