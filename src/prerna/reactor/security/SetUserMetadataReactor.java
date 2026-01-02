package prerna.reactor.security;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import prerna.auth.utils.SecurityUserUtils;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class SetUserMetadataReactor extends AbstractSetMetadataReactor {

	public SetUserMetadataReactor() {
		this.keysToGet = new String[] { META, ReactorKeysEnum.ENCODED.getKey() };
		this.keyRequired = new int[] { 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		Map<String, Collection<String>> metadata = getMetadata();
		// check for invalid metakeys
		List<String> validMetakeys = SecurityUserUtils.getAllMetakeys();
		if (!validMetakeys.containsAll(metadata.keySet())) {
			throw new IllegalArgumentException("Unallowed metakeys. Can only use: " + String.join(", ", validMetakeys));
		}

		SecurityUserUtils.updateUserMetadata(this.insight.getUser(), metadata);
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully set the new metadata values"));
		return noun;
	}

	@Override
	public String getReactorDescription() {
		return "Define metadata on a user";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(META)) {
			return "Map containing {'metaKey':['value1','value2', etc.]} containing the list of metadata values to define on the user. The list of values will determine the order that is defined for field";
		}
		return super.getDescriptionForKey(key);
	}

	protected Map<String, Collection<String>> getMetadata() {
		Boolean encoded = Boolean.parseBoolean(this.keyValue.get(ReactorKeysEnum.ENCODED.getKey()) + "");
		GenRowStruct metaGrs = this.store.getGenRowStruct(META);
		if (encoded) {
			if (metaGrs != null && !metaGrs.isEmpty()) {
				List<NounMetadata> encodedStrInputs = metaGrs.getNounsOfType(PixelDataType.CONST_STRING);
				if (encodedStrInputs != null && !encodedStrInputs.isEmpty()) {
					String encodedStr = (String) encodedStrInputs.get(0).getValue();
					String decodedStr = Utility.decodeURIComponent(encodedStr);
					return GSON.fromJson(decodedStr, Map.class);
				}
			}

			List<NounMetadata> encodedStrInputs = this.curRow.getNounsOfType(PixelDataType.CONST_STRING);
			if (encodedStrInputs != null && !encodedStrInputs.isEmpty()) {
				String encodedStr = (String) encodedStrInputs.get(0).getValue();
				String decodedStr = Utility.decodeURIComponent(encodedStr);
				return GSON.fromJson(decodedStr, Map.class);
			}
		} else {
			if (metaGrs != null && !metaGrs.isEmpty()) {
				List<NounMetadata> mapInputs = metaGrs.getNounsOfType(PixelDataType.MAP);
				if (mapInputs != null && !mapInputs.isEmpty()) {
					return (Map<String, Collection<String>>) mapInputs.get(0).getValue();
				}
			}

			List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
			if (mapInputs != null && !mapInputs.isEmpty()) {
				return (Map<String, Collection<String>>) mapInputs.get(0).getValue();
			}
		}

		throw new IllegalArgumentException("Must define a metadata map");
	}

}
