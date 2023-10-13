package prerna.reactor.utils;

import java.util.Map;

import prerna.algorithm.api.AdditionalDataType;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AdditionalTypesHelpReactor extends AbstractReactor  {
	
	/**
	 * This reactor allows the user to view the names of all additional types
	 * There are no inputs to the reactor
	 */

	private static String enumDescriptionsString = null;

	@Override
	public NounMetadata execute() {
		if(enumDescriptionsString == null) {
			Map<AdditionalDataType, String> mapOfEnumDescriptions = AdditionalDataType.getHelp();
			StringBuilder allDescriptions = new StringBuilder("Additional Types:\n");
			mapOfEnumDescriptions.forEach((adtlType, description) -> {
				allDescriptions.append("Name: ").append(adtlType).append(" | ").append("Description: ").append(description).append(";\n");
			});
	
			enumDescriptionsString = allDescriptions.toString();
		}
		return new NounMetadata(enumDescriptionsString, PixelDataType.CONST_STRING, PixelOperationType.HELP);
	}
}
