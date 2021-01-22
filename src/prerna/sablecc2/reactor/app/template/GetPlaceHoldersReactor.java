package prerna.sablecc2.reactor.app.template;

import java.util.List;
import java.util.Map;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

/** This reactor will fetch the place holder information from the selected template file 
 *  place holder is a bookmark tagged to a cell with a name, default value and cell value. 
 *  intent is to dynamically place the content
 * @author kprasannakumar
 *
 */
public class GetPlaceHoldersReactor extends AbstractReactor {

	public GetPlaceHoldersReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.TEMPLATE_NAME.getKey() };
	}

	public NounMetadata execute() {
		organizeKeys();

		String appId = this.keyValue.get(ReactorKeysEnum.APP.getKey());
		String templateName = this.keyValue.get(ReactorKeysEnum.TEMPLATE_NAME.getKey());
		Map<String, List<String>> placeHoldersMap = TemplateUtility.getPlaceHolderInfo(appId, templateName);

		// returns the complete place holder data with key as placeholder label name 
		// and values containing place holder default value, cell position
		return new NounMetadata(placeHoldersMap, PixelDataType.MAP);
	}

}
