package prerna.sablecc2.reactor.app.template;

import java.util.Map;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

/** This reactor will fetch the template information of the app from the corresponding template property file. 
 * @author kprasannakumar
 *
 */
public class GetTemplateList extends AbstractReactor {

	public GetTemplateList() {
		this.keysToGet = new String[] { ReactorKeysEnum.APP.getKey() };
	}

	public NounMetadata execute() {
		organizeKeys();
		String appId = this.keyValue.get(ReactorKeysEnum.APP.getKey());
		Map<String, String> templateDataMap = TemplateUtility.getTemplateList(appId);

		// templateDataMap will contain all the template information with template name as key 
		// and file name as the value for the corresponding app
		return new NounMetadata(templateDataMap, PixelDataType.MAP);
	}
}
