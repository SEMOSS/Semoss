package prerna.sablecc2.reactor.app.template;

import java.util.Map;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

/** This reactor will add update the existing template information in the app corresponding template property file. 
 * @author kprasannakumar
 *
 */
public class UpdateTemplateReactor extends AbstractReactor {

	public UpdateTemplateReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.TEMPLATE_NAME.getKey(),
				ReactorKeysEnum.TEMPLATE_FILE.getKey() };
	}

	public NounMetadata execute() {
		organizeKeys();
		String appId = this.keyValue.get(ReactorKeysEnum.APP.getKey());
		String templateFile = this.keyValue.get(ReactorKeysEnum.TEMPLATE_FILE.getKey());
		String templateName = this.keyValue.get(ReactorKeysEnum.TEMPLATE_NAME.getKey());
		
		Map<String, String> templateDataMap = TemplateUtility.editTemplate(appId, templateFile, templateName);

		// returning back the updated template information which will contain all the template information with 
		// template name as key and file name as the value
		return new NounMetadata(templateDataMap, PixelDataType.MAP);
	}
}
