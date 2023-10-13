package prerna.reactor.project.template;

import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/** This reactor will fetch the template information of the app from the corresponding template property file. 
 * @author kprasannakumar
 *
 */
public class GetTemplateList extends AbstractReactor {

	public GetTemplateList() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey() };
	}

	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());
		Map<String, String> templateDataMap = TemplateUtility.getTemplateList(projectId);

		// templateDataMap will contain all the template information with template name as key 
		// and file name as the value for the corresponding app
		return new NounMetadata(templateDataMap, PixelDataType.MAP);
	}
}
