package prerna.reactor.project.template;

import java.util.Map;

import prerna.cluster.util.ClusterUtil;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;


/** This reactor will delete the existing template information from the app corresponding template property file. 
 * @author kprasannakumar
 *
 */
public class DeleteTemplateReactor extends AbstractReactor {
	public DeleteTemplateReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.TEMPLATE_NAME.getKey(),
				ReactorKeysEnum.TEMPLATE_FILE.getKey() };
	}

	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());
		String templateFile = this.keyValue.get(ReactorKeysEnum.TEMPLATE_FILE.getKey());
		String templateName = this.keyValue.get(ReactorKeysEnum.TEMPLATE_NAME.getKey());
		
		IProject project = Utility.getProject(projectId);
		ClusterUtil.pullProjectFolder(project, AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId));
		Map<String, String> templateDataMap = TemplateUtility.deleteTemplate(projectId, templateFile, templateName);
		ClusterUtil.pushProjectFolder(project, AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId));

		// returning back the updated template information which will contain all the template
		// information with template name as key and file name as the value
		return new NounMetadata(templateDataMap, PixelDataType.MAP);
	}

}
