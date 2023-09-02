package prerna.util.git.gitlab;

import java.util.List;
import java.util.Map;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class GitlabListJobsForProjectReactor extends AbstractReactor {

	public GitlabListJobsForProjectReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.HOST.getKey(), ReactorKeysEnum.GITLAB_PROJECT_ID.getKey(),
				ReactorKeysEnum.GITLAB_PRIVATE_TOKEN.getKey(), ReactorKeysEnum.GITLAB_JOB_SCOPE.getKey(),
				ReactorKeysEnum.USE_APPLICATION_CERT.getKey()
			};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String host = this.keyValue.get(ReactorKeysEnum.HOST.getKey());
		String gitProjectId = this.keyValue.get(ReactorKeysEnum.GITLAB_PROJECT_ID.getKey());
		String gitPrivateToken = this.keyValue.get(ReactorKeysEnum.GITLAB_PRIVATE_TOKEN.getKey());
		String gitJobScope = this.keyValue.get(ReactorKeysEnum.GITLAB_JOB_SCOPE.getKey());
		Boolean useApplicationCert = Boolean.parseBoolean(this.keyValue.get(ReactorKeysEnum.USE_APPLICATION_CERT.getKey()) + "");
		List<Map<String, Object>> responseData = GitlabUtility.getGitlabJobs(host, gitProjectId, null, gitPrivateToken, gitJobScope, useApplicationCert);
		return new NounMetadata(responseData, PixelDataType.VECTOR);
	}

}
