/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.util.git.gitlab;

import java.io.File;
import org.apache.commons.io.FilenameUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GitlabPullJobArtifactByIdReactor extends AbstractReactor {

  public GitlabPullJobArtifactByIdReactor() {
    this.keysToGet =
        new String[] {
          ReactorKeysEnum.HOST.getKey(),
          ReactorKeysEnum.GITLAB_PROJECT_ID.getKey(),
          ReactorKeysEnum.GITLAB_JOB_ID.getKey(),
          ReactorKeysEnum.GITLAB_PRIVATE_TOKEN.getKey(),
          ReactorKeysEnum.USE_APPLICATION_CERT.getKey()
        };
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    String host = this.keyValue.get(ReactorKeysEnum.HOST.getKey());
    String gitProjectId = this.keyValue.get(ReactorKeysEnum.GITLAB_PROJECT_ID.getKey());
    String gitJobId = this.keyValue.get(ReactorKeysEnum.GITLAB_JOB_ID.getKey());
    String gitPrivateToken = this.keyValue.get(ReactorKeysEnum.GITLAB_PRIVATE_TOKEN.getKey());
    Boolean useApplicationCert =
        Boolean.parseBoolean(this.keyValue.get(ReactorKeysEnum.USE_APPLICATION_CERT.getKey()) + "");

    String saveFilePath = this.insight.getInsightFolder();
    File artifact =
        GitlabUtility.pullJobArtifact(
            host,
            gitProjectId,
            gitJobId,
            null,
            gitPrivateToken,
            useApplicationCert,
            saveFilePath,
            null);

    String artifactFilePath = artifact.getAbsolutePath();
    String artifactFileName = FilenameUtils.getName(artifactFilePath);
    return new NounMetadata(artifactFileName, PixelDataType.CONST_STRING);
  }

  @Override
  public String getReactorDescription() {
    return "This reactor pulls the artifact for a specific GitLab project job execution";
  }
}
