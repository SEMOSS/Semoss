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
package prerna.reactor.project;

import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.date.SemossDate;
import prerna.util.git.ProjectGitStatusUtils.GitCommitInfo;
import prerna.util.git.ProjectGitStatusUtils.GitFileStatus;
import prerna.util.git.ProjectGitStatusUtils.GitStatusResult;

/**
 * Shared JSON-shape builder for the {@code ProjectGitStatus} payload, reused
 * by {@link ProjectGitStatusReactor} and every mutating project git reactor
 * that embeds a refreshed status in its own response.
 */
final class ProjectGitReactorUtils {

	private ProjectGitReactorUtils() {
	}

	static Map<String, Object> buildStatusMap(String projectId, GitStatusResult status) {
		Map<String, Object> map = new LinkedHashMap<>();
		map.put("branch", status.branch);
		map.put("detached", status.detached);
		map.put("headCommitId", status.headCommitId);
		map.put("lastCommit", toLastCommitMap(status.lastCommit));
		map.put("staged", toFileList(status.staged));
		map.put("unstaged", toFileList(status.unstaged));
		map.put("untracked", toFileList(status.untracked));
		map.put("conflicted", toFileList(status.conflicted));
		map.put("clean", status.clean);

		Map<String, Object> publishMetadata = SecurityProjectUtils.getPublishMetadata(projectId);
		map.put("publishedCommitId", publishMetadata.get("publishedCommitId"));
		map.put("publishedBy", publishMetadata.get("publishedBy"));
		Object publishedAt = publishMetadata.get("publishedAt");
		map.put("publishedAt", publishedAt instanceof SemossDate
				? DateTimeFormatter.ISO_INSTANT.format(((SemossDate) publishedAt).getDate().toInstant())
				: null);

		return map;
	}

	private static Map<String, Object> toLastCommitMap(GitCommitInfo lastCommit) {
		if (lastCommit == null) {
			return null;
		}
		Map<String, Object> map = new LinkedHashMap<>();
		map.put("commitId", lastCommit.commitId);
		map.put("message", lastCommit.message);
		map.put("author", lastCommit.author);
		map.put("authorEmail", lastCommit.authorEmail);
		map.put("date", lastCommit.date);
		return map;
	}

	private static List<Map<String, Object>> toFileList(List<GitFileStatus> files) {
		List<Map<String, Object>> list = new ArrayList<>();
		for (GitFileStatus file : files) {
			Map<String, Object> map = new LinkedHashMap<>();
			map.put("path", file.path);
			map.put("status", file.status);
			list.add(map);
		}
		return list;
	}
}
