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
package prerna.reactor.mgmt;

import java.util.HashSet;
import java.util.Set;

import prerna.auth.User;
import prerna.om.ClientProcessWrapper;
import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetUserMemory extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		User user = requireUser();

		Set<Long> pids = new HashSet<>();
		if (user.getrProcess() != null) {
			int rPid = MgmtUtil.getPidByPort(user.getrPort());
			if (rPid > 0) {
				pids.add((long) rPid);
			}
		}
		addProcessTree(pids, user.getPythonClientProcessWrapper());
		addProcessTree(pids, user.getJavaClientProcessWrapper());
		addProcessTree(pids, user.getNodeClientProcessWrapper());

		long memory = 0;
		for (long pid : pids) {
			memory += MgmtUtil.memoryUtilizationPerProcess((int) pid);
		}
		return new NounMetadata("Memory : " + (memory / (1024 * 1024)) + " MB", PixelDataType.CONST_STRING);
	}

	/**
	 * Adds the process a wrapper started, and every process under it, to the set of
	 * process ids to measure.
	 *
	 * @param pids the process ids collected so far
	 * @param cpw  the wrapper whose process tree to add, or null
	 */
	private static void addProcessTree(Set<Long> pids, ClientProcessWrapper cpw) {
		if (cpw == null || cpw.getProcess() == null || !cpw.getProcess().isAlive()) {
			return;
		}
		ProcessHandle handle = cpw.getProcess().toHandle();
		pids.add(handle.pid());
		handle.descendants().forEach(child -> pids.add(child.pid()));
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition()
				.description("Returns the memory, in MB, used by the signed in user's R, Python, Java worker and "
						+ "node processes")
				.noKeys().output(String.class, "The memory in use, as Memory : <n> MB");
	}
}
