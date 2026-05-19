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
package prerna.reactor.agent.subagent;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Platform reactor that blocks until a spawned subagent finishes (or {@code timeoutSec}
 * elapses), then returns the subagent's final-text string.
 *
 * <h3>Pixel syntax</h3>
 * <pre>{@code
 * WaitSubAgent(jobId='<id>')                      -- default 300s timeout
 * WaitSubAgent(jobId='<id>', timeoutSec=60)
 * }</pre>
 *
 * <p>Returns the subagent's final-text string on success; a JSON
 * {@code {"error":"timeout","jobId":"...","timeoutSec":N}} on timeout; or a JSON
 * {@code {"error":...}} when the underlying job errored / was canceled.
 */
public class WaitSubAgentReactor extends AbstractReactor {

    private static final String TIMEOUT_KEY = "timeoutSec";

    public WaitSubAgentReactor() {
        this.keysToGet = new String[] {
                ReactorKeysEnum.JOB_ID.getKey(),  // 0 required
                TIMEOUT_KEY                        // 1 optional
        };
        this.keyRequired = new int[] { 1, 0 };
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        String jobId = this.keyValue.get(ReactorKeysEnum.JOB_ID.getKey());
        if (jobId == null || jobId.trim().isEmpty()) {
            throw new IllegalArgumentException("jobId is required for WaitSubAgent");
        }
        int timeoutSec = SubAgentDispatcher.DEFAULT_WAIT_TIMEOUT_SEC;
        String raw = this.keyValue.get(TIMEOUT_KEY);
        if (raw != null && !raw.trim().isEmpty()) {
            try {
                int parsed = Integer.parseInt(raw.trim());
                if (parsed > 0) timeoutSec = parsed;
            } catch (NumberFormatException ignored) { /* keep default */ }
        }
        return new NounMetadata(SubAgentDispatcher.wait(jobId, timeoutSec), PixelDataType.CONST_STRING);
    }

    @Override
    public String getReactorDescription() {
        return "Block until a spawned subagent completes or timeoutSec (default 300) elapses; "
                + "returns the final-text string, or a JSON error object on timeout / failure.";
    }
}
