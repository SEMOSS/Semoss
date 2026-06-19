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
package prerna.reactor.agent.exceptions;

/**
 * Thrown when the SEMOSS agent loop reaches the maximum turn cap without
 * the model producing a {@code RESPONSE_TEXT}.
 *
 * <p>Callers are responsible for recording the failure and surfacing a clear error message.
 */
public class AgentMaxTurnsException extends RuntimeException {

    private final int maxTurns;

    public AgentMaxTurnsException(int maxTurns) {
        super("Agent loop exceeded max turns (" + maxTurns
                + ") without producing a final RESPONSE_TEXT");
        this.maxTurns = maxTurns;
    }

    /** The turn cap that was reached. */
    public int getMaxTurns() {
        return maxTurns;
    }
}
