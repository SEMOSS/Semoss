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
package prerna.reactor.playwright;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Acts as a container for a complete Playwright recording, encapsulating all
 * necessary information about a recorded sequence of actions. This record is an
 * immutable data carrier.
 *
 * @param version A string indicating the version of the recording format.
 * @param meta    A {@link RecordingMeta} object containing metadata about the
 *                recording (e.g., ID, title, description, timestamps).
 * @param steps   A nested map representing the actual recorded steps. The outer
 *                map's keys are {@code tabId}s, and its values are
 *                {@code List<List<PlaywrightStep>>}. This structure allows for
 *                organizing steps by tab and then by "page" (where a new inner
 *                list might represent steps on a new page load or significant
 *                page change).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record StepsEnvelope(String version, RecordingMeta meta, Map<String, List<List<PlaywrightStep>>> steps) {
}
