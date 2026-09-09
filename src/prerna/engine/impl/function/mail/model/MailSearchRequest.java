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
package prerna.engine.impl.function.mail.model;

import prerna.om.Insight;

/**
 * One search, as it reaches a mailbox.
 *
 * <p>
 * Everything here has already been checked: the folder is one the engine
 * allows, and the limit has been held down to the most it will return. An
 * implementation can act on these without asking whether it should, which is
 * the point of assembling the request at the engine boundary rather than
 * passing the caller's own parameters through.
 *
 * @param folder              the folder to read
 * @param criteria            what to match on
 * @param limit               the most messages to return
 * @param includeBody         whether the body text comes back with each message
 * @param downloadAttachments whether attachments are written into the insight
 *                            rather than only described
 * @param insight             the insight this call is running under, or null
 *                            when there is none, in which case nothing can be
 *                            written
 */
public record MailSearchRequest(String folder, MailSearchCriteria criteria, int limit, boolean includeBody,
		boolean downloadAttachments, Insight insight) {
}
