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
package prerna.io.connector.ms.outlook;

import prerna.auth.User;
import prerna.io.connector.ConnectorPage;
import prerna.io.connector.mail.AbstractListMailFoldersReactor;
import prerna.io.connector.mail.MailApp;
import prerna.io.connector.mail.MailFolder;
import prerna.io.connector.ms.MicrosoftLoginUtils;

/**
 * Lists the mail folders of the signed in user's own Microsoft 365 mailbox.
 *
 * <p>
 * Required delegated Microsoft Graph scope: {@code Mail.Read}.
 * </p>
 */
public class MicrosoftOutlookListMailFoldersReactor extends AbstractListMailFoldersReactor {

	@Override
	protected MailApp getMailApp() {
		return MailApp.OUTLOOK;
	}

	@Override
	protected ConnectorPage<MailFolder> listFolders(User user, int offset, int limit) throws Exception {
		// one more than the page holds, which is how a short page is told from a full
		// one with more after it
		return ConnectorPage.ofWindow(new MicrosoftOutlookMailHelper()
				.listFolders(MicrosoftLoginUtils.getValidAccessToken(user), null, limit + 1, offset).stream()
				.map(MicrosoftOutlookMessageMapper::toMailFolder).toList(), limit);
	}
}
