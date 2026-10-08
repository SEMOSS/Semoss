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
package prerna.io.connector;

import prerna.auth.AuthProvider;

/**
 * One app of a provider that the mail and calendar reactors work against, such
 * as Outlook or Gmail.
 *
 * <p>
 * The reactors of one operation share everything but the app, so this is what
 * tells them apart: whose login they need, what they call the app when they
 * describe themselves, and which provider a view URI names.
 * </p>
 */
public interface IConnectorApp {

	/**
	 * @return the provider the user signs in to for this app
	 */
	AuthProvider getAuthProvider();

	/**
	 * @return the app's name as a person knows it, such as Outlook or Gmail
	 */
	String getDisplayName();

	/**
	 * @return the provider as a view URI names it, such as microsoft or google
	 */
	String getProviderId();

	/**
	 * @return what every reactor name of the app starts with, such as
	 *         MicrosoftOutlook, so a description can name the reactor another value
	 *         comes from
	 */
	String getReactorPrefix();
}
