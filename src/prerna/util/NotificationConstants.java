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
package prerna.util;

public final class NotificationConstants {

	private NotificationConstants() {
	}

	// app catalog key
	public static final String APP_CATALOG = "APP";

	public static final class Priority {
		public static final String HIGH = "HIGH";
		public static final String MEDIUM = "MEDIUM";
		public static final String LOW = "LOW";
	}

	public static final class Type {
		public static final String USER_REQUEST = "USER_REQUEST";
		public static final String USER_ADDITION = "USER_ADDITION";
		public static final String REQUEST_APPROVAL = "REQUEST_APPROVAL";
		public static final String PERMISSION_CHANGE = "PERMISSION_CHANGE";
		public static final String REQUEST_DENIAL = "REQUEST_DENIAL";
		public static final String SMSS_UPDATE = "SMSS_UPDATE";
	}

}
