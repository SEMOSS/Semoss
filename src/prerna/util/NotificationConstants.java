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

import java.util.Set;

public final class NotificationConstants {

	private NotificationConstants() {
	}

	// app catalog key
	public static final String APP_CATALOG = "APP";

	public static final class Priority {
		public static final String URGENT = "URGENT";
		public static final String HIGH = "HIGH";
		public static final String NORMAL = "NORMAL";
		public static final String MEDIUM = "MEDIUM";
		public static final String LOW = "LOW";

		private static final Set<String> VALUES = Set.of(URGENT, HIGH, NORMAL, LOW);

		public static boolean isValid(String priority) {
			return VALUES.contains(priority);
		}
	}

	// In-app render surface; external channels are modeled separately in Phase 5.
	public static final class DisplaySurface {
		public static final String BELL = "BELL";
		public static final String MODAL = "MODAL";
		public static final String TOAST = "TOAST";
		public static final String BANNER = "BANNER";

		private static final Set<String> VALUES = Set.of(BELL, MODAL, TOAST, BANNER);

		public static boolean isValid(String displaySurface) {
			return VALUES.contains(displaySurface);
		}
	}

	public static final class Type {
		public static final String USER_REQUEST = "USER_REQUEST";
		public static final String USER_ADDITION = "USER_ADDITION";
		public static final String REQUEST_APPROVAL = "REQUEST_APPROVAL";
		public static final String PERMISSION_CHANGE = "PERMISSION_CHANGE";
		public static final String REQUEST_DENIAL = "REQUEST_DENIAL";
		public static final String SMSS_UPDATE = "SMSS_UPDATE";
		public static final String ACCESS_REQUEST = "ACCESS_REQUEST";
		public static final String ANNOUNCEMENT = "ANNOUNCEMENT";
		public static final String APP_TASK_COMPLETE = "APP_TASK_COMPLETE";
	}

	public static final class Scope {
		public static final String SYSTEM = "SYSTEM";
		public static final String APP = "APP";
	}

	/** Scope selectors accepted by notification read APIs. ALL is never persisted. */
	public static final class FetchScope {
		public static final String ALL = "ALL";
		public static final String SYSTEM = Scope.SYSTEM;
		public static final String APP = Scope.APP;

		private static final Set<String> VALUES = Set.of(ALL, SYSTEM, APP);

		public static boolean isValid(String scope) {
			return VALUES.contains(scope);
		}
	}

	public static final class Audience {
		public static final String USER = "USER";
		public static final String APP_MEMBERS = "APP_MEMBERS";
		public static final String APP_OWNERS = "APP_OWNERS";
		public static final String APP_EDITORS = "APP_EDITORS";
		public static final String GLOBAL = "GLOBAL";
	}

	public static final class Source {
		public static final String SYSTEM = "SYSTEM";
		public static final String USER = "USER";
		public static final String ENGINE = "ENGINE";
		public static final String PROJECT = "PROJECT";
	}

	public static final class Target {
		public static final String NONE = "NONE";
		public static final String ROUTE = "ROUTE";
		public static final String APP = "APP";
	}

}
