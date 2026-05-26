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
/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 *  This program is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License
 *  as published by the Free Software Foundation; either version 2
 *  of the License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *******************************************************************************/
package prerna.engine.impl.model.message;

import java.util.List;

/**
 * Central place to upgrade persisted Room message JSON across schema versions.
 * <p>
 * Current behavior:
 * <ul>
 * <li>Schema v1 (no {@code schemaVersion}) is upgraded to v2 by normalizing
 * each message for write.</li>
 * </ul>
 * <p>
 * Future-ready: bump {@link AbstractMessage#LATEST_SCHEMA_VERSION} and add step
 * methods here (e.g. v2 -&gt; v3) while keeping upgrades sequential.
 */
public final class MessageSchemaUpgrader {

	private MessageSchemaUpgrader() {
	}

	public static boolean needsUpgrade(List<AbstractMessage> messages) {
		if (messages == null || messages.isEmpty()) {
			return false;
		}
		for (AbstractMessage m : messages) {
			if (m == null) {
				continue;
			}
			Integer v = m.getSchemaVersion();
			if (v == null || v < AbstractMessage.LATEST_SCHEMA_VERSION) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Upgrades messages in-place to the latest schema version, returning true if
	 * any message changed.
	 */
	public static boolean upgradeInPlace(List<AbstractMessage> messages) {
		if (messages == null || messages.isEmpty()) {
			return false;
		}
		boolean changed = false;
		for (AbstractMessage m : messages) {
			if (m == null) {
				continue;
			}
			Integer v = m.getSchemaVersion();
			int from = (v == null ? 1 : v.intValue());
			if (from < AbstractMessage.LATEST_SCHEMA_VERSION) {
				upgradeMessageToLatest(m, from);
				changed = true;
			}
		}
		return changed;
	}

	private static void upgradeMessageToLatest(AbstractMessage message, int fromVersion) {
		int v = fromVersion;
		while (v < AbstractMessage.LATEST_SCHEMA_VERSION) {
			if (v == 1) {
				upgradeV1ToV2(message);
				v = 2;
			} else {
				// Unknown future version; stop rather than corrupting.
				break;
			}
		}
	}

	private static void upgradeV1ToV2(AbstractMessage message) {
		// v2: parts + io + schemaVersion, and omit legacy fields on write.
		message.normalizeForWrite();
	}
}
