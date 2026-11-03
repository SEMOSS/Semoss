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
package prerna.auth.access;

import java.util.ArrayList;
import java.util.List;

/**
 * One change in a permission batch: give a subject access, change it, or take
 * it away.
 */
public class PermissionChange {

	/** What the change does */
	public enum Action {
		ADD, CHANGE, REMOVE
	}

	private Action action;
	private PermissionSubject subject;
	/** The access after an add or a change; unused for a removal */
	private PermissionGrant to;

	public PermissionChange() {

	}

	/**
	 * Creates a change from its action, its subject and the access after it.
	 *
	 * @param action  what the change does
	 * @param subject whose access it changes
	 * @param to      the access after an add or a change, or null for a removal
	 */
	public PermissionChange(Action action, PermissionSubject subject, PermissionGrant to) {
		this.action = action;
		this.subject = subject;
		this.to = to;
	}

	/**
	 * Builds a change that gives a subject access.
	 *
	 * @param subject who gets access
	 * @param to      the access they get
	 * @return a change giving the subject access
	 */
	public static PermissionChange add(PermissionSubject subject, PermissionGrant to) {
		return new PermissionChange(Action.ADD, subject, to);
	}

	/**
	 * Builds a change that replaces a subject's access with the given grant.
	 *
	 * @param subject whose access changes
	 * @param to      the whole access they have after it
	 * @return a change to the subject's access
	 */
	public static PermissionChange change(PermissionSubject subject, PermissionGrant to) {
		return new PermissionChange(Action.CHANGE, subject, to);
	}

	/**
	 * Builds a change that takes a subject's access away.
	 *
	 * @param subject whose access is taken away
	 * @return a change taking the subject's access away
	 */
	public static PermissionChange remove(PermissionSubject subject) {
		return new PermissionChange(Action.REMOVE, subject, null);
	}

	/**
	 * Builds one change with the same action and grant for each person, each
	 * subject given by user id with no login type.
	 *
	 * @param action  what each change does
	 * @param userIds the people, by user id
	 * @param to      the access each of them has after it; unused for a removal
	 * @return the same change for each person
	 */
	public static List<PermissionChange> forPeople(Action action, List<String> userIds, PermissionGrant to) {
		List<PermissionChange> changes = new ArrayList<>();
		for (String userId : userIds) {
			changes.add(new PermissionChange(action, PermissionSubject.ofUser(userId, null),
					action == Action.REMOVE ? null : to));
		}
		return changes;
	}

	public Action getAction() {
		return action;
	}

	public PermissionSubject getSubject() {
		return subject;
	}

	public PermissionGrant getTo() {
		return to;
	}
}
