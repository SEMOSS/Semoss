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

/**
 * Someone, or some group, whose access a permission batch changes.
 */
public class PermissionSubject {

	/**
	 * The categories of subject: a person, a team, or a model's public use or total
	 */
	public enum SubjectCategory {
		USER, TEAM,
		/**
		 * A model's public use, whose usage limit a batch can change. Like
		 * {@link #MODEL}, it has no id or type, takes only a {@code CHANGE} on a model
		 * engine, by its owners or in admin mode, and its grant carries only a
		 * {@code usageLimit}, with no level or end date.
		 */
		PUBLIC,
		/**
		 * A model's total, for everyone together, whose usage limit a batch can change,
		 * with the same rules as {@link #PUBLIC}
		 */
		MODEL
	}

	/** What the subject is */
	private SubjectCategory category;
	private String id;
	/** The user's login type, or the group's type */
	private String type;

	public PermissionSubject() {

	}

	/**
	 * Creates a subject from its category, its id and its type.
	 *
	 * @param category a person, a team, or a model's public use or total
	 * @param id       the user or group id
	 * @param type     the user's login type, or the group's type
	 */
	public PermissionSubject(SubjectCategory category, String id, String type) {
		this.category = category;
		this.id = id;
		this.type = type;
	}

	/**
	 * Builds the subject for one person.
	 *
	 * @param id   the person's user id
	 * @param type the person's login type, or null when the target keeps only the
	 *             id, as a project, an engine or an insight does
	 * @return the person
	 */
	public static PermissionSubject ofUser(String id, String type) {
		return new PermissionSubject(SubjectCategory.USER, id, type);
	}

	/**
	 * Builds the subject for a group, by its id and its type.
	 *
	 * @param id   the group id
	 * @param type the group's type, such as {@code CUSTOM}
	 * @return the group
	 */
	public static PermissionSubject ofTeam(String id, String type) {
		return new PermissionSubject(SubjectCategory.TEAM, id, type);
	}

	/**
	 * What the subject is: a person, a team, or a model's public use or total.
	 *
	 * @return the subject's category
	 */
	public SubjectCategory getCategory() {
		return category;
	}

	public String getId() {
		return id;
	}

	public String getType() {
		return type;
	}

	/**
	 * Describes the subject as its category and id, or as public use or the model's
	 * total.
	 *
	 * @return the subject in words, for messages, such as "user jdoe"
	 */
	public String describe() {
		switch (category) {
		case TEAM:
			return "team " + id;
		case PUBLIC:
			return "public use";
		case MODEL:
			return "the model's total";
		default:
			return "user " + id;
		}
	}
}
