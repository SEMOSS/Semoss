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
package prerna.util.sql;

/**
 * Enum to categorize database types as SQL or NoSQL
 */
public enum DatabaseCategoryEnum {

	SQL("SQL"), NOSQL("NoSQL"), RDF("RDF"), UNKNOWN("Unknown");

	private final String categoryName;

	DatabaseCategoryEnum(String categoryName) {
		this.categoryName = categoryName;
	}

	public String getCategoryName() {
		return this.categoryName;
	}

	/**
	 * Determines the database category
	 * 
	 * @param rdbmsType The database_subtype string from the SMSS file
	 * @return DatabaseCategoryEnum (SQL or NOSQL)
	 */
	public static DatabaseCategoryEnum getCategoryFromRdbmsType(String rdbmsType) {
		if (rdbmsType == null || rdbmsType.trim().isEmpty()) {
			return UNKNOWN;
		}

		RdbmsTypeEnum rdbmsEnum = RdbmsTypeEnum.getEnumFromString(rdbmsType.trim());
		if (rdbmsEnum != null) {
			switch (rdbmsEnum) {
			case CASSANDRA:
			case ELASTIC_SEARCH:
			case OPEN_SEARCH:
				return NOSQL;
			default:
				return SQL; // most entries are SQL databases
			}
		}

		// Check for known NoSQL and RDF types that might not be in RdbmsTypeEnum
		String upperRdbmsType = rdbmsType.toUpperCase().trim();
		switch (upperRdbmsType) {
		case "MONGODB":
		case "NEO4J":
		case "JANUSGRAPH":
		case "TINKER":
			return NOSQL;
		case "JENA":
		case "JENA_TDB":
		case "SESAME":
		case "RDF4J":
			return RDF;
		default:
			return UNKNOWN;
		}
	}
}