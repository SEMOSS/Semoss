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
package prerna.testing;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Vector;

import prerna.util.Constants;

public class ApiTestsSemossConstants {

	public static final String BASE_DIRECTORY = new File("").getAbsolutePath();
	public static final String TEST_RESOURCES_DIRECTORY = Paths.get(BASE_DIRECTORY, "test", "resources").toAbsolutePath().toString();

	public static final String TEST_BASE_DIRECTORY = IntegrationTestWorkspace.basePath().toAbsolutePath().toString();

	public static final String TEST_DB_DIRECTORY = Paths.get(TEST_BASE_DIRECTORY, "db").toAbsolutePath().toString();
	public static final String TEST_PROJECT_DIRECTORY = Paths.get(TEST_BASE_DIRECTORY, "project").toAbsolutePath().toString();
	public static final Path TEST_CONFIG_DIRECTORY = Paths.get(TEST_BASE_DIRECTORY, "testconfig");

	public static final Path BASE_RDF_MAP = Paths.get(BASE_DIRECTORY, "RDF_Map.prop");
	public static final Path TEST_RDF_MAP = Paths.get(TEST_BASE_DIRECTORY, "RDF_Map.prop");

	public static final String LMD_SMSS = Paths.get(TEST_DB_DIRECTORY, Constants.LOCAL_MASTER_DB + ".smss").toAbsolutePath().toString();
	public static final String SECURITY_SMSS = Paths.get(TEST_DB_DIRECTORY, Constants.SECURITY_DB + ".smss").toAbsolutePath().toString();
	public static final String SCHEDULER_SMSS = Paths.get(TEST_DB_DIRECTORY, Constants.SCHEDULER_DB + ".smss").toAbsolutePath().toString();
	public static final String THEMES_SMSS = Paths.get(TEST_DB_DIRECTORY, Constants.THEMING_DB + ".smss").toAbsolutePath().toString();
	public static final String UTDB_SMSS = Paths.get(TEST_DB_DIRECTORY, Constants.USER_TRACKING_DB + ".smss").toAbsolutePath().toString();
	
	// default user info
	public static final String USER_NAME = "user1";	
	public static final String USER_EMAIL = "user1@example.com";	
	
	
	// constants for email api
	public static final String EMAIL_BCC = "Bcc";
	public static final String EMAIL_CC = "Cc";		
	public static final String EMAIL_TO = "To";	
	public static final String EMAIL_FROM = "From";	
	public static final String EMAIL_ADDRESS = "Address";	
	public static final String EMAIL_SUBJECT = "Subject";	
	public static final String EMAIL_ATTACHMENTS = "Subject";	
	public static final String EMAIL_MESSAGE_ID = "MessageID";	
	public static final String EMAIL_ID = "ID";	
	public static final String EMAIL_HTML = "HTML";	
	public static final String EMAIL_TEXT = "Text";	
	public static final String EMAIL_DATE = "Date";	
	public static final String EMAIL_READ = "Read";	//boolean
	
	// movie data
	public static final String MOVIE_CSV_FILE_NAME = "Movies.csv";
	public static final String DELIMITER = ",";
	public static final Path TEST_MOVIE_CSV_PATH = Paths.get(TEST_RESOURCES_DIRECTORY, MOVIE_CSV_FILE_NAME);
	public static final String MOVIE_TABLE_NAME = "MOVIES";
	public static final String TITLE = "Title";
	public static final String MOVIE_BUDGET = "MovieBudget";
	public static final String ROTTEN_TOMATOES_AUDIENCE = "RottenTomatoes_Critics";
	public static final String ROTTEN_TOMATOES_CRITICS = "RottenTomatoes_Audience";
	public static final String REVENUE_DOMESTIC = "Revenue_Domestic";
	public static final String REVENUE_INTERNATIONAL = "Revenue_International";
	public static final String DIRECTOR = "Director";
	public static final String STUDIO = "Studio";
	public static final String GENRE = "Genre";
	public static final String NOMINATED = "Nominated";
	
	public static List<String> MOVIE_TABLE_COLUMNS = new Vector<String>();
	static {
		MOVIE_TABLE_COLUMNS.add(TITLE);
		MOVIE_TABLE_COLUMNS.add(MOVIE_BUDGET);
		MOVIE_TABLE_COLUMNS.add(ROTTEN_TOMATOES_AUDIENCE);
		MOVIE_TABLE_COLUMNS.add(ROTTEN_TOMATOES_CRITICS);
		MOVIE_TABLE_COLUMNS.add(REVENUE_DOMESTIC);
		MOVIE_TABLE_COLUMNS.add(REVENUE_INTERNATIONAL);
		MOVIE_TABLE_COLUMNS.add(DIRECTOR);
		MOVIE_TABLE_COLUMNS.add(STUDIO);
		MOVIE_TABLE_COLUMNS.add(GENRE);
		MOVIE_TABLE_COLUMNS.add(NOMINATED);
	}



}
