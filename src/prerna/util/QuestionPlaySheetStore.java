///*******************************************************************************
// * Copyright 2015 Defense Health Agency (DHA)
// *
// * If your use of this software does not include any GPLv2 components:
// * 	Licensed under the Apache License, Version 2.0 (the "License");
// * 	you may not use this file except in compliance with the License.
// * 	You may obtain a copy of the License at
// *
// * 	  http://www.apache.org/licenses/LICENSE-2.0
// *
// * 	Unless required by applicable law or agreed to in writing, software
// * 	distributed under the License is distributed on an "AS IS" BASIS,
// * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * 	See the License for the specific language governing permissions and
// * 	limitations under the License.
// * ----------------------------------------------------------------------------
// * If your use of this software includes any GPLv2 components:
// * 	This program is free software; you can redistribute it and/or
// * 	modify it under the terms of the GNU General Public License
// * 	as published by the Free Software Foundation; either version 2
// * 	of the License, or (at your option) any later version.
// *
// * 	This program is distributed in the hope that it will be useful,
// * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
// * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * 	GNU General Public License for more details.
// *******************************************************************************/
//package prerna.util;
//
//import java.util.HashSet;
//import java.util.Hashtable;
//import java.util.Map;
//import java.util.Set;
//
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
//
//import prerna.ui.components.api.IPlaySheet;
//
///**
// * This class is used to store question playsheets. 
// */
//public class QuestionPlaySheetStore extends Hashtable<String, IPlaySheet> {
//
//	private static final long serialVersionUID = 6959842357268983932L;
//	private static final Logger LOGGER = LogManager.getLogger(QuestionPlaySheetStore.class.getName());
//	private Map<String, Set<String>> sessionIdHash = new Hashtable<String, Set<String>>();
//
//	private static QuestionPlaySheetStore store;
//
//	public int idCount = 0;
//	public static IPlaySheet activeSheet = null;
//
//	/**
//	 * Gets an instance from a specific playsheet.
//	 * @return QuestionPlaySheetStore 
//	 */
//	public static QuestionPlaySheetStore getInstance() {
//		if(store == null) {
//			store = new QuestionPlaySheetStore();
//		}
//		return store; 
//	}
//
//	/**
//	 * Puts into a hashtable the question as the key and the playsheet as the mapped value.
//	 * @param 	Question
//	 * @param 	Specified playsheet for a question.
//	 * @return 	Playsheet
//	 */
//	public IPlaySheet put(String question, IPlaySheet sheet) {
//		IPlaySheet ret = super.put(question, sheet);
//		return ret;
//	}
//
//	/**
//	 * Checks whether there are more playsheets.
//	 * @return 	False. 
//	 */
//	public boolean hasMorePlaySheet() {
//		return false;
//	}
//
//
//	/**
//	 * Sets the active playsheet for the internal frame.
//	 * @param 	Active playsheet
//	 */
//	public void setActiveSheet(IPlaySheet sheet) {
//		activeSheet = sheet;
//	}
//
//	/**
//	 * Gets the active sheet.
//	 * @return 	Active sheet 
//	 */
//	public IPlaySheet getActiveSheet() {
//		// need to clear when the internal frame is closed
//		return activeSheet;
//	}
//
//	/**
//	 * Gets a playsheet based on the question (key) from the store.
//	 * @param 	Question
//	 * @return 	Returned playsheet 
//	 */
//	public IPlaySheet get(String question) {
//		return super.get(question);
//	}
//
//	/**
//	 * Removes a question from the store.
//	 * @param 	Question to be removed.
//	 */
//	public void remove(String question) {
//		super.remove(question);
//		if(activeSheet != null && activeSheet.getQuestionID().equalsIgnoreCase(question)) {
//			activeSheet = null;
//		}
//	}
//
//	/**
//	 * Gets the count of all the sheets in the question store.	
//	 * @return Count 
//	 */
//	public int getIDCount() {
//		int total = idCount;
//		return total;
//	}
//
//	/**
//	 * Gets a set of all the sheets in the question sheet store.
//	 * @return Set of strings. 
//	 */
//	public Set<String> getAllSheets() {
//		return store.keySet();
//	}
//	
//	public void addToSessionHash(String sessionID, String tableID) {
//		Set<String> tableIDs = null;
//		
//		if(sessionIdHash.containsKey(sessionID)) {
//			tableIDs = sessionIdHash.get(sessionID);
//			if(tableIDs == null) {
//				tableIDs = new HashSet<String>();
//			}
//			tableIDs.add(tableID);
//		} else {
//			tableIDs = new HashSet<String>();
//			tableIDs.add(tableID);
//		}
//		
//		sessionIdHash.put(sessionID, tableIDs);
//	}
//	
//	public boolean removeFromSessionHash(String sessionID, String tableID) {
//		if(!sessionIdHash.containsKey(sessionID)) {
//			return false;
//		}
//		Set<String> tableIDs = sessionIdHash.get(sessionID);
//		if(tableIDs.contains(tableID)) {
//			tableIDs.remove(tableID);
//			return true;
//		} 
//
//		return false;
//	}
//	
//	public Set<String> getPlaySheetIDsForSession(String sessionID) {
//		return sessionIdHash.get(sessionID);
//	}
//
//}
