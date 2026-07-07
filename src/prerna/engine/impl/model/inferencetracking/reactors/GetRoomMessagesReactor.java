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
package prerna.engine.impl.model.inferencetracking.reactors;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetRoomMessagesReactor extends AbstractReactor {
	@SuppressWarnings("unused")
	private static final Logger classLogger = LogManager.getLogger(GetRoomMessagesReactor.class);

    public GetRoomMessagesReactor() {
        this.keysToGet = new String[] {"roomId","limit","offset","sort"};
        this.keyRequired = new int[] {1};
    }
    
	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
        if (user == null) {
            throw new IllegalArgumentException("You are not properly logged in");
        }
        Integer limit = -1, offset = -1;
        String roomId = this.keyValue.get(this.keysToGet[0]);
        String dateSort = "ASC";
        
        if ( (this.keyValue.get(this.keysToGet[1])!= null && !this.keyValue.get(this.keysToGet[1]).isEmpty()) 
        		&& 
        	(this.keyValue.get(this.keysToGet[2]) != null && !this.keyValue.get(this.keysToGet[2]).isEmpty())) {
        	limit = Integer.parseInt(this.keyValue.get(this.keysToGet[1]));
        	offset = Integer.parseInt(this.keyValue.get(this.keysToGet[2]));
        }
        if(this.keyValue.get(this.keysToGet[3]) != null && !this.keyValue.get(this.keysToGet[3]).isEmpty()
        		&& this.keyValue.get(this.keysToGet[3]).equals("DESC")) 
        	dateSort = "DESC";
        
        // Convert each message to output map for return
        List<Map<String, Object>> outpuMap = new ArrayList<>();
        
        // Get room object (will load or fetch as needed) and convert messages to new format from legacy
        Room room ;
        try {
        room = RoomUtils.getOrLoadRoom(roomId, this.insight);
        } catch(Exception e){
        	// returning empty map as that is the closet to the old design of empty array from db
        	return new NounMetadata(outpuMap, PixelDataType.VECTOR);
        }

        // Filter/slice results without altering room message object
        List<AbstractMessage> page = RoomUtils.getPagedMessages(room.getMessages(), dateSort, offset, limit);
 
        for (AbstractMessage m : page) {
        	outpuMap.add(messageToMap(m));
        }
 
        
		return new NounMetadata(outpuMap, PixelDataType.VECTOR);
	}
	
	/** Converts an AbstractMessage to a Map<String,Object> for wire output */
	private static Map<String,Object> messageToMap(AbstractMessage m) {
	    Map<String, Object> map = new java.util.LinkedHashMap<>();
	    if (m instanceof prerna.engine.impl.model.message.InputMessage) {
	        prerna.engine.impl.model.message.InputMessage im = (prerna.engine.impl.model.message.InputMessage)m;
	        map.put("MESSAGE_DATA", im.getInputUIPrompt());
	        map.put("DATE_CREATED", im.getDateCreated());
	        map.put("MESSAGE_ID", im.getMessageId());
	        map.put("MESSAGE_TYPE", "INPUT");
	        map.put("MESSAGE_TYPE_FORMAT", m.getMessageType());
	    } else if (m instanceof prerna.engine.impl.model.message.ResponseMessage) {
	        prerna.engine.impl.model.message.ResponseMessage rm = (prerna.engine.impl.model.message.ResponseMessage)m;
	        map.put("MESSAGE_DATA", rm.getContent());
	        map.put("DATE_CREATED", rm.getDateCreated());
	        map.put("MESSAGE_ID", rm.getMessageId());
	        map.put("MESSAGE_TYPE", "RESPONSE");
	        map.put("MESSAGE_TYPE_FORMAT", m.getMessageType());
	    } else {
	        map.put("MESSAGE_TYPE", "UNKNOWN");
	    }
	    return map;
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("roomId")) {
			return "The room or conversation ID for a given chat app";
		} 
		return super.getDescriptionForKey(key);
	}
}