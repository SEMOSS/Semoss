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
package prerna.reactor.collaboration;

import prerna.collaboration.BrainTopicSources;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class BrainLinkTopicSourceReactor extends AbstractCollaborationReactor {
    public BrainLinkTopicSourceReactor() {
        this.keysToGet = new String[]{"topicId","sourceId","remove"};
        this.keyRequired = new int[]{1,1,0};
    }
    @Override public NounMetadata execute() {
        return mapResult(BrainTopicSources.link(getUser(), getString("topicId"), getString("sourceId"), Boolean.TRUE.equals(getBoolean("remove"))));
    }
    @Override public String getReactorDescription() {
        return "Links an existing owned source to another topic or removes that association. Unlinking preserves the source and its other topic links.";
    }
    @Override protected String getDescriptionForKey(String key) {
        return switch (key) {
            case "topicId" -> "An explicitly linked topic id; assistant tools may only use this chat's confirmed topics";
            case "sourceId" -> "Source id returned by the source list or search";
            case "version" -> "Saved source version; omit for latest";
            case "expectedVersion" -> "Current version from the editor; rejects stale updates";
            case "kind" -> "document, note, or link";
            case "title" -> "Source title, at most 255 characters";
            case "content" -> "User-provided text for a note or a saved copy accompanying a link";
            case "filePath" -> "Relative path of the uploaded file in this insight or the specified owned room";
            case "roomId" -> "Optional owned active room to import a saved file copy from";
            case "url" -> "HTTP or HTTPS reference URL; never fetched";
            case "remove" -> "True to unlink only this topic association";
            case "query" -> "Words to find in saved source titles and contents; omit to list";
            default -> super.getDescriptionForKey(key);
        };
    }
}
