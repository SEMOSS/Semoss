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
package prerna.engine.api;

import java.util.HashSet;
import java.util.Set;

public enum TokenTypeEnum {
    // Base token types
    INPUT(Category.BASE, "INPUT_MESSAGE_TOKENS", "numberOfTokensInPrompt"),
    OUTPUT(Category.BASE, "OUTPUT_MESSAGE_TOKENS", "numberOfTokensInResponse"),
    
    // Additional/extended token types
    THINKING(Category.ADDITIONAL, "THINKING_TOKENS", "numberOfThinkingTokens"),
    CACHED(Category.ADDITIONAL, "CACHED_TOKENS", "numberOfCachedTokens");
    
    private final Category category;
    private final String dbColumnName;
    private final String responseMapKey;

    TokenTypeEnum(Category category, String dbName, String responseMapKey) {
        this.category = category;
        this.dbColumnName = dbName;
        this.responseMapKey = responseMapKey;
    }
    
    public String getDbColumnName() {
        return dbColumnName;
    }
    
    public String getResponseMapKey() {
        return responseMapKey;
    }
    
    public boolean isAdditional() {
        return category == Category.ADDITIONAL;
    }
    
    public boolean isBase() {
        return category == Category.BASE;
    }

    public static Set<TokenTypeEnum> getAdditionalTokenTypes() {
        Set<TokenTypeEnum> additionalTokens = new HashSet<>();
        for (TokenTypeEnum tokenType : TokenTypeEnum.values()) {
            if (tokenType.isAdditional()) {
                additionalTokens.add(tokenType);
            }
        }
        return additionalTokens;
    }

    public static Set<TokenTypeEnum> getTokenTypesAsSet() {
        Set<TokenTypeEnum> tokenTypeSet = new HashSet<>();
        for (TokenTypeEnum tokenType : TokenTypeEnum.values()) {
            tokenTypeSet.add(tokenType);
        }
        return tokenTypeSet;
    }

    public enum Category {
        BASE,
        ADDITIONAL
    }
}