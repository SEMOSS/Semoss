package prerna.engine.api;

import java.util.HashSet;
import java.util.Set;

public enum TokenTypeEnum {
    // Base token types
    INPUT(Category.BASE, "INPUT_MESSAGE_TOKENS", "numberOfTokensInPrompt"),
    OUTPUT(Category.BASE, "OUTPUT_MESSAGE_TOKENS", "numberOfTokensInResponse"),
    
    // Additional/extended token types
    THINKING(Category.ADDITIONAL, "THINKING_TOKENS", "numberOfThinkingTokens"),
    CACHED(Category.ADDITIONAL, "CACHED_MESSAGE_TOKENS", "numberOfCachedTokens");
    
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