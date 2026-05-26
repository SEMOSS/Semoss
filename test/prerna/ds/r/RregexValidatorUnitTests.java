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
package prerna.ds.r;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class RregexValidatorUnitTests {

    private RregexValidator validator;

    @BeforeEach
    void setUp() {
        validator = new RregexValidator();
    }

    @Nested
    class NullAndEmptyInputTests {

        @Test
        void null_throwsNullPointerException() {
            assertThrows(NullPointerException.class, () -> validator.Validate(null));
        }

        @Test
        void emptyString_doesNotThrow() {
            assertDoesNotThrow(() -> validator.Validate(""));
        }

        @Test
        void whitespaceOnly_doesNotThrow() {
            assertDoesNotThrow(() -> validator.Validate("   "));
        }
    }

    @Nested
    class NoQuotesTests {

        @Test
        void simpleScript_noQuotes_doesNotThrow() {
            assertDoesNotThrow(() -> validator.Validate("x <- 42"));
        }

        @Test
        void scriptWithBackslashOutsideQuotes_doesNotThrow() {
            // Backslash outside quotes is at even index in split array, so skipped
            assertDoesNotThrow(() -> validator.Validate("x <- y\\z"));
        }

        @Test
        void multipleStatements_noQuotes_doesNotThrow() {
            assertDoesNotThrow(() -> validator.Validate("x <- 1; y <- 2; z <- x + y"));
        }
    }

    @Nested
    class NoBackslashInQuotesTests {

        @Test
        void noBackslashInQuotes_doesNotThrow() {
            assertDoesNotThrow(() -> validator.Validate("x <- \"hello world\""));
        }

        @Test
        void emptyQuotedString_doesNotThrow() {
            assertDoesNotThrow(() -> validator.Validate("x <- \"\""));
        }

        @Test
        void singleCharInQuotes_doesNotThrow() {
            assertDoesNotThrow(() -> validator.Validate("x <- \"a\""));
        }

        @Test
        void twoQuotedStrings_noBackslash_doesNotThrow() {
            assertDoesNotThrow(() -> validator.Validate("x <- \"hello\"; y <- \"world\""));
        }

        @Test
        void noBackslashAnywhere_doesNotThrow() {
            assertDoesNotThrow(() -> validator.Validate("paste(\"a\", \"b\", \"c\")"));
        }
    }

    @Nested
    class BackslashInQuotesThrowsTests {

        @Test
        void backslashN_inQuotes_throwsIllegalArgument() {
            // The validator checks substring(j, j+1) which is the backslash itself,
            // so any backslash inside quotes triggers an exception
            assertThrows(IllegalArgumentException.class,
                    () -> validator.Validate("x <- \"hello\\nworld\""));
        }

        @Test
        void backslashT_inQuotes_throwsIllegalArgument() {
            assertThrows(IllegalArgumentException.class,
                    () -> validator.Validate("x <- \"col1\\tcol2\""));
        }

        @Test
        void backslashR_inQuotes_throwsIllegalArgument() {
            assertThrows(IllegalArgumentException.class,
                    () -> validator.Validate("x <- \"line1\\rline2\""));
        }

        @Test
        void doubleBackslash_inQuotes_throwsIllegalArgument() {
            assertThrows(IllegalArgumentException.class,
                    () -> validator.Validate("x <- \"path\\\\file\""));
        }

        @Test
        void backslashQ_inQuotes_throwsIllegalArgument() {
            assertThrows(IllegalArgumentException.class,
                    () -> validator.Validate("x <- \"hello\\qworld\""));
        }

        @Test
        void backslashD_inQuotes_throwsIllegalArgument() {
            assertThrows(IllegalArgumentException.class,
                    () -> validator.Validate("x <- \"test\\dvalue\""));
        }

        @Test
        void backslashAtStartOfQuotedString_throwsIllegalArgument() {
            assertThrows(IllegalArgumentException.class,
                    () -> validator.Validate("x <- \"\\nhello\""));
        }

        @Test
        void trailingBackslash_inQuotes_throwsIllegalArgument() {
            assertThrows(IllegalArgumentException.class,
                    () -> validator.Validate("x <- \"test\\\""));
        }

        @Test
        void consecutiveEscapes_throwsIllegalArgument() {
            assertThrows(IllegalArgumentException.class,
                    () -> validator.Validate("x <- \"\\n\\t\\r\\\\\""));
        }
    }

    @Nested
    class ExceptionMessageTests {

        @Test
        void exceptionMessage_isInvalidInput() {
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> validator.Validate("x <- \"bad\\escape\""));
            assertEquals("Invalid Input!", ex.getMessage());
        }
    }

    @Nested
    class MixedQuotedStringsTests {

        @Test
        void firstQuotedHasBackslash_throws() {
            assertThrows(IllegalArgumentException.class,
                    () -> validator.Validate("x <- \"a\\n\"; y <- \"world\""));
        }

        @Test
        void secondQuotedHasBackslash_throws() {
            assertThrows(IllegalArgumentException.class,
                    () -> validator.Validate("x <- \"hello\"; y <- \"b\\nworld\""));
        }

        @Test
        void multipleQuotedStrings_noneWithBackslash_doesNotThrow() {
            assertDoesNotThrow(() -> validator.Validate("paste(\"a\", \"b\", \"c\")"));
        }
    }

    @Nested
    class BackslashOutsideQuotesTests {

        @Test
        void backslashOnlyOutsideQuotes_doesNotThrow() {
            // Backslash in even-indexed segment (outside quotes) is not checked
            assertDoesNotThrow(() -> validator.Validate("x\\y"));
        }

        @Test
        void backslashBeforeFirstQuote_doesNotThrow() {
            // "x\\y <- \"hello\"" - backslash is in first segment (even index)
            assertDoesNotThrow(() -> validator.Validate("x\\y <- \"hello\""));
        }

        @Test
        void backslashBetweenQuotedStrings_doesNotThrow() {
            // Split: ["a <- ", "hello", " \\z ", "world", ""]
            // Backslash is in segment index 2 (even), not checked
            assertDoesNotThrow(() -> validator.Validate("a <- \"hello\" \\z \"world\""));
        }
    }

    @Nested
    class NoBackslashTests {

        @Test
        void scriptWithNoBackslash_containsCheckFalse_doesNotThrow() {
            assertDoesNotThrow(() -> validator.Validate("library(dplyr)"));
        }

        @Test
        void quotedStringWithSpecialChars_noBackslash_doesNotThrow() {
            assertDoesNotThrow(() -> validator.Validate("x <- \"hello! @#$%^&*()\""));
        }
    }
}
