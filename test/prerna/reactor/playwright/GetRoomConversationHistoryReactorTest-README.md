# GetRoomConversationHistoryReactorTest - Line-by-Line Explanation

## Overview
This test class validates the behavior of `GetRoomConversationHistoryReactor`, which retrieves and formats conversation history from chat rooms. The tests focus on utility methods for parsing, pagination, sorting, and string formatting, as well as input validation.

---

## Package and Imports (Lines 1-22)

**Line 1**: Package declaration matching the source code structure.

**Lines 3-8**: Static imports for JUnit 5 assertions and Mockito mocking framework methods.

**Line 10**: Java reflection API for accessing private methods in the reactor.

**Lines 11-14**: Standard Java collections for test data structures.

**Lines 16-17**: JUnit 5 annotations for test lifecycle management.

**Lines 19-22**: Project-specific classes needed for testing the reactor.

---

## Test Class Declaration and Fields (Lines 24-38)

**Line 24**: Test class declaration (package-private access).

**Line 26**: The reactor instance being tested, recreated fresh for each test.

**Line 27**: Mock of Insight, the main context object containing user and session information.

**Line 28**: Mock of User, which represents the authenticated user.

**Lines 30-38**: Setup method annotated with `@BeforeEach` runs before each test, creating fresh instances and configuring mocks.

---

## Test 1: testExecuteWithNullUser (Lines 40-50)

**Line 42**: Provides a valid roomId to bypass framework validation and reach the custom user check.

**Line 43**: Configures the insight mock to return null for the user, simulating an unauthenticated state.

**Lines 45-47**: Asserts that execution throws an IllegalArgumentException.

**Line 49**: Verifies the exception message matches the custom validation at line 58 in the reactor.

---

## Test 2: testExecuteWithMissingRoomId (Lines 52-60)

**Line 53**: Provides an empty string for roomId.

**Lines 55-57**: Asserts that execution throws an IllegalArgumentException.

**Line 59**: Verifies the exception message matches the custom validation at line 63 in the reactor that checks for empty strings.

---

## Test 3: testExecuteWithNullRoomId (Lines 63-70)

**Line 65**: Executes the reactor without providing any roomId in the keyValue map.

**Lines 65-67**: Asserts that execution throws an IllegalArgumentException.

**Line 69**: Verifies the exception message matches the framework's `checkOptional()` validation format with double space.

---

## Test 4: testParseIntWithValidInteger (Lines 72-78)

**Lines 73-74**: Uses reflection to access the private `parseInt()` method.

**Line 76**: Invokes the method with a valid integer string "42" and default value 0.

**Line 77**: Verifies the method correctly parses and returns 42.

---

## Test 5: testParseIntWithNullValue (Lines 80-87)

**Lines 82-83**: Accesses the private `parseInt()` method via reflection.

**Line 85**: Invokes with null value and default 10.

**Line 86**: Verifies the method returns the default value when input is null.

---

## Test 6: testParseIntWithEmptyString (Lines 89-96)

**Lines 91-92**: Accesses the private `parseInt()` method.

**Line 94**: Invokes with whitespace-only string and default 15.

**Line 95**: Verifies the method returns the default value for empty/whitespace strings.

---

## Test 7: testParseIntWithInvalidNumber (Lines 98-105)

**Lines 100-101**: Accesses the private `parseInt()` method.

**Line 103**: Invokes with non-numeric string "abc" and default 20.

**Line 104**: Verifies the method catches NumberFormatException and returns the default value.

---

## Test 8: testParseIntWithNegativeNumber (Lines 107-114)

**Lines 109-110**: Accesses the private `parseInt()` method.

**Line 112**: Invokes with negative number string "-5".

**Line 113**: Verifies the method correctly handles negative integers.

---

## Test 9: testNormalizeSortWithAscending (Lines 116-123)

**Lines 118-119**: Accesses the private `normalizeSort()` method.

**Line 121**: Invokes with lowercase "asc".

**Line 122**: Verifies the method normalizes to uppercase "ASC".

---

## Test 10: testNormalizeSortWithDescending (Lines 125-132)

**Lines 127-128**: Accesses the private `normalizeSort()` method.

**Line 130**: Invokes with lowercase "desc".

**Line 131**: Verifies the method normalizes to uppercase "DESC".

---

## Test 11: testNormalizeSortWithMixedCase (Lines 134-141)

**Lines 136-137**: Accesses the private `normalizeSort()` method.

**Line 139**: Invokes with mixed case "DeSc".

**Line 140**: Verifies the method normalizes to uppercase "DESC" regardless of input case.

---

## Test 12: testNormalizeSortWithNull (Lines 143-150)

**Lines 145-146**: Accesses the private `normalizeSort()` method.

**Line 148**: Invokes with null value.

**Line 149**: Verifies the method defaults to "ASC" when input is null.

---

## Test 13: testNormalizeSortWithInvalidValue (Lines 152-159)

**Lines 154-155**: Accesses the private `normalizeSort()` method.

**Line 157**: Invokes with invalid value "invalid".

**Line 158**: Verifies the method defaults to "ASC" for unrecognized values.

---

## Test 14: testApplyPagingWithNoOffset (Lines 161-171)

**Lines 163-165**: Accesses the private `applyPaging()` method.

**Line 167**: Creates 5 test pairs.

**Line 168**: Invokes with offset=0, limit=3.

**Line 170**: Verifies that 3 items are returned from the beginning of the list.

---

## Test 15: testApplyPagingWithOffset (Lines 173-183)

**Lines 175-177**: Accesses the private `applyPaging()` method.

**Line 179**: Creates 10 test pairs.

**Line 180**: Invokes with offset=2, limit=3.

**Line 182**: Verifies that 3 items are returned starting from index 2 (items 2, 3, 4).

---

## Test 16: testApplyPagingWithNoLimit (Lines 185-195)

**Lines 187-189**: Accesses the private `applyPaging()` method.

**Line 191**: Creates 5 test pairs.

**Line 192**: Invokes with offset=0, limit=-1 (no limit).

**Line 194**: Verifies all 5 items are returned when limit is -1.

---

## Test 17: testApplyPagingWithOffsetBeyondSize (Lines 197-207)

**Lines 199-201**: Accesses the private `applyPaging()` method.

**Line 203**: Creates 5 test pairs.

**Line 204**: Invokes with offset=10 (beyond list size), limit=3.

**Line 206**: Verifies an empty list is returned when offset exceeds list size.

---

## Test 18: testApplyPagingWithEmptyList (Lines 209-219)

**Lines 211-213**: Accesses the private `applyPaging()` method.

**Line 215**: Creates an empty list.

**Line 216**: Invokes with offset=0, limit=5.

**Line 218**: Verifies an empty list is returned for empty input.

---

## Test 19: testApplyPagingWithNegativeOffset (Lines 221-231)

**Lines 223-225**: Accesses the private `applyPaging()` method.

**Line 227**: Creates 5 test pairs.

**Line 228**: Invokes with offset=-5 (negative), limit=3.

**Line 230**: Verifies negative offsets are normalized to 0 via `Math.max(0, offset)`.

---

## Test 20: testApplyPagingWithLimitExceedingSize (Lines 233-243)

**Lines 235-237**: Accesses the private `applyPaging()` method.

**Line 239**: Creates 5 test pairs.

**Line 240**: Invokes with offset=0, limit=100 (exceeds size).

**Line 242**: Verifies all 5 items are returned when limit exceeds actual size.

---

## Test 21: testFirstNonBlankWithFirstNonBlank (Lines 245-253)

**Lines 247-249**: Accesses the private `firstNonBlank()` method.

**Line 251**: Invokes with both strings non-blank.

**Line 252**: Verifies the first string is returned.

---

## Test 22: testFirstNonBlankWithFirstBlank (Lines 255-263)

**Lines 257-259**: Accesses the private `firstNonBlank()` method.

**Line 261**: Invokes with first string blank (whitespace only).

**Line 262**: Verifies the second string is returned.

---

## Test 23: testFirstNonBlankWithBothBlank (Lines 265-273)

**Lines 267-269**: Accesses the private `firstNonBlank()` method.

**Line 271**: Invokes with both strings blank.

**Line 272**: Verifies null is returned when both strings are blank.

---

## Test 24: testFirstNonBlankWithBothNull (Lines 275-283)

**Lines 277-279**: Accesses the private `firstNonBlank()` method.

**Line 281**: Invokes with both strings null.

**Line 282**: Verifies null is returned when both strings are null.

---

## Test 25: testFirstNonBlankWithFirstNull (Lines 285-293)

**Lines 287-289**: Accesses the private `firstNonBlank()` method.

**Line 291**: Invokes with first string null, second non-blank.

**Line 292**: Verifies the second string is returned.

---

## Test 26: testBuildHistoryStringWithValidPairs (Lines 295-307)

**Lines 297-298**: Accesses the private `buildHistoryString()` method.

**Line 300**: Creates 2 test pairs with questions and answers.

**Line 301**: Invokes the method with the pairs.

**Lines 303-306**: Verifies the result contains "User:", "Assistant:", and the separator "---".

---

## Test 27: testBuildHistoryStringWithEmptyList (Lines 309-318)

**Lines 311-312**: Accesses the private `buildHistoryString()` method.

**Line 314**: Creates an empty list.

**Line 315**: Invokes the method with empty list.

**Line 317**: Verifies an empty string is returned.

---

## Test 28: testBuildHistoryStringWithNullList (Lines 320-328)

**Lines 322-323**: Accesses the private `buildHistoryString()` method.

**Line 325**: Invokes with null list.

**Line 327**: Verifies an empty string is returned for null input.

---

## Test 29: testBuildHistoryStringWithSinglePair (Lines 330-343)

**Lines 332-333**: Accesses the private `buildHistoryString()` method.

**Line 335**: Creates a single test pair.

**Line 336**: Invokes the method.

**Lines 338-341**: Verifies the result contains "User:" and "Assistant:" but no separator "---" since there's only one pair.

---

## Test 30: testBuildHistoryStringWithNullQuestionAndAnswer (Lines 345-358)

**Lines 347-348**: Accesses the private `buildHistoryString()` method.

**Lines 350-353**: Creates a HashMap with null question and answer values (Map.of() doesn't accept nulls).

**Line 354**: Invokes the method.

**Lines 356-357**: Verifies the method handles null values gracefully by producing "User: \nAssistant: ".

---

## Test 31-36: Descriptor Tests (Lines 360-396)

**testGetReactorDescription (Lines 360-361)**: Verifies the reactor description contains "conversation history".

**testGetDescriptionForRoomIdKey (Lines 364-368)**: Verifies the roomId key description contains "room".

**testGetDescriptionForLimitKey (Lines 371-375)**: Verifies the limit key description contains "Maximum".

**testGetDescriptionForOffsetKey (Lines 378-382)**: Verifies the offset key description contains "skip".

**testGetDescriptionForSortKey (Lines 385-389)**: Verifies the sort key description contains "Sort".

**testGetDescriptionForIncludePartialKey (Lines 392-396)**: Verifies the includePartial key description contains "unanswered".

---

## Helper Method (Lines 398-404)

**createTestPairs (Lines 398-404)**: Creates a list of test question-answer pairs for use in multiple tests. Each pair contains a question and answer with sequential numbering.

---

## Testing Strategy

### Reflection-Based Testing
Uses Java reflection to access and test private utility methods, ensuring they behave correctly in isolation without requiring full integration.

### Pure Method Focus
Tests only pure utility methods that have no external dependencies, making them truly unit testable.

### Input Validation Coverage
Tests the reactor's validation logic for null users, missing room IDs, and empty room IDs.

### Edge Case Coverage
Each utility method is tested with:
- Valid inputs
- Null inputs
- Empty inputs
- Edge cases (negative offsets, limits exceeding size, etc.)

### Equivalence Partitioning
Tests cover representative cases from each category: valid data, null/empty data, boundary conditions.

---

## Key Testing Patterns

1. **Reflection Access Pattern**: All private method tests follow the pattern of getting the method via reflection, making it accessible, invoking it, and verifying results.

2. **Boundary Testing**: Pagination tests verify behavior at boundaries (offset=0, offset beyond size, limit=-1, negative offset).

3. **Null Handling**: Multiple tests verify graceful handling of null inputs across all utility methods.

4. **String Normalization**: Tests verify case-insensitive input handling and default value logic.

5. **Descriptor Testing**: Validates that all key descriptions are properly defined and contain expected keywords.

---

## Limitations

**Cannot Test Full Execute Path**: The full `execute()` method cannot be tested without mocking `RoomUtils.getOrLoadRoom()`, which requires external room/database infrastructure. Tests focus on the utility methods that perform the core formatting and pagination logic.

**Framework Validation Priority**: The framework's `checkOptional()` validation runs before custom validation, so some tests must provide valid data to bypass framework checks and reach custom validation logic.
