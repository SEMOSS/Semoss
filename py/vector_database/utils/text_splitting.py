from typing import List, Union, Optional

import re
import pandas as pd

from ..constants import ENCODING_OPTIONS


def clean_up_string(input_string: str):
    cleaned_string = re.sub(r"(\.\s)+", ". ", input_string)
    cleaned_string = re.sub(
        r"\s+", " ", cleaned_string
    )  # Replace multiple spaces with a single space
    cleaned_string = re.sub(r"\.+", ".", cleaned_string)
    # cleaned_string = re.sub(r'[\.\s]+', ' ', cleaned_string) # Replace multiple dots and spaces with a single space

    return cleaned_string.strip()  # Remove leading and trailing spaces


def extract_page_number(input_string):
    # Use regular expression to extract the integer
    match = re.search(r"\d+", input_string)

    if match:
        return match.group()
    else:
        raise ValueError("No integer found in the string.")


def find_keyword_indices(sentences: List[str], keywords):
    """
    This function takes a list of sentences and a list of keywords as inputs, and returns a list of indices in which the keywords occurred along with the sentences with all keywords removed.

    Args:
        sentences (List[str]): A list of sentences.
        keywords (List[str]): A list of keywords to find in sentences.

    Returns:
        list[tuple]: A tuple with a list of indices where keywords occurred and a list of sentences with all keywords removed.
    """
    from ordered_set import OrderedSet

    keyword_indices = []
    pages_in_chunk = OrderedSet()
    previous_match = "<CFG_IDENTIFIED_AS_PAGE_1>"

    # Remove the first page since we dont have to search for it
    keyword_position = 0
    modified_sentence = (
        sentences[0][:keyword_position]
        + sentences[0][keyword_position + len(previous_match) :]
    )
    sentences[0] = modified_sentence
    pages_in_chunk.append("1")

    for index, sentence in enumerate(sentences):
        for keyword in keywords:
            keyword_position = sentence.find(keyword)
            if keyword_position != -1:
                # We found a match
                if keyword_position > 0:
                    # Need to add the previous page since there is overlap
                    pages_in_chunk.add(extract_page_number(previous_match))

                pages_in_chunk.add(extract_page_number(keyword))
                previous_match = keyword

        if len(pages_in_chunk) == 0:
            pages_in_chunk.add(extract_page_number(previous_match))

        keyword_indices.append(pages_in_chunk)
        pages_in_chunk = OrderedSet()

    # Separate loop to remove keywords from sentences
    for keyword in keywords:
        sentences = [sentence.replace(keyword, "") for sentence in sentences]

    return [", ".join(indicies) for indicies in keyword_indices], sentences


def regex_split_text(text, pattern):
    parts = re.split(pattern, text)
    chunks = []

    for i in range(1, len(parts), 2):
        if i < len(parts):
            timestamp = parts[i].strip()
            content = parts[i + 1].strip()
            if timestamp:
                chunk = f"{timestamp} {content}".strip()
                chunks.append(chunk)

    return chunks


def extract_timestamp_range(input_string):
    match = re.search(r"([0-9]+\.?[0-9]*\s*-\s*[0-9]+\.?[0-9]*)", input_string)
    if match:
        return match.group(1)
    match = re.search(r"([0-9]+\.?[0-9]*)", input_string)
    if match:
        return match.group(1)
    raise ValueError("No timestamp found in the string.")


def find_timestamp_indices(sentences: List[str], timestamp_keywords):
    from ordered_set import OrderedSet

    timestamp_indices = []

    for sentence in sentences:
        timestamp_in_chunk = OrderedSet()
        for keyword in timestamp_keywords:
            keyword_position = sentence.find(keyword)
            if keyword_position != -1:
                timestamp_in_chunk.add(extract_timestamp_range(keyword))
        timestamp_indices.append(timestamp_in_chunk)

    for keyword in timestamp_keywords:
        sentences = [sentence.replace(keyword, "") for sentence in sentences]

    return [", ".join(indicies) for indicies in timestamp_indices], sentences


def split_text(
    csv_file_location: str,
    cfg_tokenizer,
    chunk_unit: str,
    chunk_size: int,
    chunk_overlap: int,
    chunking_strategy: Optional[Union[str, List[int]]] = [],
    chunking_method: Optional[str] = "recursive",
) -> None:
    """
    Splits text content in a CSV file into chunks based on specified parameters.

    Args:
        csv_file_location (`str`): Path to the CSV file containing text data.
        cfg_tokenizer: Tokenizer object used to count tokens (if chunk_unit is 'tokens').
        chunk_unit (`str`): Unit for chunk size ('characters' or 'tokens').
        chunk_size (`int`): Maximum size of each chunk (in tokens or characters).
        chunk_overlap (`int`): Number of characters/tokens to overlap between chunks.
        chunking_strategy (`Optional[Union[str, List[int]]]`): Optional strategy for customizing chunking (defaults to splitting all text).
        chunking_method (`Optional[str]`): Method for splitting text (options are 'token' (default), 'semantic' (using Chonky), and 'recursive').

    Raises:
        AssertionError: If chunk_unit is not 'characters' or 'tokens'.
        Exception: If the CSV file cannot be read with any of the specified encodings.
    """

    assert chunk_unit in [
        "characters",
        "tokens",
    ], f"Unable to create chunks using {chunk_unit}. Please specify either 'characters' or 'tokens'."

    for encoding in ENCODING_OPTIONS:
        try:
            main_df = pd.read_csv(csv_file_location, encoding=encoding)
            break
        except:
            continue
    else:
        # The else clause is executed if the loop completes without encountering a break
        raise Exception("Unable to read the file with any of the specified encodings")

    # clean the csv to make sure all content was extracted for each row
    main_df = main_df.dropna(subset=["Content"])
    main_df.reset_index(
        inplace=True, drop=True
    )  # reset the index to make it a clean df

    # Separate text and other modalities
    assert "Modality" in main_df.columns
    text_rows = main_df["Modality"] == "text"
    text_results_df = main_df[text_rows]
    other_modalities_df = main_df[~text_rows]
    # else:
    #     text_results_df = main_df
    #     other_modalities_df = pd.DataFrame()

    # Guard against key error when checking for first row if there are no rows in the CSV file after dropping all invalid content
    if main_df.empty:
        raise Exception("No valid content found to chunk.")

    document_name = main_df["Source"][0]

    if chunking_method.lower() == "semantic":
        text_results_df = split_text_semantically(
            text_results_df=text_results_df,
            document_name=document_name,
            cfg_tokenizer=cfg_tokenizer,
        )
    elif chunking_method.lower() == "recursive":
        text_results_df = split_text_recursively(
            text_results_df=text_results_df,
            chunking_strategy=chunking_strategy,
            document_name=document_name,
            cfg_tokenizer=cfg_tokenizer,
            chunk_unit=chunk_unit,
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
        )
    else:
        text_results_df = split_by_tokens(
            text_results_df=text_results_df,
            chunking_strategy=chunking_strategy,
            document_name=document_name,
            cfg_tokenizer=cfg_tokenizer,
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
        )

    # Combine text chunks with other modalities and save to CSV
    result = pd.concat([text_results_df, other_modalities_df], ignore_index=True)

    result.to_csv(csv_file_location, index=False)


def split_by_tokens(
    text_results_df: pd.DataFrame,
    chunking_strategy: Union[str, List[int]],
    document_name: str,
    cfg_tokenizer,
    # chunk_unit: str,
    chunk_size: int,
    chunk_overlap: int,
) -> pd.DataFrame:
    from langchain_text_splitters import TokenTextSplitter

    # Initialize text splitter with specified parameters
    text_splitter = TokenTextSplitter.from_huggingface_tokenizer(
        cfg_tokenizer.tokenizer,
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
    )
    # Audio and Video extensions
    audio_video_extensions = [
        ".mp3",
        ".wav",
        ".flac",
        ".ogg",
        ".amr",
        ".webm",
        ".mp4",
        ".mov",
        ".avi",
    ]

    # Handle different chunking strategies
    if (isinstance(chunking_strategy, List) and len(chunking_strategy) == 0) or (
        chunking_strategy == "ALL"
    ):
        if document_name.endswith(tuple(audio_video_extensions)):
            timestamp_pattern = r"(<CFG_IDENTIFIED_AS_TIMESTAMP_[\d.]+ - [\d.]+>)"
            identified_timestamps = (
                "<CFG_IDENTIFIED_AS_TIMESTAMP_"
                + text_results_df["Divider"].astype(str)
                + ">"
            )
            combined_text = " ".join(
                identified_timestamps
                + text_results_df["Content"].apply(clean_up_string)
            )

            chunks_strings = regex_split_text(combined_text, timestamp_pattern)

            timestamp_ranges, chunks = find_timestamp_indices(
                [chunk for chunk in chunks_strings],
                identified_timestamps.to_list(),
            )

            counter = {}  # initialize the counter
            parts = []  # keep track of the parts list
            for i in timestamp_ranges:
                part = counter.get(i, 0)
                counter[i] = part + 1
                parts.append(part)

        else:
            identified_pages = (
                "<CFG_IDENTIFIED_AS_PAGE_"
                + text_results_df["Divider"].astype(str)
                + ">"
            )
            chunks = text_splitter.create_documents(
                [
                    " ".join(
                        identified_pages
                        + text_results_df["Content"].apply(clean_up_string)
                    )
                ]
            )

            page_numbers, chunks = find_keyword_indices(
                [document.page_content for document in chunks],
                identified_pages.to_list()[1:],
            )

            counter = {}  # initialize the counter
            parts = []  # keep track of the parts list
            for i in page_numbers:
                part = counter.get(i, 0)
                counter[i] = part + 1
                parts.append(part)

    elif (
        isinstance(chunking_strategy, List)
        and len(chunking_strategy) == text_results_df.shape[0]
    ) or (chunking_strategy == "PAGE_BY_PAGE"):
        # Create chunks from each page individually
        chunks = []
        page_numbers = []
        parts = []
        for page_number, page_text in zip(
            text_results_df["Divider"],
            text_results_df["Content"].apply(clean_up_string),
        ):

            page_chunks = text_splitter.create_documents([page_text])

            for part in range(len(page_chunks)):
                chunk = page_chunks[part].page_content
                chunks.append(chunk)
                page_numbers.append(page_number)
                parts.append(part)

    elif all(isinstance(sublist, list) for sublist in chunking_strategy):
        # Raise exception for unsupported chunking strategy not implemented
        # The goal here is to create chunks from specific pages
        raise Exception("Specific chunking strategy is not implemented yet")

    else:
        raise Exception("Chunking strategy is not defined")

    if document_name.endswith(tuple(audio_video_extensions)):
        text_results_df = pd.DataFrame(
            [
                [
                    document_name,
                    "text",
                    timestamp_range,
                    part,
                    cfg_tokenizer.count_tokens(chunk),
                    chunk,
                ]
                for timestamp_range, part, chunk in zip(timestamp_ranges, parts, chunks)
            ],
            columns=["Source", "Modality", "Divider", "Part", "Tokens", "Content"],
        )
        return text_results_df
    else:
        text_results_df = pd.DataFrame(
            [
                [
                    document_name,
                    "text",
                    page_number,
                    part,
                    cfg_tokenizer.count_tokens(chunk),
                    chunk,
                ]
                for page_number, part, chunk in zip(page_numbers, parts, chunks)
            ],
            columns=["Source", "Modality", "Divider", "Part", "Tokens", "Content"],
        )
        return text_results_df


def split_text_recursively(
    text_results_df: pd.DataFrame,
    chunking_strategy: Union[str, List[int]],
    document_name: str,
    cfg_tokenizer,
    chunk_unit: str,
    chunk_size: int,
    chunk_overlap: int,
) -> pd.DataFrame:
    """
    This function splits a DataFrame containing text data (text_results_df) into chunks based on the chunking_strategy.

    Args:
        text_results_df (`pd.DataFrame`): DataFrame containing text data with columns like 'Divider' and 'Content'.
        chunking_strategy (`Union[str, List[int]]`): Defines how to split the text. Can be a string ('ALL' or 'PAGE_BY_PAGE') or a list of integers.
        document_name (`str`): Name of the document being processed.
        cfg_tokenizer: Tokenizer object used to count tokens (if chunk_unit is 'tokens').
        chunk_unit (`str`): Unit for chunk size ('tokens' or characters).
        chunk_size (`int`): Maximum size of each chunk (in tokens or characters).
        chunk_overlap (`int`): Number of characters/tokens to overlap between chunks.

    Returns:
        A new DataFrame with additional columns like 'Source', 'Modality', 'Part', 'Tokens', and 'Content'.
    """
    from langchain_text_splitters import RecursiveCharacterTextSplitter

    # Define function to determine chunk length based on unit
    length_function = cfg_tokenizer.count_tokens if chunk_unit == "tokens" else len

    # Initialize text splitter with specified parameters
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        length_function=length_function,
        is_separator_regex=False,
    )

    # Audio and Video extensions
    audio_video_extensions = [
        ".mp3",
        ".wav",
        ".flac",
        ".ogg",
        ".amr",
        ".webm",
        ".mp4",
        ".mov",
        ".avi",
    ]

    # Handle different chunking strategies
    if (isinstance(chunking_strategy, List) and len(chunking_strategy) == 0) or (
        chunking_strategy == "ALL"
    ):
        if document_name.endswith(tuple(audio_video_extensions)):
            timestamp_pattern = r"(<CFG_IDENTIFIED_AS_TIMESTAMP_[\d.]+ - [\d.]+>)"
            identified_timestamps = (
                "<CFG_IDENTIFIED_AS_TIMESTAMP_"
                + text_results_df["Divider"].astype(str)
                + ">"
            )
            combined_text = " ".join(
                identified_timestamps
                + text_results_df["Content"].apply(clean_up_string)
            )
            chunks_strings = regex_split_text(combined_text, timestamp_pattern)

            timestamp_ranges, chunks = find_timestamp_indices(
                [chunk for chunk in chunks_strings],
                identified_timestamps.to_list(),
            )

            counter = {}  # initialize the counter
            parts = []  # keep track of the parts list
            for i in timestamp_ranges:
                part = counter.get(i, 0)
                counter[i] = part + 1
                parts.append(part)
        else:
            identified_pages = (
                "<CFG_IDENTIFIED_AS_PAGE_"
                + text_results_df["Divider"].astype(str)
                + ">"
            )
            chunks = text_splitter.create_documents(
                [
                    " ".join(
                        identified_pages
                        + text_results_df["Content"].apply(clean_up_string)
                    )
                ]
            )
            page_numbers, chunks = find_keyword_indices(
                [document.page_content for document in chunks],
                identified_pages.to_list()[1:],
            )
            counter = {}  # initialize the counter
            parts = []  # keep track of the parts list
            for i in page_numbers:
                part = counter.get(i, 0)
                counter[i] = part + 1
                parts.append(part)

    elif (
        isinstance(chunking_strategy, List)
        and len(chunking_strategy) == text_results_df.shape[0]
    ) or (chunking_strategy == "PAGE_BY_PAGE"):
        # Create chunks from each page individually
        chunks = []
        page_numbers = []
        parts = []
        for page_number, page_text in zip(
            text_results_df["Divider"],
            text_results_df["Content"].apply(clean_up_string),
        ):

            page_chunks = text_splitter.create_documents([page_text])

            for part in range(len(page_chunks)):
                chunk = page_chunks[part].page_content
                chunks.append(chunk)
                page_numbers.append(page_number)
                parts.append(part)

    elif all(isinstance(sublist, list) for sublist in chunking_strategy):
        # Raise exception for unsupported chunking strategy not implemented
        # The goal here is to create chunks from specific pages
        raise Exception("Specific chunking strategy is not implemented yet")

    else:
        raise Exception("Chunking strategy is not defined")

    if document_name.endswith(tuple(audio_video_extensions)):
        text_results_df = pd.DataFrame(
            [
                [
                    document_name,
                    "text",
                    timestamp_range,
                    part,
                    cfg_tokenizer.count_tokens(chunk),
                    chunk,
                ]
                for timestamp_range, part, chunk in zip(timestamp_ranges, parts, chunks)
            ],
            columns=["Source", "Modality", "Divider", "Part", "Tokens", "Content"],
        )
        return text_results_df
    else:
        text_results_df = pd.DataFrame(
            [
                [
                    document_name,
                    "text",
                    page_number,
                    part,
                    cfg_tokenizer.count_tokens(chunk),
                    chunk,
                ]
                for page_number, part, chunk in zip(page_numbers, parts, chunks)
            ],
            columns=["Source", "Modality", "Divider", "Part", "Tokens", "Content"],
        )
        return text_results_df


def split_text_semantically(
    text_results_df: pd.DataFrame,
    document_name: str,
    cfg_tokenizer,
) -> pd.DataFrame:
    """
    This function uses Chonky's semantic TextSplitter to break the input text into meaningful chunks.

    Args:
        text_results_df (`pd.DataFrame`): DataFrame containing text data with columns like 'Divider' and 'Content'.
        document_name (`str`): Name of the document being processed.
        cfg_tokenizer: Tokenizer object used to count tokens.

    Returns:
        A new DataFrame with additional columns like 'Source', 'Modality', 'Divider', 'Part', 'Tokens', and 'Content'.
    """
    from chonky import TextSplitter

    # Combine all text into one string, cleaned up
    full_text = " ".join(text_results_df["Content"].apply(clean_up_string))

    # Initialize Chonky's semantic text splitter (uses transformer models under the hood)
    splitter = TextSplitter(device="cpu")

    chunks = splitter(full_text)

    text_results_df = pd.DataFrame(
        [
            [
                document_name,
                "text",
                0,  # No specific page number since it's semantic text splitting [Default - 0]
                part,
                cfg_tokenizer.count_tokens(chunk),
                chunk,
            ]
            for part, chunk in enumerate(chunks)
        ],
        columns=["Source", "Modality", "Divider", "Part", "Tokens", "Content"],
    )

    return text_results_df
