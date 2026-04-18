"""
Markdown-aware text splitter that splits documents at header boundaries.

This module provides a chunking strategy that respects document structure
by splitting at markdown headers (h1/h2/h3/h4), preserving header hierarchy
in each chunk for better semantic coherence.
"""

import re
from typing import List, Dict, Optional, Tuple


# Markdown header patterns ordered by level
HEADER_PATTERNS = [
    (1, re.compile(r'^#{1}\s+(.+)$', re.MULTILINE)),
    (2, re.compile(r'^#{2}\s+(.+)$', re.MULTILINE)),
    (3, re.compile(r'^#{3}\s+(.+)$', re.MULTILINE)),
    (4, re.compile(r'^#{4}\s+(.+)$', re.MULTILINE)),
]

# HTML header patterns for converted documents
HTML_HEADER_PATTERNS = [
    (1, re.compile(r'<h1[^>]*>(.*?)</h1>', re.IGNORECASE | re.DOTALL)),
    (2, re.compile(r'<h2[^>]*>(.*?)</h2>', re.IGNORECASE | re.DOTALL)),
    (3, re.compile(r'<h3[^>]*>(.*?)</h3>', re.IGNORECASE | re.DOTALL)),
    (4, re.compile(r'<h4[^>]*>(.*?)</h4>', re.IGNORECASE | re.DOTALL)),
]


class MarkdownSplitter:
    """
    Splits text at markdown header boundaries while preserving header context.
    Each chunk includes the parent headers for context continuity.
    """

    def __init__(
        self,
        max_chunk_size: int = 512,
        chunk_overlap: int = 0,
        min_chunk_size: int = 50,
        split_level: int = 2,
    ):
        """
        Args:
            max_chunk_size: Maximum characters per chunk
            chunk_overlap: Number of characters to overlap between chunks
            min_chunk_size: Minimum characters for a chunk (smaller chunks merge with next)
            split_level: Header level to split at (1=h1, 2=h2, 3=h3, 4=h4)
        """
        self.max_chunk_size = max_chunk_size
        self.chunk_overlap = chunk_overlap
        self.min_chunk_size = min_chunk_size
        self.split_level = split_level

    def split(self, text: str) -> List[Dict[str, str]]:
        """
        Split text into chunks at header boundaries.

        Returns:
            List of dicts with keys: 'content', 'header_path', 'section_title'
        """
        if not text or not text.strip():
            return []

        # detect if markdown or html
        is_html = bool(re.search(r'<h[1-4][^>]*>', text, re.IGNORECASE))
        sections = self._extract_sections(text, is_html)

        if not sections:
            # no headers found — fall back to simple splitting
            return self._fallback_split(text)

        chunks = []
        for section in sections:
            content = section['content'].strip()
            if not content:
                continue

            if len(content) <= self.max_chunk_size:
                chunks.append({
                    'content': content,
                    'header_path': section['header_path'],
                    'section_title': section['title'],
                })
            else:
                # split large sections by paragraph
                sub_chunks = self._split_by_paragraph(content, self.max_chunk_size)
                for i, sub in enumerate(sub_chunks):
                    chunks.append({
                        'content': sub,
                        'header_path': section['header_path'],
                        'section_title': section['title'] + (f" (part {i+1})" if len(sub_chunks) > 1 else ""),
                    })

        # merge small chunks with the next
        merged = self._merge_small_chunks(chunks)
        return merged

    def _extract_sections(self, text: str, is_html: bool) -> List[Dict]:
        """Extract sections by splitting at header boundaries."""
        patterns = HTML_HEADER_PATTERNS if is_html else HEADER_PATTERNS

        # find all headers up to split_level
        headers: List[Tuple[int, int, str]] = []  # (level, position, title)
        for level, pattern in patterns:
            if level > self.split_level:
                continue
            for match in pattern.finditer(text):
                title = match.group(1).strip()
                if is_html:
                    title = re.sub(r'<[^>]+>', '', title).strip()
                headers.append((level, match.start(), title))

        if not headers:
            return []

        # sort by position
        headers.sort(key=lambda x: x[1])

        # build sections
        sections = []
        header_stack = {}  # level -> title

        for i, (level, pos, title) in enumerate(headers):
            # update header stack
            header_stack[level] = title
            # clear deeper levels
            for deeper in range(level + 1, 5):
                header_stack.pop(deeper, None)

            # get content between this header and the next
            next_pos = headers[i + 1][1] if i + 1 < len(headers) else len(text)
            content = text[pos:next_pos]

            # build header path
            path_parts = [header_stack[l] for l in sorted(header_stack.keys()) if l in header_stack]
            header_path = " > ".join(path_parts)

            sections.append({
                'title': title,
                'header_path': header_path,
                'content': content,
            })

        # handle content before first header
        first_pos = headers[0][1]
        if first_pos > 0:
            pre_content = text[:first_pos].strip()
            if pre_content:
                sections.insert(0, {
                    'title': 'Introduction',
                    'header_path': 'Introduction',
                    'content': pre_content,
                })

        return sections

    def _split_by_paragraph(self, text: str, max_size: int) -> List[str]:
        """Split text by paragraph boundaries, respecting max size."""
        paragraphs = re.split(r'\n\s*\n', text)
        chunks = []
        current = ""

        for para in paragraphs:
            para = para.strip()
            if not para:
                continue

            if len(current) + len(para) + 2 <= max_size:
                current = current + "\n\n" + para if current else para
            else:
                if current:
                    chunks.append(current)
                if len(para) > max_size:
                    # split long paragraph by sentences
                    sentences = re.split(r'(?<=[.!?])\s+', para)
                    current = ""
                    for sent in sentences:
                        if len(current) + len(sent) + 1 <= max_size:
                            current = current + " " + sent if current else sent
                        else:
                            if current:
                                chunks.append(current)
                            current = sent
                else:
                    current = para

        if current:
            chunks.append(current)

        return chunks if chunks else [text[:max_size]]

    def _merge_small_chunks(self, chunks: List[Dict]) -> List[Dict]:
        """Merge chunks smaller than min_chunk_size with the next chunk."""
        if not chunks:
            return chunks

        merged = []
        pending = None

        for chunk in chunks:
            if pending is not None:
                # merge pending into current
                chunk = {
                    'content': pending['content'] + "\n\n" + chunk['content'],
                    'header_path': chunk['header_path'],
                    'section_title': chunk['section_title'],
                }
                pending = None

            if len(chunk['content']) < self.min_chunk_size:
                pending = chunk
            else:
                merged.append(chunk)

        if pending is not None:
            if merged:
                last = merged[-1]
                merged[-1] = {
                    'content': last['content'] + "\n\n" + pending['content'],
                    'header_path': last['header_path'],
                    'section_title': last['section_title'],
                }
            else:
                merged.append(pending)

        return merged

    def _fallback_split(self, text: str) -> List[Dict[str, str]]:
        """Simple paragraph-based splitting when no headers are found."""
        sub_chunks = self._split_by_paragraph(text, self.max_chunk_size)
        return [
            {
                'content': chunk,
                'header_path': '',
                'section_title': f'Section {i+1}',
            }
            for i, chunk in enumerate(sub_chunks)
        ]
