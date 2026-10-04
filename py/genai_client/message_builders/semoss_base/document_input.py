"""Prepare document attachments for endpoints that need their contents as text.

Docling is loaded only when a binary document needs conversion. Original media
and persisted history are never modified; the provider receives a separate copy.
"""

import base64
import hashlib
import json
from collections import OrderedDict
from dataclasses import dataclass
from io import BytesIO
from pathlib import PurePath
from threading import RLock
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from .media_types import (
    decode_base64_text,
    is_text_mime_type,
    prepare_base64_media,
    resolve_mime_type,
)

if TYPE_CHECKING:
    from .semoss_models import ModelSettings, SEMOSSMediaContent, SEMOSSMessage


DOCUMENT_FORMATS = {
    "application/pdf": "pdf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation": "pptx",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",
}


class DocumentInputError(ValueError):
    """An attachment could not be delivered without losing its contents."""


@dataclass(frozen=True)
class ExtractedDocument:
    text: str
    pictures: int = 0


class DocumentInputProcessor:
    """Bounded, engine-local extraction cache, partitioned by conversation scope."""

    def __init__(
        self,
        *,
        max_file_bytes: int = 20 * 1024 * 1024,
        max_text_chars: int = 1_000_000,
        max_pages: int = 200,
        max_cache_bytes: int = 8 * 1024 * 1024,
    ):
        self.max_file_bytes = max_file_bytes
        self.max_text_chars = max_text_chars
        self.max_pages = max_pages
        self.max_cache_bytes = max_cache_bytes
        self._cache: OrderedDict[tuple[str, str, str], ExtractedDocument] = (
            OrderedDict()
        )
        self._cache_bytes = 0
        self._converter = None
        self._lock = RLock()

    def prepare(
        self,
        messages: list["SEMOSSMessage"],
        settings: "ModelSettings",
        *,
        input_modalities: list[str] | None = None,
        cache_scope: str | None = None,
        source_messages: list[dict] | None = None,
    ) -> list["SEMOSSMessage"]:
        """Replace unsupported input documents, including those in prior turns.

        Engine metadata is authoritative: FILE passes all documents through,
        PDF passes PDFs through, and other documents become text. The old MIME
        setting remains a fallback for callers without engine metadata.
        """
        modalities = (
            {value.strip().upper() for value in input_modalities}
            if input_modalities is not None
            else None
        )
        if modalities is not None:
            if "FILE" in modalities:
                return messages
            native_types = {"application/pdf"} if "PDF" in modalities else set()
        elif settings.native_document_mime_types is not None:
            native_types = {
                resolve_mime_type(mime) for mime in settings.native_document_mime_types
            }
        else:
            return messages
        can_extract = modalities is None or "TEXT" in modalities

        from .semoss_models import SEMOSSMessageType, SEMOSSTextMessagePart

        if source_messages:
            cache_scope = cache_scope or next(
                (m.get("messageId") for m in source_messages if m.get("io") == "INPUT"),
                None,
            )
            # The parts-based parser skips media without data. In extraction
            # mode, fail explicitly instead of sending a question without its file.
            for source in source_messages:
                if source.get("io") != "INPUT":
                    continue
                media_inputs = source.get("mediaInputs") or []
                media_inputs = media_inputs + [
                    part.get("mediaInfo") or part.get("media_info") or {}
                    for part in source.get("parts") or []
                    if part.get("type") == "MEDIA"
                ]
                for media in media_inputs:
                    if (
                        media
                        and not media.get("base64Data")
                        and not media.get("sourceUrl")
                    ):
                        raise DocumentInputError(
                            f"Attachment {media.get('fileName') or 'document'!r} has no readable data. "
                            "Upload the file again."
                        )
        prepared = []
        converted = False
        extracted_chars = 0
        for original in messages:
            message = original.model_copy(deep=True)
            prepared.append(message)
            if message.io != "INPUT":
                continue

            if message.parts:
                parts = []
                for part in message.parts:
                    text = (
                        self._prepare_media(
                            part.media_info, native_types, cache_scope, can_extract
                        )
                        if part.type == "MEDIA"
                        else None
                    )
                    if text is None:
                        parts.append(part)
                    else:
                        parts.append(SEMOSSTextMessagePart(text=text))
                        extracted_chars += len(text)
                        converted = True
                message.parts = parts
            elif message.media_content:
                remaining = []
                text_parts = [message.content] if message.content else []
                for media in message.media_content:
                    text = self._prepare_media(
                        media, native_types, cache_scope, can_extract
                    )
                    if text is None:
                        remaining.append(media)
                    else:
                        text_parts.append(text)
                        extracted_chars += len(text)
                        converted = True
                message.content = "\n\n".join(text_parts)
                message.media_content = remaining or None
                if not remaining and message.type == SEMOSSMessageType.INPUT_MEDIA:
                    message.type = SEMOSSMessageType.INPUT_TEXT

            if extracted_chars > self.max_text_chars:
                raise DocumentInputError(
                    "The attached documents contain too much text for one conversation "
                    f"({self.max_text_chars:,} character limit). Use fewer pages or files."
                )

        if converted:
            self._check_context_budget(prepared, settings)
        return prepared

    def _prepare_media(
        self,
        media: "SEMOSSMediaContent",
        native_types: set[str],
        scope: str | None,
        can_extract: bool,
    ) -> str | None:
        name = media.file_name or PurePath(urlparse(media.url or "").path).name
        name = name or "attachment"
        mime = resolve_mime_type(media.mime_type, name, media.format)
        if media.type == "base64":
            data, mime = prepare_base64_media(media)
        else:
            data = None

        # This setting describes document support, not image/audio/video support.
        if mime.startswith(("image/", "audio/", "video/")):
            return None
        if mime in native_types:
            return None
        if not can_extract:
            raise DocumentInputError(
                "Document extraction requires a model with TEXT input capability."
            )
        if data is None:
            raise DocumentInputError(
                f"Upload {name!r} as a file to extract its text; document URLs "
                "are not fetched by the extraction service."
            )
        if not is_text_mime_type(mime) and mime not in DOCUMENT_FORMATS:
            raise DocumentInputError(
                f"Cannot extract text from {name!r} ({mime}). "
                "Use PDF, DOCX, PPTX, XLSX, CSV, or a plain-text file."
            )
        if len(data) > 4 * ((self.max_file_bytes + 2) // 3):
            raise DocumentInputError(f"{name!r} exceeds the document size limit.")
        try:
            raw = base64.b64decode(data, validate=True)
        except (ValueError, TypeError) as exc:
            raise DocumentInputError(f"{name!r} has invalid attachment data.") from exc
        if len(raw) > self.max_file_bytes:
            raise DocumentInputError(f"{name!r} exceeds the document size limit.")

        if is_text_mime_type(mime):
            try:
                document = ExtractedDocument(decode_base64_text(data))
            except UnicodeError as exc:
                raise DocumentInputError(
                    f"Save {name!r} as UTF-8 text and attach it again."
                ) from exc
        else:
            try:
                document = self._extract(raw, DOCUMENT_FORMATS[mime], scope)
            except DocumentInputError as exc:
                raise DocumentInputError(f"{name!r}: {exc}") from exc

        if not document.text.strip():
            raise DocumentInputError(f"No readable text was found in {name!r}.")
        if len(document.text) > self.max_text_chars:
            raise DocumentInputError(
                f"{name!r} exceeds the {self.max_text_chars:,} extracted-character "
                "limit. Attach a smaller document."
            )
        notice = ""
        if document.pictures:
            notice = (
                f"\nExtraction note: {document.pictures} image(s) were not visually "
                "analyzed. Extracted text does not preserve the visual layout."
            )
        # Encode the filename as a quoted label so embedded newlines cannot alter
        # the attachment header. The contents remain ordinary user input.
        return f"Attached file: {json.dumps(name, ensure_ascii=False)}{notice}\n\n{document.text}"

    def _extract(
        self, raw: bytes, extension: str, scope: str | None
    ) -> ExtractedDocument:
        key = (scope or "", extension, hashlib.sha256(raw).hexdigest())
        with self._lock:
            if scope and key in self._cache:
                self._cache.move_to_end(key)
                return self._cache[key]
            document = self._convert(raw, extension)
            size = len(document.text.encode("utf-8"))
            if (
                scope
                and size <= self.max_cache_bytes
                and len(document.text) <= self.max_text_chars
            ):
                while self._cache and (
                    self._cache_bytes + size > self.max_cache_bytes
                    or len(self._cache) >= 32
                ):
                    _, evicted = self._cache.popitem(last=False)
                    self._cache_bytes -= len(evicted.text.encode("utf-8"))
                self._cache[key] = document
                self._cache_bytes += size
            return document

    def _convert(self, raw: bytes, extension: str) -> ExtractedDocument:
        try:
            from docling.datamodel.base_models import (
                ConversionStatus,
                DocumentStream,
                InputFormat,
            )
            from docling.datamodel.pipeline_options import (
                ConvertPipelineOptions,
                PdfPipelineOptions,
            )
            from docling.document_converter import (
                DocumentConverter,
                ExcelFormatOption,
                PdfFormatOption,
                PowerpointFormatOption,
                WordFormatOption,
            )
            from docling_core.types.doc import ContentLayer
        except ImportError as exc:
            raise DocumentInputError(
                "Document text extraction requires Docling in the model's Python environment."
            ) from exc

        if self._converter is None:
            simple = ConvertPipelineOptions(
                document_timeout=120, enable_remote_services=False
            )
            self._converter = DocumentConverter(
                allowed_formats=[
                    InputFormat.PDF,
                    InputFormat.DOCX,
                    InputFormat.PPTX,
                    InputFormat.XLSX,
                ],
                format_options={
                    InputFormat.PDF: PdfFormatOption(
                        pipeline_options=PdfPipelineOptions(
                            document_timeout=120,
                            enable_remote_services=False,
                        )
                    ),
                    InputFormat.DOCX: WordFormatOption(pipeline_options=simple),
                    InputFormat.PPTX: PowerpointFormatOption(pipeline_options=simple),
                    InputFormat.XLSX: ExcelFormatOption(pipeline_options=simple),
                },
            )
        try:
            result = self._converter.convert(
                DocumentStream(name=f"attachment.{extension}", stream=BytesIO(raw)),
                raises_on_error=False,
                max_num_pages=self.max_pages,
                max_file_size=self.max_file_bytes,
            )
            if result.status != ConversionStatus.SUCCESS:
                raise DocumentInputError(
                    "Document extraction did not complete. The file may be unreadable, "
                    f"password protected, over {self.max_pages} pages, or timed out. "
                    "No partial document was sent to the model."
                )
            doc = result.document
            options = {
                "included_content_layers": {ContentLayer.BODY, ContentLayer.FURNITURE},
                "image_placeholder": "[Image not visually analyzed]",
            }
            if extension in {"pdf", "pptx", "xlsx"} and doc.pages:
                sections = []
                sheet_names = [
                    group.name for group in doc.groups if group.label == "sheet"
                ]
                for page_no in sorted(doc.pages):
                    body = doc.export_to_markdown(page_no=page_no, **options).strip()
                    notes = doc.export_to_markdown(
                        page_no=page_no,
                        included_content_layers={ContentLayer.NOTES},
                    ).strip()
                    if notes:
                        body += f"\n\n### Notes\n\n{notes}"
                    if body:
                        label = "Slide" if extension == "pptx" else "Page"
                        if extension == "xlsx":
                            label = "Sheet"
                            if page_no <= len(sheet_names):
                                body = f"### {sheet_names[page_no - 1]}\n\n{body}"
                        sections.append(f"## {label} {page_no}\n\n{body}")
                text = "\n\n".join(sections)
            else:
                options["included_content_layers"].add(ContentLayer.NOTES)
                text = doc.export_to_markdown(**options).strip()
            if not doc.texts and not doc.tables:
                raise DocumentInputError(
                    "No readable text or tables were found in the document. "
                    "Provide a searchable document or run OCR first."
                )
            return ExtractedDocument(text=text, pictures=len(doc.pictures))
        except DocumentInputError:
            raise
        except Exception as exc:
            raise DocumentInputError(
                "Unable to extract the document. Check that it opens correctly and "
                "that Docling's local model artifacts are available."
            ) from exc

    @staticmethod
    def _check_context_budget(
        messages: list["SEMOSSMessage"], settings: "ModelSettings"
    ) -> None:
        """Conservative text-byte preflight; the endpoint's tokenizer is authoritative.

        Include history, system text, tool definitions/results and the requested
        output allowance. Never silently truncate attachments or conversation.
        Native media token costs remain the serving endpoint's responsibility.
        """
        if not settings.context_window:
            return
        params = {**messages[-1].param_map, **(settings.global_param_override or {})}
        output_tokens = next(
            (
                params[key]
                for key in (
                    "max_tokens",
                    "max_completion_tokens",
                    "max_output_tokens",
                    "max_new_tokens",
                )
                if params.get(key) is not None
            ),
            settings.max_tokens or 0,
        )
        text_bytes = 256 + 32 * len(messages)
        for message in messages:
            payload = message.model_dump(mode="json", exclude={"media_content"})
            if payload.get("parts"):
                payload["parts"] = [
                    part for part in payload["parts"] if part["type"] != "MEDIA"
                ]
            text_bytes += len(json.dumps(payload, ensure_ascii=False).encode("utf-8"))
        if text_bytes + int(output_tokens) > settings.context_window:
            raise DocumentInputError(
                "The extracted documents and conversation exceed the conservative "
                "text budget for this model. Attach fewer pages, start a new conversation, "
                "or reduce the requested output length. No content was truncated."
            )
