from typing import List, Optional
from dataclasses import dataclass
from .      schema import *

@dataclass
class NamedEntity:
    id: int
    txt: str
    ne_class: int
    txt_norm: str
    span_start: int
    span_end: int
    doc_id: int
    sent_idx: int
    doc_title: Optional[str] = None
    summary_id: Optional[int] = None
    normalized_entity_text: Optional[str] = None
    intra_doc_fq: Optional[int] = None
    tf: Optional[float] = None
    inter_doc_fq: Optional[int] = None
    tf_idf: Optional[float] = None
    idf: Optional[float] = None
    overlap: bool = False
    pmi: float = None
    error_id: int = None


@dataclass
class Sentence:
    txt: str
    sentence_index: int
    doc_id: int
    word_count: int
    token_count: int
    alpha_count: int
    entities: Optional[List[NamedEntity]] = None
    validation_errors: List[dict] = None

    def __post_init__(self, validate_entities: bool = True):
        """
        1. Ensure that span_start and span_end are non-negative.
        2. Ensure that span_start and span_end are within the bounds of the sentence.
        3. Ensure that span_start is less than span_end.
        4. Ensure that the entity text matches text[span_start->span_end] of the sentence.

        """
        self.validation_errors = []
        if validate_entities:
            for entity in self.entities:
                if entity.span_start < 0 or entity.span_end < 0:
                    self.validation_errors.append(
                        {
                            "entity_id": entity.id,
                            "error": "Span start and end must be non-negative",
                            "entity": entity,
                        }
                    )
                elif (
                    entity.txt != self.txt[entity.span_start : entity.span_end]
                ):
                    self.validation_errors.append(
                        {
                            "entity_id": entity.id,
                            "error": f"Text mismatch: '{entity.txt}' vs '{self.txt[entity.span_start : entity.span_end]}'",
                            "entity": entity,
                        }
                    )
                elif entity.span_start >= entity.span_end:
                    self.validation_errors.append(
                        {
                            "entity_id": entity.id,
                            "error": "Invalid span range",
                            "entity": entity,
                        }
                    )
                elif (
                    entity.txt != self.txt[entity.span_start : entity.span_end]
                ):
                    self.validation_errors.append(
                        {
                            "entity_id": entity.id,
                            "error": f"Text mismatch: '{entity.txt}' vs '{self.txt[entity.span_start : entity.span_end]}'",
                            "entity": entity,
                        }
                    )


@dataclass
class Document:
    id: int
    title: str
    word_count: int
    token_count: int
    alpha_count: int
    sentences: Optional[List[Sentence]] = None

    def __repr__(self) -> str:
        s = f"Document(id={self.id}, title={self.title})"
        s += f"\nsentences: {len(self.sentences)}"
        s += f"\nentities: {sum(len(sentence.entities) for sentence in self.sentences)}"
        s += f"\nWord Count: {self.word_count}"

    def print_document(self):
        print(f"Document ID: {self.id}")
        print(f"Title: {self.title}")
        print(f"Word Count: {self.word_count}")
        print(f"Token Count: {self.token_count}")
        print(f"Alpha Count: {self.alpha_count}")
        for sentence in self.sentences:
            print(f"\nSentence {sentence.sentence_index}: {sentence.txt}")
            for entity in sentence.entities:
                print(f"  Entity: {entity.ne_class}")
                if entity.validation_errors:
                    print(f"    Validation Errors: {entity.validation_errors}")

    def to_html(self) -> str:
        has_errors = any(sentence.validation_errors for sentence in self.sentences)

        html = f"""
        <div class="document-title">
            <h1>{self.title}</h1>
            <div class="document-meta">
                <span>Document ID: {self.id}</span>
                <span>Words: {self.word_count}</span>
                <span>Tokens: {self.token_count}</span>
                <span>Alpha: {self.alpha_count}</span>
            </div>
        </div>
        """

        if has_errors:
            html += """
            <div class="validation-warning">
                <p>⚠️ Some entities have validation errors. These are highlighted in red.</p>
            </div>
            """

        for sentence in self.sentences:
            html += f"""
            <p class='sentence'>
                <span class='sentence-nbr'>Sentence {sentence.sentence_index}:</span>
                <span class='sentence-text'>{self._highlight_entities(sentence)}</span>
            </p>
            """

        html += self._generate_entity_table()
        return html

    def _highlight_entities(self, sentence: Sentence) -> str:
        text = sentence.txt
        entities = sorted(sentence.entities, key=lambda e: e.span_start)
        offset = 0
        for entity in entities:
            open_tag = f"<span class='entity' data-entity-id='{entity.id}'>"
            close_tag = "</span>"
            start = entity.span_start + offset
            end = entity.span_end + offset
            text = text[:start] + open_tag + text[start:end] + close_tag + text[end:]
            offset += len(open_tag) + len(close_tag)
        return text

    def _get_error_message(self, entity_id: int, sentence: Sentence) -> str:
        """Get error message for entity if it exists."""
        for error in sentence.validation_errors:
            if error["entity_id"] == entity_id:
                return error["error"]
        return ""

    def _generate_entity_table(self) -> str:
        html = """
        <table class='entity-table'>
            <thead>
                <tr>
                    <th>ID</th>
                    <th>Text</th>
                    <th>Type</th>
                    <th>Start</th>
                    <th>End</th>
                    <th>Status</th>
                </tr>
            </thead>
            <tbody>
        """

        for sentence in self.sentences:
            error_ids = {err["entity_id"] for err in sentence.validation_errors}
            for entity in sentence.entities:
                error_class = " class='error-row'" if entity.id in error_ids else ""
                error_message = self._get_error_message(entity.id, sentence)
                status = (
                    f"<span class='error-status' title='{error_message}'>⚠️ Error</span>"
                    if entity.id in error_ids
                    else "✓ Valid"
                )

                html += f"""
                <tr data-entity-id='{entity.id}'{error_class}>
                    <td>{entity.id}</td>
                    <td>{entity.txt}</td>
                    <td>{entity.ne_class}</td>
                    <td>{entity.span_start}</td>
                    <td>{entity.span_end}</td>
                    <td>{status}</td>
                </tr>
                """

        html += "</tbody></table>"
        return html
