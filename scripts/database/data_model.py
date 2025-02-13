from typing import List, Optional


class NamedEntity:
    def __init__(
        self,
        id: int,
        entity_text: str,
        entity_id: int,
        span_start: int,
        span_end: int,
        document_id: int,
        sentence_index: int,
        summary_id: int,
        intra_doc_fq: int,
        tf: float,
        inter_doc_fq: int,
        tf_idf: float,
        idf: float,
    ):
        self.id = id
        self.entity_text = entity_text
        self.named_entity = entity_id
        self.span_start = span_start
        self.span_end = span_end # Adjust to follow Python slicing convention of [start:end) i.e. exclusive end
        self.document_id = document_id
        self.sentence_index = sentence_index
        self.summary_id = summary_id
        self.intra_doc_fq = intra_doc_fq
        self.tf = tf
        self.inter_doc_fq = inter_doc_fq
        self.tf_idf = tf_idf
        self.idf = idf




class Sentence:
    def __init__(
        self,
        text: str,
        sentence_index: int,
        document_id: int,
        word_count: int,
        token_count: int,
        alpha_count: int,
        entities: Optional[List[NamedEntity]] = None,
        validate_entities: bool = True,
    ):
        self.text = text
        self.sentence_index = sentence_index
        self.document_id = document_id
        self.word_count = word_count
        self.token_count = token_count
        self.alpha_count = alpha_count

        self.entities = entities if entities is not None else []
        if validate_entities:
            self.validate_entities()


    def validate_entities(self):
        """
        1. Ensure that span_start and span_end are non-negative.
        2. Ensure that span_start and span_end are within the bounds of the sentence.
        3. Ensure that span_start is less than span_end.
        4. Ensure that the entity text matches text[span_start->span_end] of the sentence.

        """

        for entity in self.entities:
            if entity.span_start < 0 or entity.span_end < 0:
                raise ValueError("Span start and end must be non-negative.")
            if entity.span_start >= len(self.text) or entity.span_end >= len(self.text):
                raise ValueError("Span start and end must be within the bounds of the sentence.")
            if entity.span_start >= entity.span_end:
                raise ValueError("Span start must be less than span end.")
            if entity.entity_text != self.text[entity.span_start : entity.span_end]:
                raise ValueError(f"Entity text: [{entity.entity_text}] must match the text span [{self.text[entity.span_start : entity.span_end]}] of the sentence.")


class Document:
    def __init__(
        self,
        id: int,
        title: str,
        word_count: int,
        token_count: int,
        alpha_count: int,
        sentences: Optional[List[Sentence]] = None,
    ):
        self.id = id
        self.title = title
        self.word_count = word_count
        self.token_count = token_count
        self.alpha_count = alpha_count
        self.sentences = sentences if sentences is not None else []

    def print_document(self):
        print(f"Document ID: {self.id}")
        print(f"Title: {self.title}")
        print(f"Word Count: {self.word_count}")
        print(f"Token Count: {self.token_count}")
        print(f"Alpha Count: {self.alpha_count}")
        for sentence in self.sentences:
            print(f"\nSentence {sentence.sentence_index}: {sentence.text}")
            for entity in sentence.entities:
                print(f"  Entity: {entity.named_entity}")

    def to_html(self) -> str:
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
        for sentence in self.sentences:
            html += f"<p class='sentence'><span class='sentence-nbr'>Sentence {sentence.sentence_index}:</span> <span class='sentence-text'>{self._highlight_entities(sentence)}</span></p>"
        html += self._generate_entity_table()
        return html

    def _highlight_entities(self, sentence: Sentence) -> str:
        text = sentence.text
        entities = sorted(sentence.entities, key=lambda e: e.span_start)
        offset = 0
        for entity in entities:
            open_tag = f"<span class='entity' data-entity-id='{entity.id}'>"
            close_tag = "</span>"
            start = entity.span_start + offset
            end = entity.span_end + offset
            text = (
                text[:start] + open_tag + text[start:end] + close_tag + text[end:]
            )
            offset += len(open_tag) + len(close_tag)
        return text

    def _generate_entity_table(self) -> str:
        html = "<table class='entity-table'><thead><tr><th>ID</th><th>Text</th><th>Type</th><th>Start</th><th>End</th></tr></thead><tbody>"
        for sentence in self.sentences:
            for entity in sentence.entities:
                html += f"<tr data-entity-id='{entity.id}'><td>{entity.id}</td><td>{entity.entity_text}</td><td>{entity.named_entity}</td><td>{entity.span_start}</td><td>{entity.span_end}</td></tr>"
        html += "</tbody></table>"
        return html
