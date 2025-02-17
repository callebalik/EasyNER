from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class NamedEntity(Base):
    __tablename__ = 'named_entities'
    id = Column(Integer, primary_key=True)
    entity_text = Column(String)
    named_entity = Column(Integer)
    span_start = Column(Integer)
    span_end = Column(Integer)
    document_id = Column(Integer, ForeignKey('documents.id'))
    sentence_index = Column(Integer)
    summary_id = Column(Integer)
    intra_doc_fq = Column(Integer)
    tf = Column(Float)
    inter_doc_fq = Column(Integer)
    tf_idf = Column(Float)
    idf = Column(Float)
    overlap = Column(Boolean, default=False)
    pmi = Column(Float, nullable=True)
    error_id = Column(Integer, nullable=True)

class Sentence(Base):
    __tablename__ = 'sentences'
    id = Column(Integer, primary_key=True)
    text = Column(String)
    sentence_index = Column(Integer)
    document_id = Column(Integer, ForeignKey('documents.id'))
    word_count = Column(Integer)
    token_count = Column(Integer)
    alpha_count = Column(Integer)
    entities = relationship('NamedEntity', backref='sentence')

class Document(Base):
    __tablename__ = 'documents'
    id = Column(Integer, primary_key=True)
    title = Column(String)
    word_count = Column(Integer)
    token_count = Column(Integer)
    alpha_count = Column(Integer)
    sentences = relationship('Sentence', backref='document')