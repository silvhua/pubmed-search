from haystack import Pipeline
from haystack.components.preprocessors import DocumentCleaner, DocumentSplitter
from haystack.components.embedders import SentenceTransformersDocumentEmbedder
from haystack.components.writers import DocumentWriter
from haystack.document_stores.types import DuplicatePolicy

from haystack import Document
from haystack_integrations.document_stores.chroma import ChromaDocumentStore
import sys
sys.path.append(r"/home/silvhua/custom_python")
from silvhua import *
from Custom_Logger import *

def create_indexing_pipeline(document_store, metadata_fields=None):
    """
    Sample notebook: https://colab.research.google.com/github/deepset-ai/haystack-tutorials/blob/main/tutorials/39_Embedding_Metadata_for_Improved_Retrieval.ipynb#scrollTo=nAE4fVvsALXm
    """
    document_cleaner = DocumentCleaner()
    document_splitter = DocumentSplitter(split_by="sentence", split_length=2)
    document_embedder = SentenceTransformersDocumentEmbedder(
        # model="thenlper/gte-large", 
        meta_fields_to_embed=metadata_fields
    )
    document_writer = DocumentWriter(document_store=document_store, policy=DuplicatePolicy.OVERWRITE)

    indexing_pipeline = Pipeline()
    indexing_pipeline.add_component("cleaner", document_cleaner)
    indexing_pipeline.add_component("splitter", document_splitter)
    indexing_pipeline.add_component("embedder", document_embedder)
    indexing_pipeline.add_component("writer", document_writer)

    indexing_pipeline.connect("cleaner", "splitter")
    indexing_pipeline.connect("splitter", "embedder")
    indexing_pipeline.connect("embedder", "writer")

    return indexing_pipeline

class Index_Docs():

    def __init__(
            self, json_filename, json_filepath, content_key='abstract', meta_keys=[
                'article_title', 'journal',
                # 'keywords', 'mesh_headings', 'major_topics'
                ], logger=None, logging_level=logging.INFO
            ):
        self.logger = create_function_logger(__name__, parent_logger=logger, level=logging_level)
        dictionary_list = load_json(json_filename, json_filepath)
        raw_docs = []
        self.logger.info(f'***Instantiating `Index_Docs`***')
        for dictionary in dictionary_list:
            doc = Document(content=dictionary[content_key], meta={key: dictionary[key] for key in meta_keys})
            raw_docs.append(doc)
        self.raw_docs = raw_docs
        self.logger.info(f'Initialized Index_Docs with {len(raw_docs)} documents')
    
    def run_pipeline(self, indexing_pipeline, first_component_name='cleaner'):
        self.logger.info(f'***Running indexing pipeline***')
        indexing_pipeline.run({first_component_name: {"documents": self.raw_docs}})

if __name__ == "__main__":
    logger = create_function_logger(__name__, parent_logger=None, level=logging.INFO)
    logger.info(f'System arguments: {sys.argv[1:]}')
    filename = sys.argv[1] if len(sys.argv) > 1 else 'pubmed_results_2024-04-06_235718.json'
    collection_name = sys.argv[2] if len(sys.argv) > 2 else 'test_set'
    filepath = sys.argv[3] if len(sys.argv) > 3 else '/home/silvhua/repositories/pubmed-search/data/'
    document_store = ChromaDocumentStore( # https://docs.haystack.deepset.ai/reference/integrations-chroma#chromadocumentstore
        collection_name=collection_name, 
        persist_path='../data/processed/'
        )
    
    metadata_fields = ['article_title', 'journal']
    
    indexer = Index_Docs(
        filename, filepath, meta_keys=metadata_fields
        )
    indexing_pipeline = create_indexing_pipeline(
        document_store, metadata_fields=metadata_fields
        )
    indexer.run_pipeline(indexing_pipeline, first_component_name='cleaner')
    logger.info(f'Finished indexing pipeline')