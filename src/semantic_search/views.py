from django.shortcuts import render
import sys
sys.path.append(r"/home/silvhua/custom_python")
from retrieval_pipeline import *
from django.http import HttpResponse

# Create your views here.
def retrieve(request, query='metabolic syndrome', collection_name='test_set'):
    logger = create_function_logger(__name__, parent_logger=None, level=logging.INFO)
    retriever = Retrieve_Docs(collection_name, logger=logger)
    
    results_list = retriever.run(query)
    logger.info(f'Results list: {results_list}')
    messages = []
    for index, result in enumerate(results_list['retriever_with_embeddings'].get('documents', [])):
        messages.append(f'Result {index+1}: {result}')
    logger.info('\n'.join(messages))
    return HttpResponse(messages)
    # return HttpResponse('Hello')

def index(request):
    return HttpResponse("Hello, world. You're at the semantic search index.")