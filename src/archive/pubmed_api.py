import sys
sys.path.append(r"/home/silvhua/custom_python")
import os
import pandas as pd
import string
import re
import requests
# from article_processing import create_text_dict_from_folder
# from orm_summarize import *
api_key = os.getenv('api_ncbi') # Pubmed API key

def retrieve_citation(article_id, api_key):
    """
    Retrieve article metadata from PubMed database.
    """
    base_url = f'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi'
    if api_key:
        base_url += f'&api_key={api_key}'
    params = {
        'db': 'pubmed',
        'id': article_id
    }

    response = requests.get(base_url, params=params)
    return response.content

def search_article(
        query, api_key, query_tag=None, publication=None, reldate=None, retmax=None,
        systematic_only=False, review_only=False, verbose=False,
        additional_search_params=None
        ):
    """
    Search for article title in PubMed database.

    Parameters:
    - title (str): article title
    - api_key (str): NCBI API key
    - reldate (int): the search returns only those items that have a date specified by datetype within the last n days.

    Returns:
    response (str): Article metadata from PubMed database if present. Otherwise, returns list of PMIDs.

    API documentation: https://www.ncbi.nlm.nih.gov/books/NBK25499/#chapter4.ESearch
    Pubmed User Guide including tags for filtering results: https://pubmed.ncbi.nlm.nih.gov/help/
    """
    base_url = f'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi'
    data = {}
    if api_key:
        base_url += f'&api_key={api_key}'
    search_term = f'"{re.sub(r"not", "", query)}"' # Remove 'not' since it will be treated as a boolean
    if query_tag:
        search_term += f'{query_tag}'
    if publication:
        search_term = f'AND {publication} [ta]'
    if systematic_only:
        search_term += ' AND systematic[sb]'
    elif review_only:
        search_term += ' AND (systematic[sb] OR review[pt])'
    params = {
        'db': 'pubmed',
        'term': search_term,
        'retmax': 5,
        'retmode': 'json',
        'datetype':'edat',
    }
    if reldate:
        params['reldate'] = reldate
    if retmax:
        params['retmax'] = retmax
    if additional_search_params:
        params.update(additional_search_params)
    print(f'Search term: {search_term}')

    response = requests.get(base_url, params=params)
    data = response.json()
    return data

def batch_retrieve_citation(data):
    result_list = []
    try:
        id_list = data['esearchresult']['idlist']
        if id_list:
            print(f'Extracting these {len(id_list)} PMIDs: {id_list}')
            for index, id in enumerate(id_list):
                result_list.append(retrieve_citation(id, api_key).decode('utf-8'))
                current_index, current_id = index+1, id
        else:
            print(f'No results found.')
                
    except Exception as error: 
        print(f'Response: \n{data}')
        exc_type, exc_obj, tb = sys.exc_info()
        file = tb.tb_frame
        lineno = tb.tb_lineno
        filename = file.f_code.co_filename
        print(f'\tAn error occurred on line {lineno} in {filename}: {error}')    
        print('Article {current_index} [{current_id}] not found.')
    return result_list

def extract_pubmed_details(record_string):
    """
    Helper function called by `pubmed_details_by_title` to parse article metadata from PubMed database.
    """
    authors = re.findall(r'<Author ValidYN="Y".*?><LastName>(.*?)</LastName><ForeName>(.*?)</ForeName>', record_string)
    formatted_authors = ', '.join(['{} {}'.format(author[1], author[0]) for author in authors])

    # Extract publication year
    publication_year = re.search(r'<PubDate><Year>(\d{4})</Year>', record_string)
    publication_year = publication_year.group(1) if publication_year else ''
    publication_month = re.search(r'<PubDate>.*?<Month>(Aug)</Month>.*?</PubDate>', record_string)
    publication_month = publication_month.group(1) if publication_month else ''

    # Extract article title
    article_title = re.search(r'<ArticleTitle>(.*?)</ArticleTitle>', record_string)
    article_title = article_title.group(1) if article_title else ''

    # Extract journal title
    journal_title = re.search(r'<Title>(.*?)</Title>', record_string)
    journal_title = journal_title.group(1) if journal_title else ''

    # Extract journal volume
    journal_volume = re.search(r'<Volume>(.*?)</Volume>', record_string)
    journal_volume = journal_volume.group(1) if journal_volume else ''

    # Extract journal issue
    journal_issue = re.search(r'<Issue>(.*?)</Issue>', record_string)
    journal_issue = journal_issue.group(1) if journal_issue else ''

    # Extract start page
    start_page = re.search(r'<StartPage>(.*?)</StartPage>', record_string)
    start_page = start_page.group(1) if start_page else ''

    # Extract end page
    end_page = re.search(r'<EndPage>(.*?)</EndPage>', record_string)
    end_page = end_page.group(1) if end_page else ''

    # Extract ELocationID
    doi = re.search(r'<ELocationID.*?EIdType="doi".*?>(.*?)</ELocationID>', record_string)
    doi = doi.group(1) if doi else ''

    abstract_matches = re.findall(r'(<AbstractText.*?>.*?</AbstractText>)', record_string)
    print(f'Number of abstract sections: {len(abstract_matches)}')
    if len(abstract_matches) > 1:
        cleaned_abstract_sections = []
        for match in abstract_matches:
            clean_match = re.sub(r'<AbstractText.*?((?:Label=".*")?.*?>.*)</AbstractText>', r'\1', match)
            clean_match = re.sub(r'(?: Label="(.*?)")?.*?>(.*)', r'\1: \2', clean_match)
            cleaned_abstract_sections.append(clean_match)
            
        abstract = ''.join([f'{group}<br>' for group in cleaned_abstract_sections])
    else:
        abstract = re.sub(r'<AbstractText.*?>(.*?)</AbstractText>', r'\1', abstract_matches[0])  if abstract_matches else ''
        
    # Extract MeshHeadingList
    MeshHeadingList = re.search(r'<MeshHeadingList>(.*?)</MeshHeadingList>', record_string)
    MeshHeadingList = MeshHeadingList.group(1) if MeshHeadingList else ''
    
    return {
        'pubmed_title': article_title,
        'abstract': abstract,
        'journal': journal_title,
        'authors': formatted_authors,
        'year': publication_year,
        'month': publication_month,
        'pub_volume': journal_volume,
        'pub_issue': journal_issue,
        'start_page': start_page,
        'end_page': end_page,
        'doi': doi,
        'mesh_headings': MeshHeadingList
    }

def pubmed_details_by_title(api_response={}, record_strings_list=[], **kwargs):
    """
    Search for article title in PubMed database and return article details.

    Parameters:
    - api_response (dict)
    - record_strings_list (list): List of record strings from `retrieve_citation()`.
    - **kwargs: Parameters to pass to the `search_article()` function.

    Returns:
    article_details (dict): Article metadata from PubMed database if present. 
    """
    result = api_response
    try:
        if api_response==None:
            api_response = search_article(**kwargs)
            result = api_response
        
        result_dict = {}
        if len(record_strings_list) == 0:
            record_strings_list = batch_retrieve_citation(api_response)
            result = record_strings_list
        for index, record_string in enumerate(record_strings_list):
            result_dict[index] = extract_pubmed_details(record_string)
        result = result_dict

    except Exception as error: 
        print(f'Response: \n{api_response}')
        exc_type, exc_obj, tb = sys.exc_info()
        file = tb.tb_frame
        lineno = tb.tb_lineno
        filename = file.f_code.co_filename
        message = f'\tAn error occurred on line {lineno} in {filename}: {error}'
        print(message) 
    return result