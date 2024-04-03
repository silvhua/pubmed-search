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

import sys
import os
import requests
from Custom_Logger import *

class Pubmed_API:
    def __init__(self, api_key=os.getenv('api_ncbi'), logger=None, logging_level=logging.INFO):
        self.api_key = api_key
        self.logger = create_function_logger('Pubmed_API', logger, level=logging_level)
        self.iteration = 0
        self.responses_dict = {}
        self.results_dict = {}
        self.PMIDs_dict = {}
        self.record_strings_dict = {}

    def search_article(self, query, query_tag=None, publication=None, reldate=None, retmax=None,
        systematic_only=False, review_only=False, additional_search_params=None, ids_only=False, 
        verbose=True
        ):
        base_url = f'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi'
        if self.api_key:
            base_url += f'&api_key={self.api_key}'
        response = {}
        results = pd.DataFrame()
        search_term = f'{re.sub(r"not", "", query)}'  # Remove 'not' since it will be treated as a boolean
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
            'datetype': 'edat',
        }
        if reldate:
            params['reldate'] = reldate
        if retmax:
            params['retmax'] = retmax
        if additional_search_params:
            params.update(additional_search_params)
        self.logger.info(f'Search term: {search_term}')
        messages = []
        try:
            self.iteration += 1
            response = requests.get(base_url, params=params)
            response_dict = response.json()
            id_list = response_dict['esearchresult']['idlist']
            messages.append(f'{len(id_list)} PMIDs found.')
            if verbose==True:
                messages.append(f'{id_list}')
            self.PMIDs_dict[self.iteration] = id_list
            self.responses_dict[self.iteration] = response_dict
            if ids_only==False:
                results = self.get_article_data_by_title()
            else:
                results = id_list
            self.logger.info('\n'.join(messages))
        except Exception as error:
            error_messages = []
            exc_type, exc_obj, tb = sys.exc_info()
            file = tb.tb_frame
            lineno = tb.tb_lineno
            filename = file.f_code.co_filename
            message = f'\tAn error occurred on line {lineno} in {filename}: {error}'
            error_messages.append(message)
            self.logger.error('\n'.join(error_messages))
        
        return results

    def get_article_data_by_title(self, iteration=None):
        result_df = pd.DataFrame()
        try:
            result_dict = {}
            iteration = self.iteration if iteration == None else iteration
            record_strings_list = self.batch_retrieve_citation(iteration)
            self.record_strings_dict[iteration] = record_strings_list
            for index, record_string in enumerate(record_strings_list):
                result_dict[index] = self.extract_pubmed_details(record_string)
            self.results_dict[iteration] = result_dict
            result_df = pd.DataFrame(result_dict).transpose()
        except Exception as error:
            error_messages = []
            error_messages.append(f'Response: \n{self.PMIDs_dict.get(iteration)}')
            exc_type, exc_obj, tb = sys.exc_info()
            file = tb.tb_frame
            lineno = tb.tb_lineno
            filename = file.f_code.co_filename
            message = f'\tAn error occurred on line {lineno} in {filename}: {error}'
            error_messages.append(message)
            self.logger.error('\n'.join(error_messages))
        return result_df

    def batch_retrieve_citation(self, iteration):
        result_list = []
        messages = []
        try:
            id_list = self.PMIDs_dict.get(iteration)
            if id_list:
                self.logger.info(f'Extracting these {len(id_list)} PMIDs: {id_list}')
                for index, id in enumerate(id_list):
                    result_list.append(self.retrieve_citation(id).decode('utf-8'))
                    current_index, current_id = index+1, id
            else:
                self.logger.warning(f'No results found.')
        except Exception as error:
            messages.append(f'Response: \n{self.responses_dict.get(iteration)}')
            exc_type, exc_obj, tb = sys.exc_info()
            file = tb.tb_frame
            lineno = tb.tb_lineno
            filename = file.f_code.co_filename
            messages.append(f'\tAn error occurred on line {lineno} in {filename}: {error}')
            messages.append(f'Article {current_index} [{current_id}] not found.')
        return result_list

    def retrieve_citation(self, article_id):
        base_url = f'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi'
        if self.api_key:
            base_url += f'&api_key={self.api_key}'
        params = {
            'db': 'pubmed',
            'id': article_id
        }
        response = requests.get(base_url, params=params)
        return response.content

    def extract_pubmed_details(self, record_string):
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

        # Extract PMID
        pmid = re.search(r'<PMID.*?>(.*?)</PMID>', record_string)
        pmid = pmid.group(1) if pmid else ''

        abstract_matches = re.findall(r'(<AbstractText.*?>.*?</AbstractText>)', record_string)
        self.logger.debug(f'Number of abstract sections: {len(abstract_matches)}')
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
            'pmid': pmid,
            'mesh_headings': MeshHeadingList
        }