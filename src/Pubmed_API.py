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
from table_mapping import concat_columns

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

    def extract_pubmed_details_df(self, iteration=None):
        """
        Extract the Pubmed article details for the given list of record strings for the given iteration.

        Returns:
        DataFrame of the Pubmed article details.
        """
        df = pd.DataFrame()
        record_strings = pd.Series(self.record_strings_dict.get(iteration if iteration else self.iteration))
        regex_dict = {
            'article_title': r'<ArticleTitle>(.*?)</ArticleTitle>',
            'pmid': r'<PMID.*?>(.*?)</PMID>',
            'journal': r'<Title>(.*?)</Title>',
            'volume': r'<Volume>(.*?)</Volume>',
            'issue': r'<Issue>(.*?)</Issue>',
            'year': r'<PubDate><Year>(\d{4})</Year>',
            'month': r'<PubDate>.*?<Month>(Aug)</Month>.*?</PubDate>',
            'start_page': r'<StartPage>(.*?)</StartPage>',
            'end_page': r'<EndPage>(.*?)</EndPage>',
            'doi': r'<ELocationID.*?EIdType="doi".*?>(.*?)</ELocationID>',
        }
        for column, regex in regex_dict.items():
            df[column] = record_strings.str.extract(regex)
        df['abstract'] = self.df_extractall(
            record_strings, parent_regex=r'<Abstract>(.*?)</Abstract>',
            regex = r'<AbstractText.*?(?: Label="(.*?)")?.*?>(.*?)</AbstractText>',
            logger=self.logger, sep=': ', join_strings=' '
        )
        df['mesh_headings'] = self.df_extractall(
            record_strings, 
            parent_regex=r'<MeshHeadingList>(.*?)</MeshHeadingList>',
            regex=r'<MeshHeading><DescriptorName.*?>(.*?)</DescriptorName>(<QualifierName.*?>.*?</QualifierName>)?</MeshHeading>',
            nested_regex=r'<QualifierName.*?>(.*?)</QualifierName>', logger=self.logger
        )
        df['authors'] = self.df_extractall(
            record_strings, sep=' ',
            regex=r'<Author ValidYN="Y".*?><LastName>(.*?)</LastName><ForeName>(.*?)</ForeName>',
            logger=self.logger 
        )
        df['keywords'] = self.df_extractall(
            record_strings, parent_regex=r'<KeywordList.*?>(.*?)</KeywordList>',
            regex=r'<Keyword.*?>(.*?)</Keyword>', 
            logger=self.logger
        )
        df['major_topics'] = self.df_extractall(
            record_strings, 
            regex=r'<[^>]*MajorTopicYN="Y"[^>]*>([^<]+)<\/[^>]+>', 
            logger=self.logger
        )
        df['publication_type'] = self.df_extractall(
            record_strings, parent_regex=r'<PublicationTypeList.*?>(.*?)</PublicationTypeList>',
            regex=r'<PublicationType.*?>(.*?)</PublicationType>', 
            logger=self.logger
        )
        columns = [
            'article_title',
            'abstract',
            'mesh_headings',
            'keywords',
            'major_topics',
            'pmid',
            'doi',
            'journal',
            'volume',
            'issue',
            'year',
            'month',
            'start_page',
            'end_page',
            'authors',
            'publication_type'
        ]
        return df[columns]

    def df_extractall(self, 
            series, regex, parent_regex=None, nested_regex=None, sep=[' ', ' / '], 
            join_strings=False, logger=None
            ):
        """
        Helper function called by `.search_article()` and `.get_article_data_by_title()` to parse 
        article metadata from PubMed database.

        Parameters:
        - series: pd.Series
        - regex: Regular expression to extract from the series.
        - parent_regex (optional): Regular expression from which to extract the `regex`.
            If None, `regex` will be extracted from the series.
        - nested_regex (optional): Regular expression that is nested within `regex` to extract.
        - sep (str or list; optional): String or list of strings used to separate multiple capture groups.
            If it is a list, then the first value is used to separate the main capture groups. 
            The second value is used to separate the nested capture groups. If the nested regex 
            has multiple capture groups, then the last value is used to separate them.
        - join_strings (optional): Boolean indicating whether to join the extracted values.
        - logger (optional): Instance of Custom_Logger class.

        Returns:
        - pd.Series with the extracted values.
        """
        logger = create_function_logger('df_extractall', logger)
        messages = []
        messages.append(f'***Running `df_extractall` with regex {regex}***')
        if parent_regex:
            messages.append(f'\tparent_regex: {parent_regex}')
        if nested_regex:
            messages.append(f'\tnested_regex: {nested_regex}')
        if parent_regex:
            extracted = series.str.extract(parent_regex, expand=False)
            series = extracted
        extracted = series.str.extractall(regex).replace({np.nan: ''})
        if extracted.shape[1] >= 1:
            joined_values = extracted[0]
        else:
            messages.warning('No matches found.')
            return series
        if extracted.shape[1] > 1:
            extracted.index.names = [f'{name if name else "index"}{index if index !=0 else ""}' for index, name in enumerate(extracted.index.names)]
            for i in range(1, extracted.shape[1]):
                if nested_regex:
                    matches = extracted[i].str.extractall(nested_regex)#.replace({np.nan: ''})
                    messages.append(f'Number of nested capture groups: {matches.shape[1]}')
                    matches.columns = [f'nested_text{column}' for column in matches.columns]
                    regex_df = extracted.merge(
                        matches, how='left', left_index=True, right_index=True
                    ).replace({np.nan: ''})
                    nested_separator = sep if type(sep) == str else sep[1]
                    if i == 1:
                        root_column = 0 
                        capture_group_separator = nested_separator
                    else:
                        root_column = 'Text'
                        capture_group_separator = sep if type(sep) == str else sep[-1]
                    regex_df = concat_columns(
                        regex_df, [root_column, 'nested_text0'], 'Text', 
                        sep=capture_group_separator
                    )
                    joined_values = regex_df['Text']

                else:
                    separator = sep if type(sep) == str else sep[0]
                    joined_values = joined_values + separator + extracted[i]
        new_series = joined_values.groupby(level=0).apply(lambda groupby: [match for match in groupby])
        if (type(join_strings) == str) | (join_strings == True):
            new_series = new_series.apply(lambda x: f'{join_strings if type(join_strings) == str else " "}'.join(x))
        logger.debug('\n'.join(messages))
        return new_series

    def extract_pubmed_details(self, record_string):
        """
        [Archived: Use `extract_pubmed_details_df` instead to perform regex operations on the entire dataframe.]
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
        # self.logger.debug(f'Number of abstract sections: {len(abstract_matches)}')
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

        # Estract MeshHeading text and any QualifierName
        mesh_headings = []
        pattern = r'<MeshHeading><DescriptorName.*?>(.*?)</DescriptorName>(<QualifierName.*?>.*?</QualifierName>)?</MeshHeading>'
        matches = re.findall(pattern, MeshHeadingList)
        for match in matches:
            heading = match[0]
            if match[1]: # Estract Mesh QualifierName                
                MeshQualifiers = re.findall(
                    r'<QualifierName.*?>(.*?)</QualifierName>', match[1]
                    )
                print(f'mesh qualifiers: {MeshQualifiers}')
                for qualifier in MeshQualifiers:
                    heading = f"{match[0]} / {qualifier}"
                    mesh_headings.append(heading)
            else:
                mesh_headings.append(heading)

        # Extract keyword
        Keyword_List = re.search(r'<KeywordList.*?>(.*?)</KeywordList>', record_string)
        Keyword_List = Keyword_List.group(1) if Keyword_List else ''
        Keywords = re.findall(
            r'<Keyword.*?>(.*?)</Keyword>', Keyword_List
            )
        # Extract MajorTopic text
        MajorTopics = re.findall(
            r'<[^>]*MajorTopicYN="Y"[^>]*>([^<]+)<\/[^>]+>', record_string
            )
        # Extract Publication Type
        PublicationTypeList = re.search(r'<PublicationTypeList.*?>(.*?)</PublicationTypeList>', record_string)
        PublicationTypeList = PublicationTypeList.group(1) if PublicationTypeList else ''
        PublicationType = re.findall(
            r'<PublicationType.*?>(.*?)</PublicationType>', PublicationTypeList
            )
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
            'mesh_headings': mesh_headings,
            'keywords': Keywords,
            'major_topics': MajorTopics,
            'publication_type': PublicationType
        }