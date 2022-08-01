


class QueryGenerator:
    """
        This is generic query generator class for ES queries
    """
    
    def __init__(self):
        self.query = {}
        self.create_new_query()
                
    def create_new_query(self, pagination=None, sort=None):
        """Create New Query

        Args:
            pagination (List, optional): from and size values. Defaults to None.
            sort (Dict, optional): Dictionary of sort attributes and order. Defaults to None.
        """
        query = {}

        if pagination is not None:
            query['from'] = pagination[0]
            query['size'] = pagination[1]

        if sort is not None:
            query['sort'] = sort

        query['query'] = {}

        self.query = query
        
       
    def create_match_all_query(self):
        return {"match_all": {}}
        
    
    def append_sort_query(self, fields, orders):
        """Given Fields and corresponding sort orders, append the sort query with parent query

        Args:
            fields (List): List of fields to sort on
            orders (List): Which orders to sort on

        Raises:
            Exception: Lenght mismatch between fields and orders
        """
        if len(fields) != len(orders):
             raise Exception("Length mismatch in parmaeters - fields {} and orders {}".format(fields, orders))
                
        sort_query = []

        for num in range(len(fields)):
            query = {}
            query[fields[num]] = {'order':orders[num]}

            sort_query.append(query)

        if 'sort' in self.query.keys():
            self.query['sort'].extend(sort_query)
        else:
            self.query['sort'] = sort_query
            
            
    def append_pagination(self, from_value, size_value):
        """Append Pagination to parent query

        Args:
            from_value (Integer): from 
            size_value (Integer): Size
        """
        self.query['from'] = from_value
        self.query['size'] = size_value
        
            
    def append_to_bool_query(self, parent_query, queries_to_append, clause):
        """append given queries to parent using bool composition and given clause

        Args:
            parent_query (dict): Parent query to append to
            queries_to_append (list): List of queries to combine
            clause (String): Which clause to use while composition - filter, should etc.
        """
        if 'bool' not in parent_query.keys():
            parent_query['bool'] = {}

        if clause not in parent_query['bool'].keys():
            parent_query['bool'][clause] = []

        for query in queries_to_append:
            parent_query['bool'][clause].append(query)
            
    
    def create_composite_query(self, queries_to_append, composite_clause):
        """Create a composite query using given composite clause

        Args:
            queries_to_append (list): List of queries to combine
            composite_clause (String): Clause - filter, should etc

        Returns:
            dict: resultant query
        """
        comp_query = {}
        comp_query[composite_clause] = []
        
        for query in queries_to_append:
            comp_query[composite_clause].append(query)
            
        return {'bool' : comp_query}
    
    
    def create_term_query(self, field, value, boost=None):
        """Create a leaf query with term clause

        Args:
            field (String): Field to apply term query on
            value (any): Value to match the field to
            boost (float, optional): boost value for term query. Defaults to None.

        Returns:
            dict: resultant query
        """
        term = {}
        term[field] = {}

        term[field]['value'] = value
        if boost is not None:
            term[field]['boost'] = boost

        return {'term' : term}
    
    
    def create_match_query(self, field, value, analyzer=None, fuzziness=None, max_expansions=None, prefix_length=None, operator=None, minimum_should_match=None):
        """Create a leaf query with match clause

        Args:
            field (String): Field to create match query on
            value (any): value to match

        Returns:
            dict: resultant query
        """
        match_query = {}
        match_query[field] = {}

        match_query[field]['query'] = value

        if analyzer is not None:
            match_query[field]['analyzer'] = analyzer

        if fuzziness is not None:
            match_query[field]['fuzziness'] = fuzziness

        if max_expansions is not None:
            match_query[field]['max_expansions'] = max_expansions

        if prefix_length is not None:
            match_query[field]['prefix_length'] = prefix_length

        if operator is not None:
            match_query[field]['operator'] = operator

        if minimum_should_match is not None:
            match_query[field]['minimum_should_match'] = minimum_should_match


        return {'match':match_query}
    

    def create_range_query(self, field, gt=None, gte=None, lt=None, lte=None, date_format=None, boost=None):
        """Create leaf query with range clause

        Args:
            field (String): Field to create query on
            gt (any, optional): greater than. Defaults to None.
            gte (any, optional): greate than or equals. Defaults to None.
            lt (any, optional): less than. Defaults to None.
            lte (any, optional): less than or equals. Defaults to None.
            date_format (string, optional): date format to use. Defaults to None.
            boost (float, optional): boost value. Defaults to None.

        Raises:
            Exception: Filter value at least one is required 

        Returns:
            dict: combined query
        """
        if gt is None and gte is None and lt is None and lte is None:
            raise Exception("No filter value provided")
            
        range_query = {}
            
        if gt is not None:
            range_query['gt'] = gt
            
        if gte is not None:
            range_query['gte'] = gte
            
        if lt is not None:
            range_query['lt'] = lt
            
        if lte is not None:
            range_query['lte'] = lte
            
        if date_format is not None:
            range_query['format'] = date_format
            
        if boost is not None:
            range_query['boost'] = boost
            
        out_query = {}
        out_query['range'] = {}
        out_query['range'][field] = range_query
        
        return out_query


    # query_type is 'term' or 'match'
    def create_nested_query(self, field, value, query_type):
        """create leaf query with nested field

        Args:
            field (String): nested field to create query on
            value (any): value matching the field
            query_type (String): query type to use ex - term, match

        Returns:
            dict: combined query
        """
        child_query = {}

        if query_type == 'term':
            child_query = self.create_term_query(field, value)
        if query_type == 'match':
            child_query = self.create_match_query(field, value)

        query = {}
        query['query'] = child_query
        query['path'] = field.split('.')[0]

        return {'nested': query}
        
        
    def create_exist_query(self, is_exist, field):
        """create new exist query

        Args:
            is_exist (bool): exist condition value
            field (String): Field to apply condition on

        Returns:
            dict: resultant query
        """
        exist_query = {}
        exist_query['field'] = field

        exist_query =  {'exists' : exist_query}

        if is_exist:
            return {'bool' :  {'must': exist_query} }
        else:
            return {'bool' : {'must_not': exist_query}}


    
    def __helper_combine_queries_in_stack(self, query_stack):
        """Helper function for complex operand queries AND/OR simplification
           Cobined all the queries in stack till occurence of '('

        Args:
            query_stack (list): Stack consisting of the queries
        """
        operand_arr = []
        while True:
            top = query_stack.pop()
            if top == '(':
                break
            operand_arr.append(top)
            
        operator = query_stack.pop()
        
        if operator == '|':
            query_stack.append(self.create_composite_query(operand_arr, 'should'))
            
        if operator == '&':
            query_stack.append(self.create_composite_query(operand_arr, 'must'))

    
    # supported values for field_mapping_type are term, match, nested-term, nested-match
    def __helper_create_query_with_next_operand(self,query_stack, query_string, field, field_mapping_type):
        operand = ''
        for char in query_string:
            if char == ',' or char == ')':
                break
            operand = operand + char        

        if field_mapping_type=='match':
            query = self.create_match_query(field, operand)
            
        if field_mapping_type=='term':
            query = self.create_term_query(field, operand)

        if field_mapping_type.startswith('nested'):
            query = self.create_nested_query(field, operand, field_mapping_type.split('-')[1])
            
        query_stack.append(query)

        return operand


    """
        Test cases 
        1) 'abc'
        2) 'OR (abc, def)'
        3) 'AND (a1, a2, OR (a3,a4), AND(a5,a6))'
    """
    def convert_logical_operation_to_query(self, query_string, field, field_mapping_type):
        """Convert queries with logical operations - AND OR to ES queries

        Args:
            query_string (String): Input query string containing logical operators
            field (String): Field to apply condition on
            field_mapping_type (String): ex - match, term, nested-match etc

        Returns:
            dict: Combined query
        """
        
        query_stack = []
        
        query_string = query_string.replace('OR', '|')
        query_string = query_string.replace('AND', '&')
        
        while len(query_string) > 0:
            query_string = query_string.strip()
            
            # Operands are inserted in stack directly
            if (query_string.startswith('|')):
                query_stack.append('|')
                query_string = query_string[1:]
                
            # Operands are inserted in stack directly
            elif (query_string.startswith('&')):
                query_stack.append('&')
                query_string = query_string[1:]
            
            # Opening brackets are inserted in stack directly
            elif (query_string.startswith('(')):
                query_stack.append('(')
                query_string = query_string[1:]
                
            # Ignore ',' at the start of string
            elif (query_string.startswith(',')):
                query_string = query_string[1:]
                
            # For closing bracket, pop from stack and combine till occurence of opening bracket
            elif (query_string.startswith(')')):
                self.__helper_combine_queries_in_stack(query_stack)
                query_string = query_string[1:]
                        
            # Actual attribute for leaf level query
            else:
                operand = self.__helper_create_query_with_next_operand(query_stack, query_string, field, field_mapping_type)
                query_string = query_string[len(operand):]

        return query_stack.pop()


    # Aggregation related functions

    def __create_nested_aggregation(self, agg_name, field_name, agg):
        """Function to create nested aggregation

        Args:
            agg_name (String): Name of aggregation
            field_name (String): Name of field to apply aggreagation on
            agg (dict): nested aggregation created

        Returns:
            [type]: [description]
        """
        path = field_name.split(".")[0]

        nested_agg = {}
        nested_agg[agg_name] = {}
        nested_agg[agg_name]['nested'] = {
            "path" : path
        }

        nested_agg[agg_name]['aggs'] = agg
        return nested_agg


    def create_aggregation_basic(self, agg_name, agg_clause, field_name, agg_params):
        """Create basic aggreagations

        Args:
            agg_name (String): Name of the aggregation
            agg_clause (String): Aggreagation clause to use - terms, value_counts, min, max
            field_name (String): Field name to apply aggreagation on
            agg_params (dict): Aggregation clause parametrs - ex - 'interval' in case of histogram

        Returns:
            dict: combined aggregations
        """
        is_nested = False
        if 'nested' in agg_params.keys():
            is_nested = True
            field_name = field_name + '.' + agg_params['nested']
            agg_params.pop('nested')

        agg_params['field'] = field_name

        combined_agg = {}
        combined_agg[agg_name] = {}
        combined_agg[agg_name][agg_clause] = agg_params

        if not is_nested:
            return combined_agg
        else:
            return self.__create_nested_aggregation(agg_name, field_name, combined_agg)


    def combine_aggregations(self, agg_list):
        """Given all the aggreagations in a list, combine them

        Args:
            agg_list (list): List of aggregations

        Returns:
            dict: Combined aggregations
        """

        if len(agg_list) == 0:
            return {}

        combined_aggs = {}
        
        for agg in agg_list:
            key = agg.keys()[0]
            value = agg.values()[0]

            combined_aggs[key] = value

        return combined_aggs


    def add_highlighter(self, parent_query):
        parent_query['highlight'] = {}
        parent_query['highlight']['fields'] = {"tweet_text":{}}

    
    def add_auto_complete_suggestor(self, parent_query, field, value):
        parent_query['_source'] = ''
        parent_query['suggest'] = {
            'completion_suggest' : {
                'prefix': value,
                'completion': {
                    'size': 100,
                    'field': field + '.suggest',
                    'skip_duplicates': True
                }
            }
        }
        parent_query.pop('query')

