from prettytable import PrettyTable
import heapq
from operator import itemgetter
import csv

class UniversalTableMethods:
    #print list of dictionaries as table
    @staticmethod
    def print_lod(tablename, lod):
        print(tablename)
        x = PrettyTable()
        field_names = lod[0].keys()
        x._set_field_names(field_names)
        for dict in lod:
            current_dict = [dict[field] for field in field_names]
            x.add_row(current_dict)
        print(x)

    #convert dict of dicts to list of dicts (similar to pandas index to records)
    # {'john' : {'age':'20', 'dob':''}, 'ana' : {'age':'25', 'dob':''}}
    # TO =>
    # [{'name':'john', 'age':'20', dob:''}, {'name':'john', 'age':'25', 'dob':''}]
    @staticmethod
    def dod_to_lod(index_name, dict_of_dicts):
        new_lod = []
        for key, dict in dict_of_dicts.items():
            new_dict = {index_name:key}
            new_dict.update(dict)
            new_lod.append(new_dict)
        return new_lod

    #label all rows of data by adding a column with the same value in all rows
    @staticmethod
    def add_values_to_all_lod(dict_of_values_to_add, lod):
        result = [dict(item, **dict_of_values_to_add) for item in lod]
        return result

    #remove specific columns / keys from a list of dictionaries
    @staticmethod
    def remove_list_of_keys_from_lod(list_of_keys_to_remove, lod):
        def remove_list_of_keys_from_dict(dict):
            return {k:v for k, v in dict.items() if k not in list_of_keys_to_remove}
        return [remove_list_of_keys_from_dict(dict) for dict in lod]

    #filter a list of dicts using one column matching specific values in a list
    #Like filtering a table based on the values in one column
    @staticmethod
    def filter_lod_by_col_val(col_name, value_list, lod):
        result = [d for d in lod if d['col_name'] in value_list]
        return result

    # filter list of dicts to only include certain keys
    # Like filtering a table to include only certain columns
    @staticmethod
    def filter_lod_keys(keep_cols, lod):
        def filter_dict_keys(keys, dict):
            filtered_dict = {key: dict[key] for key in keys if key in dict.keys()}
            return filtered_dict
        new_lod = [filter_dict_keys(keep_cols, dict) for dict in lod]
        return new_lod

    #rename keys in list of dicts using a dictionary with Old:new values
    #Like renaming columns using a dictionary to specify the old and new column names
    @staticmethod
    def rename_lod_keys(keys_dict, lod):
        new_lod = [UniversalTableMethods.rename_dict_keys(keys_dict, dict) for dict in lod]
        return new_lod

    @staticmethod
    def rename_dict_keys(change_keys, old_dict):
        new_dict = {change_keys.get(k, k): v for k, v in old_dict.items()}
        return new_dict

    @staticmethod
    def lod_to_csv(filename, lod):
        lod = UniversalTableMethods.fill_in_missing_keys_in_lod(lod)
        lod_keys = lod[0].keys()
        if '.csv' not in filename:
            filename = filename + '.csv'
        with open(filename, 'wb') as output_file:
            dict_writer = csv.DictWriter(output_file, lod_keys)
            dict_writer.writeheader()
            dict_writer.writerows(lod)

    #update existing dict values with another dict.
    #update_keys_dict is the key0:key1 match in case updating from differently named columns
    #assumes second list has unique key, while first list need not be unique
    # or
    #Like updating one or more columns in a table using a second table and joining on one column.
    @staticmethod
    def update_lod_with_lod(lod_0, lod_1, join_key, update_keys_dict={}, default_if_na=None):
        # if the keys to be matched are not specified, assume they are called the same.
        # get the keys for the first row of data as the list of keys to update (minus the join key).
        if not update_keys_dict:
            update_keys = list(lod_1[0].keys())
            update_keys.remove(join_key)
            for key in update_keys:
                update_keys_dict.update({key:key})

        #cycle through rows of lod to be updated (lod_0) and find matching lod_1 row on join_key
        new_lod = lod_0
        for d_0 in new_lod:
            d_1 = next((dict for dict in lod_1 if dict[join_key] == d_0[join_key]), default_if_na)
            if d_1:
            #d_1 = [dict for dict in lod_1 if dict[join_key] == d_0[join_key]][0]
            #cycle through keys to be updated
                for k, v in update_keys_dict.items():
    #                if k in d_0:
    #                    d_0[k] = d_1[v]
    #               else:
                    d_0.update({k : d_1[v]})
        return new_lod

    # if a dictionary in a list of dicts is missing key values that are in other dicts, add them to the dict.
    # Like adding columns to a table row for any row that is missing the column, so that all rows have the same columns
    @staticmethod
    def fill_in_missing_keys_in_lod(lod):
        for index, d in enumerate(lod):
            #if first loop assign headers
            missing_headers = False
            if index == 0:
                headers = list(d.keys())
            #if not first loop, extend list of headers if any are missing
            else:
                missing_headers = [header for header in list(d.keys()) if header not in headers]
                if missing_headers:
                    headers.extend(missing_headers)
        #print(lod)
        #update all rows to ensure headers exist in all rows, fill in blanks
        lod = [UniversalTableMethods.fill_in_missing_keys_in_dict(dict, headers) for dict in lod]
        return lod

    @staticmethod
    def fill_in_missing_keys_in_dict(dict, keys):
        blank_data = {key: '' for key in keys if key not in list(dict.keys())}
        dict.update(blank_data)
        return dict

    #flatten nested dicts and lists
    #If using for a list of dicts, must cycle through them flattening one dict at a time with a list comprehension
    # (used one 'row' at a time)
    @staticmethod
    def flatten_json(y):
        out = {}

        def flatten(x, name=''):
            if type(x) is dict:
                for a in x:
                    flatten(x[a], name + a + '.')
            elif type(x) is list:
                i = 0
                for a in x:
                    flatten(a, name + str(i) + '.')
                    i += 1
            else:
                out[name[:-1]] = x

        flatten(y)
        return out
    
    #sorts the table and sets the rank based on one field
    @staticmethod
    def set_rank_float(lod, field, descending=False):
        newlist = UniversalTableMethods.change_field_type_lod(lod, field)
        #newlist = [(dict_[field], dict_) for dict_ in lod]
        newlist = sorted(lod, key=itemgetter(field), reverse=descending)
        for index, dict in enumerate(newlist):
            dict[field + '_rank'] = index + 1
        return newlist

    #change the data type for a key (field) in a list of dictionaries
    @staticmethod
    def change_field_type_lod(lod, field, type='float'):
        for d in lod:
            d[field] = float(d[field])
        return lod
        #new_lod = [dict([a, eval('%s(x)' % type)] if a in fields else a for a, x in b.items()) for b in lod]
        #new_lod = [{a: float(x)} if a in fields else {a: x} for b in lod for a, x in b.items()]
        #print(lod)
        #new_lod = [{a: float(x)} if a in fields else {a: x} for a, x in lod[0].items()]
        #print(new_lod)

        return new_lod

    #calculate the cumulative sum for a field and append as new column
    @staticmethod
    def set_cumsum(lod, field):
        cum = 0
        for dict in lod:
            cum += dict[field]
            dict['cumsum_' + field] = cum
        return lod

    # retrieve top N number of rows based on field value. ex: top 10 largest walls based on zscore
    @staticmethod
    def get_top_n(lod, field, top_n):
        if len(lod) > 0:
            top_dicts = heapq.nlargest(top_n, lod, key=lambda dict: dict[field])
        return top_dicts
