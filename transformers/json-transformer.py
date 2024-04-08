import json
import math
import os
import re
import pandas as pd

CURRENT_FOLDER = os.path.realpath(os.path.join(__file__, '..'))

files_directory = os.path.join(CURRENT_FOLDER, 'state-wise')

merged_json_files_directory = os.path.join(CURRENT_FOLDER, 'state-wise-json')

os.makedirs(merged_json_files_directory, exist_ok=True)


def slugify(s):
    return re.sub('\s+', '-', s.strip().lower())


def convert_nan_values_to_na_string_and_limit_decimal_precision(val):
    return 'NA' if math.isnan(val) else float(f"{val:.2f}")


def main():
    for file in os.listdir(files_directory):    
        filename = os.path.join(files_directory, file)

        if os.path.isfile(filename):
            df = pd.read_csv(filename)
            
            # Constituency Lookup Dictionary
            kf = df[['constituency', 'constituency_code']].drop_duplicates('constituency_code')
            constituency_lookup = dict(zip(kf.constituency_code, kf.constituency))
            
            state_average_dict = {}
            
            kkf = df.drop(['constituency', 'constituency_code'], axis=1).groupby(['fiscal_year','scheme_slug', 'indicator_name'])['indicator_value'].aggregate(['min', 'max', 'median']).reset_index()
            
            for i in list(kkf.values):            
                i[2] = slugify(i[2])

                state_average_values = {
                        'min': convert_nan_values_to_na_string_and_limit_decimal_precision(i[3]),
                        'max': convert_nan_values_to_na_string_and_limit_decimal_precision(i[4]),
                        'avg': convert_nan_values_to_na_string_and_limit_decimal_precision(i[5]),
                }
                
                if i[0] not in state_average_dict.keys():
                    state_average_dict[i[0]] = {i[1]: {i[2]: state_average_values}}
                    
                if i[1] not in state_average_dict[i[0]].keys():
                    state_average_dict[i[0]][i[1]] = {i[2]: state_average_values}
                    
                if i[2] not in state_average_dict[i[0]][i[1]].keys():
                    state_average_dict[i[0]][i[1]][i[2]] = state_average_values

            ddf = df.groupby(['constituency_code','fiscal_year','scheme_slug','indicator_name','indicator_value']).groups
            
            shared_dict = {}
            
            for i in ddf:
                i = list(i)
                i[0] = int(i[0])
                i[3] = slugify(i[3])
                i[4] = convert_nan_values_to_na_string_and_limit_decimal_precision(i[4])
                
                if i[0] not in shared_dict.keys():
                    shared_dict[i[0]]={"constituency_code":i[0], "constituency_name":constituency_lookup[i[0]], "fiscal_year":{i[1]:{i[2]:{i[3]:i[4]}}}}
                
                if i[1] not in shared_dict[i[0]]["fiscal_year"].keys():
                    shared_dict[i[0]]["fiscal_year"][i[1]] = {i[2]:{i[3]:i[4]}}
                if i[2] not in shared_dict[i[0]]["fiscal_year"][i[1]].keys():
                    shared_dict[i[0]]["fiscal_year"][i[1]][i[2]] = {i[3]:i[4]}
                else:
                    shared_dict[i[0]]["fiscal_year"][i[1]][i[2]][i[3]] = i[4]
            
            final_dict = {
                'state_avg': state_average_dict,
                'constituency_data': shared_dict
            }
            
            with open(
                merged_json_files_directory + r'/' + file[:-4].replace('-', '_') + '.json', 'w+'
            ) as fp:
                json.dump(final_dict, fp)

if __name__ == "__main__":
    main()