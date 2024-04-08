import os
import pandas as pd
import json

CURRENT_FOLDER = os.path.realpath(os.path.join(__file__, '..'))

files_directory = os.path.join(CURRENT_FOLDER, 'scheme-wise')

merged_metadata_file_directory = os.path.join(CURRENT_FOLDER, 'meta-data')

os.makedirs(merged_metadata_file_directory, exist_ok=True)

common_indicators = {}

unique_inducators = {}

def main():
    for file in os.listdir(files_directory):
        filename = os.path.join(files_directory, file)

        if os.path.isfile(filename):
            df = pd.read_excel(filename, 'Metadata')
            df.columns = ['metadata_name', 'metadata_value']

            df_metadata_dict = df.to_dict()

            metadata_kv_pair = {}
            for i in range(len(df_metadata_dict['metadata_name'])):
                metadata_kv_pair[df_metadata_dict['metadata_name'][i]] = df_metadata_dict['metadata_value'][i]
            
            merge_dict = {}
            
            for key_value_dict in [{k: v} for k, v in metadata_kv_pair.items() if k.startswith('indicator')]:
                merge_dict.update(key_value_dict)
            
            for i in range(1, int(len(merge_dict) / 7) + 1):
                if merge_dict["indicator_{}_type".format(i)] == 'unique':
                    unique_inducators[merge_dict["indicator_{}_name".format(i)]] = {
                        "description": merge_dict["indicator_{}_description".format(i)],
                        "unit": merge_dict["indicator_{}_unit".format(i)],
                        "note": merge_dict["indicator_{}_note".format(i)],
                    }
                else:
                    if merge_dict["indicator_{}_common_name".format(i)] not in common_indicators:
                        common_indicators[merge_dict["indicator_{}_common_name".format(i)]] = {
                            "description": merge_dict["indicator_{}_common_description".format(i)],
                            "unit": merge_dict["indicator_{}_unit".format(i)],
                            "note": merge_dict["indicator_{}_note".format(i)],
                        }
                        
    final_dict = {
        'common': common_indicators,
        'unique': unique_inducators
    }

    with open(
        merged_metadata_file_directory + '/' + 'metadata.json', 'w+'
    ) as fp:
        json.dump(final_dict, fp)


if __name__ == "__main__":
    main()