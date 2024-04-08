import os
import pandas as pd

CURRENT_FOLDER = os.path.realpath(os.path.join(__file__, '..'))

files_directory = os.path.join(CURRENT_FOLDER, 'scheme-wise')

merged_files_directory = os.path.join(CURRENT_FOLDER, 'state-wise')

os.makedirs(merged_files_directory, exist_ok=True)

def main():
    for file in os.listdir(files_directory):
        filename = os.path.join(files_directory, file)

        if os.path.isfile(filename):
            df = pd.read_excel(filename)
            
            # Reading metadata sheet and converting all the content into dict
            df_metadata = pd.read_excel(filename, 'Metadata')
            df_metadata.columns = ['metadata_name', 'metadata_value']
            df_metadata_dict = df_metadata.to_dict()
            
            metadata_kv_pair = {}
            for i in range(len(df_metadata_dict['metadata_name'])):
                metadata_kv_pair[df_metadata_dict['metadata_name'][i]] =  df_metadata_dict['metadata_value'][i]

            df = df.melt(
                id_vars=[
                    'state_ut_name', 'state_ut_code', 'constituency', 'constituency_code', 'fiscal_year'
                ],
                value_vars=list(df.columns[5:]),
                var_name='indicator_id'
            )
            
            df['scheme_slug'] = file[4:-8]
            
            # Add column indicator_name, check if common_name is there else normal name
            for i, row in df.iterrows():
                common_name = metadata_kv_pair[row['indicator_id'] + '_common_name']
                
                if type(common_name) is not float:
                    df._set_value(i, 'indicator_name', metadata_kv_pair[row['indicator_id'] + '_common_name'])
                else:
                    df._set_value(i, 'indicator_name', metadata_kv_pair[row['indicator_id'] + '_name'])
            
            df = df.groupby(['state_ut_name'])

            for group in df.groups.keys():
                df.get_group(group).drop(['state_ut_name', 'state_ut_code'], axis=1).to_csv(
                    r'{}/{}_{}.csv'.format(merged_files_directory, group.lower(), file[-7:-5]),
                    mode='a',
                    index=False,
                )
                
    # In merged files sort based on constituency name
    for file in os.listdir(merged_files_directory):
        filename = os.path.join(merged_files_directory, file)
        
        if os.path.isfile(filename):
            df = pd.read_csv(filename)
            df_header = list(df.columns)
            
            # Remove all text rows added
            df.drop(df.index[df['constituency_code'] == 'constituency_code'], inplace=True)
            
            df['constituency_code'] = list(map(lambda x : float(x), df['constituency_code']))
            df = df.sort_values(['constituency_code'])
            
            ordered_columns = list(df.columns)
            del(ordered_columns[-2])
            ordered_columns.insert(3, 'scheme_slug')
            
            df = df.reindex(columns=ordered_columns)
            
            ordered_columns = [s.replace('value', 'indicator_value') for s in ordered_columns]
            
            os.remove(filename)
            
            df.to_csv(
                filename,
                mode='a',
                index=False,
                header=ordered_columns
            )

if __name__ == "__main__":
    main()