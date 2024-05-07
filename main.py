import pandas as pd

def read_files_to_dataframe(location):
    # Get a list of file paths in the specified location
    file_paths = glob.glob(location)

    # Initialize an empty list to store dataframes
    dfs = []

    # Iterate over each file path
    for file_path in file_paths:
        # Read the file into a dataframe
        df = pd.read_csv(file_path)  # Modify this line based on your file format

        # Append the dataframe to the list
        dfs.append(df)

    # Concatenate all dataframes into a single dataframe
    merged_df = pd.concat(dfs)

    # Reset the index of the merged dataframe
    merged_df.reset_index(drop=True, inplace=True)

    # Return the merged dataframe
    return merged_df