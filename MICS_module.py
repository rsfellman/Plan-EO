#Import packages
import pandas as pd
import numpy as np
import pycountry
import os
import zipfile
import re
import unidecode
import shutil
import logging
import rapidfuzz

# country names vary between datasets so we need to standardize them
def standardize_country(name):
    try:
        return pycountry.countries.lookup(name).name
    except LookupError:
        return None


# process metadata and iso3 codes
def process_mics_metadata(mics_metadata_path, country_codes_path='ISO3_country_codes.csv'):
    """
    Processes the MICS surveys catalogue file and outputs a cleaned DataFrame.

    Parameters:
        mics_file (str): Path to the MICS surveys catalogue CSV file.
        country_codes_file (str): Path to the country codes CSV file.
        meta_dict (dict): Dictionary to map country names to standardized names.
        country_codes_dict (dict): Dictionary to map country codes to standardized names.

    Returns:
        pd.DataFrame: Processed DataFrame with standardized country names that can be used for sorting data.
    """

    meta_dict = {
        'State of Palestine' : 'Palestine',
        'Kosovo (UNSC 1244)' : 'Kosovo',
        'Trinidad & Tobago' : 'Trinidad and Tobago',
        'Sao Tome & Principe' : 'Sao Tome and Principe',
        'Congo DR' : 'Democratic Republic of Congo',
        'Korea DPR' : "Korea, Democratic People's Republic of",
        "Cote d'Ivoire" : "Côte d'Ivoire",
        'Bosnia & Herzegovina' : 'Bosnia and Herzegovina',
        'South Sudan, Sudan' : 'Sudan',
        'Montenegro, Serbia' : 'Montenegro',
        'Kosovo (UNSC 1244), Montenegro, Serbia' : 'Kosovo',
        'Turkiye' : 'Turkey'
    }

    country_codes_dict = {
        'Democratic Republic of Congo' : 'Democratic Republic of Congo',
        'Cayman Islands' : 'Cayman Islands',
        'Falkland Islands' : 'Falkland Islands',
        'Micronesia': 'Micronesia, Federated States of',
        'Saint Martin' : 'Saint Martin',
        'Palestine' : 'Palestine',
        'Sint Maarten' : 'Sint Maarten',
        'Turkey' : 'Turkey',
        'Holy See' : 'Holy See',
        'Virgin Islands (British)' : 'British Virgin Islands',
        'Virgin Islands (U.S.)' : 'US Virgin Islands',
        'Kosovo' : 'Kosovo',
        "Cote d'Ivoire":"Côte d'Ivoire"
    }



    # Load the MICS surveys catalogue file
    meta = pd.read_csv(mics_metadata_path)
    
    # Load the country codes file
    country_codes = pd.read_csv(country_codes_path, names=['country', 'country_code', 'save_name'])
    
   # Apply standardize country to both datasets
    meta.loc[:,'standardized'] = meta['country'].apply(standardize_country)
    country_codes.loc[:,'standardized'] = country_codes['save_name'].apply(standardize_country)
    
    # Fill missing values in 'standardized' column of meta using meta_dict
    meta['standardized'] = meta['standardized'].fillna(meta['country'].map(meta_dict))
    
    # Fill missing values in 'standardized' column of country_codes using country_codes_dict
    country_codes['standardized'] = country_codes['standardized'].fillna(
        country_codes['save_name'].map(country_codes_dict)
    )
    
    # Merge meta with country_codes on the 'standardized' column
    meta_merge = meta.merge(country_codes, how='left', on='standardized')

    #add a new column for the MICS round
    # Extract the digit after 'MICS'    
    meta_merge['round_num'] = meta_merge['round'].str.extract(r'MICS(\d+)').astype(int)

    meta_final = meta_merge[['round', 'round_num', 'country_x', 'country_code', 'year', 'save_name', 'standardized']]

    return meta_final


#helper functions for extracting and sorting files


# Helper function to recursively extract files from nested zip files
def extract_nested_zip(zip_path, extract_to):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
        for root, _, files in os.walk(extract_to):
            for file in files:
                file_path = os.path.join(root, file)
                # Check if the file is a zip file
                if zipfile.is_zipfile(file_path):
                    # Create a temporary directory for nested zip extraction
                    nested_temp_dir = os.path.join(extract_to, "nested_temp")
                    os.makedirs(nested_temp_dir, exist_ok=True)
                    extract_nested_zip(file_path, nested_temp_dir)
                    # Move extracted files to the main folder and delete the nested temp directory
                    for nested_file in os.listdir(nested_temp_dir):
                        shutil.move(os.path.join(nested_temp_dir, nested_file), extract_to)
                    shutil.rmtree(nested_temp_dir)
                    # Remove the nested zip file after extraction
                    os.remove(file_path)
        # Flatten the directory structure by moving all files to the root of extract_to
        for root, _, files in os.walk(extract_to, topdown=False):  # Process subdirectories last
            for file in files:
                file_path = os.path.join(root, file)
                if root != extract_to:  # Only move files from subdirectories
                    shutil.move(file_path, extract_to)
            # Remove the now-empty subdirectory
            if root != extract_to:
                try:
                    os.rmdir(root)
                except OSError as e:
                    print(f"Warning: Could not remove directory {root}. Details: {e}")


#function to normalize text
def normalize(text):
    return unidecode.unidecode(re.sub(r'\W+', ' ', text)).strip().lower()
    

# function to standardize country names and give error if not standardizable
def standardize_country_name(name):
    try:
        return pycountry.countries.lookup(name).name
    except LookupError:
        return f"error"


def parse_file_name(zip_file):
    #extract year from zip file name
    name = zip_file.replace(".zip", '')
    name = (name.replace("_", " "))
    year_match = re.search(r"\b(19|20)\d{2}(?:-(?:\d{2}|\d{4}))?\b", name)
    year = year_match.group() if year_match else "XXXX"
    year_from_filename = year_match is not None


    #extract country  name form zip file name
    country_part = name.split("MICS")[0] if 'MICS' in name else name
    if year != "XXXX":
        country_part = re.sub(rf"\b{re.escape(year)}\b", "", country_part)
                
    if country_part == '':
        country_part = name

     # Additional logic: Check if the country name is after "MICS"
    if 'MICS' in country_part:
        #Extract the part after "MICS"
        after_mics = name.split("MICS", 1)[1].strip()
        # Use regex to separate the number and the country name
        match = re.match(r"(\d+)?\s*(.*)", after_mics)
        if match:
            country_part = match.group(2).strip()  # Extract the country name
    
    # Remove the word "Datasets" (case-insensitive)
    country_part = re.sub(r"\bdatasets\b", "", country_part, flags=re.IGNORECASE).strip()

    return name, year, country_part, year_from_filename


def parse_edge_cases(country_part, name):
    #deal with countries with territory not in parentheses
    if "thailand" in country_part.lower():
        country_part = "thailand"
        if "bangkok" in name.lower():
            extra_info = 'Bangkok'
        elif "provinces" in name.lower():
            extra_info = None
        else:
            extra_info = None

                
    elif "pakistan" in country_part.lower():
        #set country_part to pakistan
        country_part = "pakistan"
        #get state information
        state_match = re.search(r"\((.*?)\)", country_part)  # Check for parentheses
        if state_match:
            extra_info = state_match.group(1).strip()
        else:
            # Look for state names outside parentheses
            state_keywords = ["punjab", "sindh", "balochistan", "khyber pakhtunkhwa"]
            for state in state_keywords:
                if state.lower() in country_part.lower():
                    extra_info = state
                    break
            else:
                extra_info = None
        
   

    return country_part, extra_info


# function to extract and sort files


def extract_and_save_zipped_files(file_extraction_log, zip_file_path, mics_metadata, survey_round):
    """
    Extracts nested zip files and saves them in a folder with a specific naming convention.

    Parameters:
        zip_dir (str): Path to the directory or zip file containing the zip files.
        mics_metadata (pd.DataFrame): DataFrame containing metadata with ISO3 codes and years.
        survey_round (int): The survey round number to filter metadata.

    Returns:
        None
    """

    # Clear existing handlers
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    #set up logging
    logging.basicConfig(
        filename = file_extraction_log,
        level= logging.INFO,
        format = "%(message)s"
    )
    logger =logging.getLogger()

    #define output directory
    output_dir = '../individual_country_data'

    #ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    #step 1: unzip outer zip file into temporary directory

    #check if zip_file_path is a zip file
    if zipfile.is_zipfile(zip_file_path):
        #unzip the zip file into a temporary directory
        logger.info(f"Unzipping outer container: {zip_file_path}")
        #make temporary directory
        temp_dir = os.path.join(os.getcwd(), 'temp_extracted_zip_dir')
        os.makedirs(temp_dir, exist_ok = True)
        #unzip outer zip file
        with zipfile.ZipFile(zip_file_path, 'r') as outer_zip:
            outer_zip.extractall(temp_dir)
        
    # step 2: get metadata for specified survey round
    metadata = mics_metadata[mics_metadata['round_num']== survey_round]

    #hard code in a few recurrent errors with country matching
    country_lookup = {
        "cote d ivoire": "Côte d'Ivoire",
        "lao pdr": "Laos",
        "guinea bissau": "Guinea-Bissau",
        "lebanon palestinians": "Lebanon",
        "swaziland": "Eswatini",
        "congo democratic republic of" : "Democratic Republic of Congo",
        'congo dr' : "Democratic Republic of Congo",
        "drcongo" : "Democratic Republic of Congo",
        "state of palestine" : "Palestine",
        "the gambia" : "Gambia",
        "kosovo under unsc res 1244" :"Kosovo",
        "thailand bangkok" : "Thailand"
        }

    # step 3: iterate over all the zip files in the temporary directory
    for zip_file in os.listdir(temp_dir):
        if zip_file.endswith('.zip'):

            logger.info(f"Processing file: {zip_file}")
            
            try:

                parse = parse_file_name(zip_file)
                name, year, country_part, year_from_filename = parse

                #deal with countries with territories not in parentheses
                if "thailand" in country_part.lower() or "pakistan" in country_part.lower():
                    edge_case = parse_edge_cases(country_part, name)
                    country_part, extra_info = edge_case
                else:
                    # Extract extra information in parentheses (if present)
                    extra_info_match = re.search(r"\((.*?)\)", country_part)
                    extra_info = extra_info_match.group(1).strip() if extra_info_match else None

                #skip territory level data
                if extra_info:
                    logger.info("territory")
                    continue

                #remove any extra information in parentheses
                country_part = re.sub(r"\s*\(.*?\)", "", country_part).strip()

                #normalize country name
                country_norm = normalize(country_part)
                country_norm = re.sub(r"\s*datasets\s*", "", country_norm, flags=re.IGNORECASE).strip()  # Remove "datasets" from normalized name
                logger.info(f"Normalized country name: {country_norm}")
                
                #deal with edge cases
                if country_norm in country_lookup:
                    country_norm = country_lookup[country_norm]

                #standadize country name
                possible_country = mics_metadata[mics_metadata['round_num'] == survey_round]['save_name']
                country = standardize_country_name(country_norm)
                logger.info(f"Standardized country name: {country}")


                #check for standardization errors
                if country == "error":
                    # Step 1: Try exact match on save_name column
                    metadata_row = metadata[metadata['save_name'].str.contains(
                        rf'\b{re.escape(country_norm)}\b', case=False, na=False)]

                    if not metadata_row.empty:
                        logger.info(f"Matched {country_norm} directly in save_name: {metadata_row['save_name'].values[0]}")
                    else:
                        # Step 2: Try fuzzy matching if exact match failed
                        result = rapidfuzz.process.extractOne(
                            country_norm, possible_country, scorer=rapidfuzz.fuzz.partial_token_ratio)

                        if result:
                            match, score, *_ = result
                            if score >= 95:
                                logger.info(f"Fuzzy matched {country_norm} to {match} (score: {score})")
                                metadata_row = metadata[metadata['save_name'].str.contains(
                                    rf'\b{re.escape(match)}\b', case=False, na=False)]
                            else:
                                logger.warning(f"Fuzzy match score too low for {country_norm}. Skipping.")
                                continue
                        else:
                            logger.warning(f"No fuzzy match found for {country_norm}. Skipping.")
                            continue
                else:
                    # Normal metadata match when country standardization succeeded
                    metadata_row = metadata[metadata['standardized'] == country]
                    if metadata_row.empty:
                        logger.warning(f"Standardized name '{country}' not found in metadata. Skipping.")
                        continue
                    else:
                        logger.info(f"Metadata row for {country}: {metadata_row['save_name'].values[0]}")
                

                # year fallback if not in filename
                if year == "XXXX":
                    unique_years = metadata_row['year'].unique()
                    if len(unique_years) == 1:
                        year = str(unique_years[0])
                    elif len(unique_years) > 1:
                        try:
                            # Extract both start and end years from ranges and find the most recent year
                            recent_year = max(
                            int(part)
                            for y in unique_years
                            for part in str(y).split('-')  # Split year ranges like "2019-2020"
                            if part.isdigit()  # Ensure the part is a valid number
                        )
                            year = str(recent_year)
                        except ValueError:
                            logger.warning(f"Warning: Could not determine the most recent year for {country_part}. Skipping.")
                            continue
                    else:
                        logger.warning(f"Warning: no year found for {country_part}. Skipping.")
                        continue
                    
                # deal with years like 2021-2022
                if '-' in year:
                    year = year.split('-')[0]

                
                logger.info(f"Extracted country: {country}")
                logger.info(f"Found metadata rows: {len(metadata_row)}")
                logger.info(f"Available years in metadata: {metadata_row['year'].unique()}")
                
                #get iso3 code
                iso3_code = metadata_row['country_code'].values[0]

                #get full country name with
                full_country_name = metadata_row['save_name'].values[0].replace(' ', '_')

                #create country folder name (eg. AFG2004MI5)
                country_folder_name = f"{iso3_code}_{full_country_name}"
                country_folder_path = os.path.join(output_dir, country_folder_name)
                os.makedirs(country_folder_path, exist_ok=True)

                #create survey folder name (eg. AFG2004MI5)
                survey_folder_name = f"{iso3_code}{year}MC{survey_round}"
                survey_folder_path = os.path.join(country_folder_path, "03_Survey_data", survey_folder_name)
                

                #check for survey_folder path conflicts
                if os.path.exists(survey_folder_path):
                    if year_from_filename:
                        logger.warning(f"Survey folder {survey_folder_path} already exists but year came from filename. Attempting to rename existing folder and keep filename year.")

                        # Try to rename the existing folder to avoid overwriting
                        available_years = metadata_row['year'].unique()
                        if len(available_years) > 0:
                            try:
                                # Extract both start and end years from ranges and find the earliest year
                                other_year = min(
                                    int(part)
                                    for y in available_years
                                    for part in str(y).split('-')
                                    if part.isdigit()
                                )
                                alt_year = str(other_year)
                                if '-' in alt_year:
                                    alt_year = alt_year.split('-')[0]

                                # Build alternative folder name
                                alt_survey_folder_name = f"{iso3_code}{alt_year}MC{survey_round}"
                                alt_survey_folder_path = os.path.join(country_folder_path, "03_Survey_data", alt_survey_folder_name)

                # Make sure we're not overwriting that one either
                                if os.path.exists(alt_survey_folder_path):
                                    logger.warning(f"Alternative path {alt_survey_folder_path} already exists too. Skipping.")
                                    continue

                                # Rename the original existing folder
                                os.rename(survey_folder_path, alt_survey_folder_path)
                                logger.info(f"Renamed existing folder to {alt_survey_folder_path}")

                                # Now continue as planned using the original survey_folder_path
                                os.makedirs(survey_folder_path, exist_ok=True)

                            except Exception as e:
                                logger.error(f"Could not rename existing folder {survey_folder_path}. Reason: {e}")
                                continue
                        else:
                            logger.warning(f"No available years to rename existing folder for {country_part}. Skipping.")
                            continue
                    else:
                        # Only resolve conflict using metadata if year was not from filename
                        available_years = metadata_row['year'].unique()
                        if len(available_years) > 1:
                            try:
                                other_year = min(
                                    int(part)
                                    for y in available_years
                                    for part in str(y).split('-')
                                    if part.isdigit()
                                )
                                alt_year = str(other_year)
                                if '-' in alt_year:
                                    alt_year = alt_year.split('-')[0]

                                survey_folder_name = f"{iso3_code}{alt_year}MC{survey_round}"
                                survey_folder_path = os.path.join(country_folder_path, "03_Survey_data", survey_folder_name)

                            except ValueError:
                                logger.warning(f"Warning: Could not determine the most recent year for {country_part}. No unique year Skipping.")
                                continue
                        else:
                            logger.warning(f"Warning: no year found for {country_part}. Skipping.")
                            continue

                    
                
                os.makedirs(survey_folder_path, exist_ok=True)     

                #extract zip file contents directly into survey folder
                inner_zip_path = os.path.join(temp_dir, zip_file)
                logger.info(f"Unzipping {zip_file} to {survey_folder_name}...")
                try:
                    ### changed here
                    extract_nested_zip(inner_zip_path, survey_folder_path)
                    

                except zipfile.BadZipFile:
                    logger.error(f"Error: Failed to extract inner zip file {zip_file}. Skipping.")
                
                logger.info(f"Extracted and saved files for {zip_file} to {survey_folder_path}.")

            except zipfile.BadZipFile:
                logger.error(f"Error: {zip_file} is not a valid zip file. Skipping.")
            except KeyError as e:
                logger.error(f"Error: Missing key in metadata for {zip_file}. Details: {e}")
            except Exception as e:
                logger.error(f"Unexpected error while processing {zip_file}: {e}")

    #clean up temporary directory
    if 'temp_dir' in locals() and os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)


#function to parse error logs


def parse_log_to_df(log_file_path):
    with open(log_file_path, 'r') as file:
        lines = file.readlines()

    records = []
    record = {}

    for line in lines:
        if line.startswith("Unzipping outer container:"):
            continue

        if line.startswith("Processing file:"):
            if record:
                records.append(record)
                record = {}
            record['zip_file'] = line.split(":", 1)[1].strip()
            record['manual_check_advised'] = np.nan
            record['success'] = np.nan
            record['failure_reason'] = np.nan

        elif line.startswith("Normalized country name:"):
            record['normalized_country'] = line.split(":", 1)[1].strip()

        elif line.startswith("Standardized country name:"):
            record['standardized_country'] = line.split(":", 1)[1].strip()

        elif line.startswith("matched"):
            match = re.search(r"matched (.+?) with (.+?) in metadata", line)
            if match:
                record['manual_check_advised'] = f"Matched to {match.group(2).strip()}"
            else:
                record['manual_check_advised'] = "Manual check advised"

        elif line.startswith("Fuzzy matched"):
            match = re.search(r"Fuzzy matched (.+?) to (.+?) \(score: (\d+)\)", line)
            if match:
                record['manual_check_advised'] = f"Fuzzy matched to {match.group(2).strip()} (score: {match.group(3)})"
            else:
                record['manual_check_advised'] = "Fuzzy match used"

        elif "No fuzzy match found" in line or "Fuzzy match score too low" in line:
            record['manual_check_advised'] = "Fuzzy match failed"
            record['success'] = False
            record['failure_reason'] = "Fuzzy match failure"

        elif "Standardized name" in line and "not found in metadata" in line:
            match = re.search(r"Standardized name '(.+?)' not found", line)
            if match:
                record['failure_reason'] = f"Standardized name '{match.group(1)}' not found"
            else:
                record['failure_reason'] = "Standardized name not found"
            record['success'] = False

        elif line.startswith("Metadata row for"):
            record['metadata_row'] = line.split(":", 1)[1].strip()

        elif line.startswith("Extracted country:"):
            record['extracted_country'] = line.split(":", 1)[1].strip()

        elif line.startswith("Found metadata rows:"):
            record['metadata_rows_found'] = line.split(":", 1)[1].strip()

        elif line.startswith("Available years in metadata:"):
            record['available_years'] = line.split(":", 1)[1].strip()

        elif line.startswith("years available:"):
            years_match = re.search(r"years available: \[(.*?)\]", line)
            if years_match:
                record['available_years'] = years_match.group(1).strip()

        elif line.startswith("Unzipping") and "to" in line:
            record['unzipping_to'] = line.split("to", 1)[1].strip("... \n")

        elif line.startswith("Extracted and saved files"):
            record['saved_to'] = line.split("to", 1)[1].strip(". \n")
            record['success'] = True
            record['failure_reason'] = np.nan

        elif line.startswith("Unexpected error while processing"):
            match = re.search(r"processing (.+?): (.+)", line)
            if match:
                record['zip_file'] = match.group(1).strip()
                record['failure_reason'] = match.group(2).strip()
                record['success'] = False
                record['manual_check_advised'] = "Manual check advised"

        elif line.startswith("Warning: Could not determine the most recent year"):
            match = re.search(r"Warning: Could not determine the most recent year (.+?)\.", line)
            if match:
                record['failure_reason'] = f"Year fallback failed for {match.group(1).strip()}"
            else:
                record['failure_reason'] = "Year fallback failed"
            record['success'] = False

        elif "could not find correct year" in line.lower():
            record['failure_reason'] = "Year fallback failed"
            record['success'] = False

        elif 'territory' in line.lower():
            record['success'] = 'territory'

    if record:
        records.append(record)

    df = pd.DataFrame(records)

    expected_cols = [
        'zip_file', 'normalized_country', 'standardized_country', 'metadata_row',
        'extracted_country', 'metadata_rows_found', 'available_years',
        'unzipping_to', 'saved_to', 'success', 'failure_reason', 'manual_check_advised'
    ]
    for col in expected_cols:
        if col not in df.columns:
            df[col] = np.nan

    return df[expected_cols]



