import json

import requests
import zipfile
import io
import pandas as pd
import mysql.connector

db_config = json.loads(open("config/db_config.json").read())

for year in range(2024,2025):
    print(f"Receiving Data for year {year}....")
    url = f"https://www.cftc.gov/files/dea/history/fut_fin_txt_{year}.zip"

    # Step 1: Download the ZIP file
    try:
        response = requests.get(url)
        response.raise_for_status()  # Check if the request was successful
    except:
        print(f"Failed to download the ZIP file for {year}.")
        continue

    # Step 2: Open the ZIP file in memory
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        # Step 3: List all contents of the ZIP file
        txt_files = [file for file in z.namelist() if file.endswith('.txt')]

        if txt_files:
            # Assuming there is at least one TXT file in the ZIP, extract and read it
            with z.open(txt_files[0]) as txt_file:
                file_content = txt_file.read().decode('utf-8')
        else:
            print("No TXT files found in the ZIP archive.")
    print("Finished Receiving Data....")
    df = pd.read_csv(io.StringIO(file_content))
    df.columns = df.columns.str.lower()
    df.rename(columns={'report_date_as_yyyy-mm-dd': 'report_date'}, inplace=True)

    db_conn = mysql.connector.connect(**db_config)
    db_cursor = db_conn.cursor()

    db_cursor.execute("SELECT instrument_id, instrument_name FROM cftc_instrument")
    instrument_mapping = dict(db_cursor.fetchall())
    value_to_key = {v: k for k, v in instrument_mapping.items()}
    df['instrument_id'] = df['market_and_exchange_names'].map(value_to_key)
    data_set = []
    df = df.dropna(subset=['instrument_id'])
    for index, row in df.iterrows():
        if row['instrument_id'] is None or row['instrument_id'] == "nan":
            continue
        data_set.append([row['instrument_id'],
                            row['report_date'],
                            row['cftc_contract_market_code'],
                            row['open_interest_all'],
                            row['dealer_positions_long_all'],
                            row['dealer_positions_short_all'],
                            row['asset_mgr_positions_long_all'],
                            row['asset_mgr_positions_short_all'],
                            row['lev_money_positions_long_all'],
                            row['lev_money_positions_short_all'],
                            row['other_rept_positions_long_all'],
                            row['other_rept_positions_short_all'],
                            row['tot_rept_positions_long_all'],
                            row['tot_rept_positions_short_all'],
                            row['nonrept_positions_long_all'],
                            row['nonrept_positions_short_all'],
                            row['traders_tot_all']])

    query = f"""
    INSERT IGNORE INTO `fundamental_data`.`cftc_instrument_record` (
        `instrument_id`,
        `report_date`,
        `cftc_contract_market_code`,
        `open_interest_all`,
        `dealer_positions_long_all`,
        `dealer_positions_short_all`,
        `asset_mgr_positions_long_all`,
        `asset_mgr_positions_short_all`,
        `lev_money_positions_long_all`,
        `lev_money_positions_short_all`,
        `other_rept_positions_long_all`,
        `other_rept_positions_short_all`,
        `tot_rept_positions_long_all`,
        `tot_rept_positions_short_all`,
        `nonrept_positions_long_all`,
        `nonrept_positions_short_all`,
        `traders_tot_all`
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    );
    """
    batch_size = 100
    try:
        for i in range(0, len(data_set), batch_size):
            print("Inserting batch:", i, "to", i + batch_size - 1)
            batch = data_set[i:i + batch_size]
            db_cursor.executemany(query, batch)
            db_conn.commit()
    except Exception as e:
        print(e)
        print(batch)
        db_conn.rollback()
    finally:
        db_conn.close()




"""
insert_queries = []
for index, row in df.iterrows():
    if row['instrument_id'] is None:
        continue
    query = f
    INSERT IGNORE INTO `fundamental_data`.`cftc_instrument_record` (
        `instrument_id`,
        `report_date`,
        `cftc_contract_market_code`,
        `open_interest_all`,
        `dealer_positions_long_all`,
        `dealer_positions_short_all`,
        `asset_mgr_positions_long_all`,
        `asset_mgr_positions_short_all`,
        `lev_money_positions_long_all`,
        `lev_money_positions_short_all`,
        `other_rept_positions_long_all`,
        `other_rept_positions_short_all`,
        `tot_rept_positions_long_all`,
        `tot_rept_positions_short_all`,
        `nonrept_positions_long_all`,
        `nonrept_positions_short_all`,
        `traders_tot_all`
    ) VALUES (
        {row['instrument_id']},
        '{row['report_date']}',
        '{row['cftc_contract_market_code']}',
        {row['open_interest_all']},
        {row['dealer_positions_long_all']},
        {row['dealer_positions_short_all']},
        {row['asset_mgr_positions_long_all']},
        {row['asset_mgr_positions_short_all']},
        {row['lev_money_positions_long_all']},
        {row['lev_money_positions_short_all']},
        {row['other_rept_positions_long_all']},
        {row['other_rept_positions_short_all']},
        {row['tot_rept_positions_long_all']},
        {row['tot_rept_positions_short_all']},
        {row['nonrept_positions_long_all']},
        {row['nonrept_positions_short_all']},
        {row['traders_tot_all']}

    );
    
    insert_queries.append(query.strip())

for query in insert_queries:
    print(f"Executing query:f{query}")
    db_cursor.execute(query)
    db_conn.commit()
db_cursor.close()
"""