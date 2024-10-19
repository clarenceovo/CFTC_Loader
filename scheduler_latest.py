import requests
import pandas as pd
import mysql.connector
COLUMN=['Market_and_Exchange_Names', 'As_of_Date_In_Form_YYMMDD',
       'Report_Date_as_YYYY-MM-DD', 'CFTC_Contract_Market_Code',
       'CFTC_Market_Code', 'CFTC_Region_Code', 'CFTC_Commodity_Code',
       'Open_Interest_All', 'Dealer_Positions_Long_All',
       'Dealer_Positions_Short_All', 'Dealer_Positions_Spread_All',
       'Asset_Mgr_Positions_Long_All', 'Asset_Mgr_Positions_Short_All',
       'Asset_Mgr_Positions_Spread_All', 'Lev_Money_Positions_Long_All',
       'Lev_Money_Positions_Short_All', 'Lev_Money_Positions_Spread_All',
       'Other_Rept_Positions_Long_All', 'Other_Rept_Positions_Short_All',
       'Other_Rept_Positions_Spread_All', 'Tot_Rept_Positions_Long_All',
       'Tot_Rept_Positions_Short_All', 'NonRept_Positions_Long_All',
       'NonRept_Positions_Short_All', 'Change_in_Open_Interest_All',
       'Change_in_Dealer_Long_All', 'Change_in_Dealer_Short_All',
       'Change_in_Dealer_Spread_All', 'Change_in_Asset_Mgr_Long_All',
       'Change_in_Asset_Mgr_Short_All', 'Change_in_Asset_Mgr_Spread_All',
       'Change_in_Lev_Money_Long_All', 'Change_in_Lev_Money_Short_All',
       'Change_in_Lev_Money_Spread_All', 'Change_in_Other_Rept_Long_All',
       'Change_in_Other_Rept_Short_All', 'Change_in_Other_Rept_Spread_All',
       'Change_in_Tot_Rept_Long_All', 'Change_in_Tot_Rept_Short_All',
       'Change_in_NonRept_Long_All', 'Change_in_NonRept_Short_All',
       'Pct_of_Open_Interest_All', 'Pct_of_OI_Dealer_Long_All',
       'Pct_of_OI_Dealer_Short_All', 'Pct_of_OI_Dealer_Spread_All',
       'Pct_of_OI_Asset_Mgr_Long_All', 'Pct_of_OI_Asset_Mgr_Short_All',
       'Pct_of_OI_Asset_Mgr_Spread_All', 'Pct_of_OI_Lev_Money_Long_All',
       'Pct_of_OI_Lev_Money_Short_All', 'Pct_of_OI_Lev_Money_Spread_All',
       'Pct_of_OI_Other_Rept_Long_All', 'Pct_of_OI_Other_Rept_Short_All',
       'Pct_of_OI_Other_Rept_Spread_All', 'Pct_of_OI_Tot_Rept_Long_All',
       'Pct_of_OI_Tot_Rept_Short_All', 'Pct_of_OI_NonRept_Long_All',
       'Pct_of_OI_NonRept_Short_All', 'Traders_Tot_All',
       'Traders_Dealer_Long_All', 'Traders_Dealer_Short_All',
       'Traders_Dealer_Spread_All', 'Traders_Asset_Mgr_Long_All',
       'Traders_Asset_Mgr_Short_All', 'Traders_Asset_Mgr_Spread_All',
       'Traders_Lev_Money_Long_All', 'Traders_Lev_Money_Short_All',
       'Traders_Lev_Money_Spread_All', 'Traders_Other_Rept_Long_All',
       'Traders_Other_Rept_Short_All', 'Traders_Other_Rept_Spread_All',
       'Traders_Tot_Rept_Long_All', 'Traders_Tot_Rept_Short_All',
       'Conc_Gross_LE_4_TDR_Long_All', 'Conc_Gross_LE_4_TDR_Short_All',
       'Conc_Gross_LE_8_TDR_Long_All', 'Conc_Gross_LE_8_TDR_Short_All',
       'Conc_Net_LE_4_TDR_Long_All', 'Conc_Net_LE_4_TDR_Short_All',
       'Conc_Net_LE_8_TDR_Long_All', 'Conc_Net_LE_8_TDR_Short_All',
       'Contract_Units', 'CFTC_Contract_Market_Code_Quotes',
       'CFTC_Market_Code_Quotes', 'CFTC_Commodity_Code_Quotes',
       'CFTC_SubGroup_Code', 'FutOnly_or_Combined','','']

url = 'https://www.cftc.gov/dea/newcot/FinFutWk.txt'
response = requests.get(url)
text_data = response.text
lines = text_data.splitlines()
rows = []
import io
for line in lines:
    columns = line.split(',')
    rows.append(columns)

df = pd.DataFrame(rows)
df.columns = COLUMN
df.columns = df.columns.str.lower()
df.rename(columns={'report_date_as_yyyy-mm-dd': 'report_date'}, inplace=True)

db_config = {
    "host":"histdata.c3o36qtx0jbm.ap-northeast-1.rds.amazonaws.com",
    "user":"admin",
    "password":"96854233",
    "database":"fundamental_data"

}

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
print("hi")
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
