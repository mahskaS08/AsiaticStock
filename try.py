import sqlite3
import pandas as pd 
from datetime import datetime, time, timedelta
# daily_data = pd.read_csv("daily.csv")
# # print(daily_data.head())
# inputSheet = pd.read_excel("output_sample.xlsx", sheet_name="Input")
# statsSheet = pd.read_excel("output_sample.xlsx", sheet_name="Stats")
# txnSheet = pd.read_excel("output_sample.xlsx", sheet_name="Transaction")
# # print(inputSheet.head())
# # print(statsSheet.head())
# # print(txnSheet.head())
# # conOption = sqlite3.connect("OPT.db")
# # cursor_opt = conOption.cursor()
# # cursor_opt.execute("SELECT name FROM sqlite_master WHERE type='table';")
# # tables_opt = cursor_opt.fetchall()
# # # print("Tables in OPT.db:", tables_opt)

# # conSpot = sqlite3.connect("SPOT.db")
# # cursor_spot = conSpot.cursor()


# # cursor_spot.execute("SELECT name FROM sqlite_master WHERE type='table';")
# # tables_spot = cursor_spot.fetchall()
# # print("Tables in SPOT.db:", tables_spot)


# # daily_data['date'] = pd.to_datetime(daily_data['date'], format='%d%m%Y')
# daily_data['movement'] = (daily_data['close'] - daily_data['open']) #/ daily_data['open']
# # print(daily_data[['date', 'open', 'close', 'movement']].head())




#SPOT
db_path ='SPOT.db'  
conn = sqlite3.connect(db_path)


query = 'SELECT * FROM "04092023"' 
df = pd.read_sql_query(query, conn)


filtered_df = df[(df['time'] >= '09:15:00') & (df['time'] <= '15:25:00')]


start_price = filtered_df.iloc[0]['open']  
end_price = filtered_df.iloc[-1]['close']
market_movement = end_price - start_price


if market_movement > 0:

    option_type = 'PE'  
 
else:

    option_type = 'CE'


atm= round(end_price / 100) * 100

if option_type == 'PE':
        hedge_strike = atm * (1 + 0.02)  
else:
        hedge_strike = atm * (1 - 0.02)  
        
hedge_strike = round(hedge_strike / 100) * 100


#OPTIONS
options_db_path = 'OPT.db'  
conn_options = sqlite3.connect(options_db_path)


query_options = 'SELECT * FROM "04092023"'  
df_options = pd.read_sql_query(query_options, conn_options)





#expiry
entry_date = datetime.strptime('04092023', '%d%m%Y')


df_options['expiry'] = pd.to_datetime(df_options['expiry'], format='%d-%m-%Y')
nearest_expiry = min(df_options['expiry'], key=lambda x: abs(x - entry_date))

filtered_options = df_options[(df_options['expiry'] == nearest_expiry) & (df_options['time'] == '15:25:00')]

def get_option_price(options_data, strike, option_type):
    option = options_data[(options_data['strike'] == strike) & (options_data['instrument_type'] == option_type)]
    if not option.empty:
        return option.iloc[0]['close']  
    else:
        return None
    
atm_price = get_option_price(filtered_options, atm, option_type)
print(f"ATM Strike Price at 3:25 PM: {atm_price}")
hedge_price = get_option_price(filtered_options, hedge_strike, option_type)
print(f"Hedge Strike Price at 3:25 PM: {hedge_price}")

next_day_date = '05092023'
query_options = f'SELECT * FROM "{next_day_date}"'
df_next_day_options = pd.read_sql_query(query_options, conn_options)
conn_options.close()



next_day_datetime = datetime.strptime(next_day_date, '%d%m%Y')

filtered_next_day_options = df_next_day_options[
    (df_next_day_options['expiry'] == nearest_expiry) &  # Same option as the previous day
    (
        (df_next_day_options['strike'] == atm) |  
        (df_next_day_options['strike'] == hedge_strike)  
    )
]


def calculate_3_high(data, start_time, end_time):
    filtered_data = data[(data['time'] >= start_time) & (data['time'] <= end_time)]
    return filtered_data['high'].max()


atm_stop = None
hedge_stop = None
trade_open = True

current_time = datetime.strptime('09:15:00', '%H:%M:%S')
exit_time = datetime.strptime('09:45:00', '%H:%M:%S')

