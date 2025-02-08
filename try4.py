import sqlite3
import pandas as pd 
from datetime import datetime, time, timedelta


#SPOT
db_path ='SPOT.db'  
conn = sqlite3.connect(db_path)

entry_date='05102023'

query = f'SELECT * FROM "{entry_date}" ' 
df = pd.read_sql_query(query, conn)


filtered_df = df[(df['time'] >= '09:15:00') & (df['time'] <= '15:25:00')]


start_price = filtered_df.iloc[0]['open']  
end_price = filtered_df.iloc[-1]['close']
market_movement = end_price - start_price
# option type

if market_movement > 0:

    option_type = 'PE'  
 
else:

    option_type = 'CE'


atm= round(end_price / 100) * 100

if option_type == 'PE':
        hedge_strike = atm * (1 + 0.02)  #strikes
else:
        hedge_strike = atm * (1 - 0.02)  
        
hedge_strike = round(hedge_strike / 100) * 100


#OPTIONS
options_db_path = 'OPT.db'  
conn_options = sqlite3.connect(options_db_path)


query_options = f'SELECT * FROM "{entry_date}"'  
df_next_day_options = pd.read_sql_query(query_options, conn_options)

#expiry
entry_date = datetime.strptime(entry_date, '%d%m%Y')


df_next_day_options['expiry'] = pd.to_datetime(df_next_day_options['expiry'], format='%d-%m-%Y')
nearest_expiry = min(df_next_day_options['expiry'], key=lambda x: abs(x - entry_date))

filtered_options = df_next_day_options[(df_next_day_options['expiry'] == nearest_expiry) & (df_next_day_options['time'] == '15:25:00')]

def get_option_price(options_data, strike, option_type):
    option = options_data[(options_data['strike'] == strike) & (options_data['instrument_type'] == option_type)]
    if not option.empty:
        return (option.iloc[0]['symbol'],option.iloc[0]['close'])  
    else:
        return None
    
    

atm_symbol , atm_price = get_option_price(filtered_options, atm, option_type)
print(f"ATM Strike Price at 3:25 PM: {atm_price}")
print(atm_symbol)

hedge_symbol, hedge_price = get_option_price(filtered_options, hedge_strike, option_type)
print(f"Hedge Strike Price at 3:25 PM: {hedge_price}")
print(hedge_symbol)



next_day_date = '06102023'
query_options = f'SELECT * FROM "{next_day_date}"'
df_next_day_options = pd.read_sql_query(query_options, conn_options)

filtered_df = df_next_day_options[
    (df_next_day_options['time'] >= '09:15:00') & 
    (df_next_day_options['time'] <= '09:45:00') & 
    (df_next_day_options['symbol'] == atm_symbol)
]



hedge_filtered_df = df_next_day_options[
    (df_next_day_options['time'] >= '09:15:00') &
    (df_next_day_options['time'] <= '09:45:00') &
    (df_next_day_options['symbol'] == hedge_symbol)
]


import pandas as pd

def atm_trail_sl_and_exit(df, atm_price):
    initial_sl = atm_price * 1.05 # 5% above cuz sold
    exit_time = None
    exit_price = None
    
    # next day 9:15 price
    next_day_df = df[df['time'] == '09:15:00']
    if not next_day_df.empty:  #in case of expiry
        next_day_price = next_day_df.iloc[0]['open']
        if next_day_price > initial_sl:
            return next_day_df.iloc[0]['time'], next_day_price  # Exit at 9:15 next day immediately
    
    stored_high = atm_price  #ATM price as stored high
    
    for i in range(0, len(df), 3): # change to 3 min candle
        three_min_highs = df.iloc[i:i+3]['high'].tolist()
        three_min_prices = df.iloc[i:i+3]['close'].tolist()
        max_three_min = max(three_min_highs)
        
        if max_three_min < stored_high:
            stored_high = max_three_min  # Update stored high only if lower
        
        # Check if price goes above (SL hit)
        for j in range(i, min(i+3, len(df))):
            if df.iloc[j]['high'] > stored_high:
                exit_time = df.iloc[j]['time']
                exit_price = df.iloc[j]['high']
                return exit_time, exit_price  # Exit trade immediately
    
    # If trade is still open, exit at 09:45
    if '09:45:00' in df['time'].values:
        exit_row = df[df['time'] == '09:45:00'].iloc[0]
        exit_time = exit_row['time']
        exit_price = exit_row['close']
        return exit_time, exit_price
    
    return exit_time, exit_price  # Final trade result

df = filtered_df 
exit_time, exit_price = atm_trail_sl_and_exit(df, atm_price)
print(f"Exit Time: {exit_time}, Exit Price: {exit_price}")


def hedge_trail_sl_and_exit(df, hedge_price):
    initial_sl = hedge_price  * 0.95  # 5% below cuz bought
    stored_low = hedge_price 
    sl=initial_sl
    
    #  next day 9:15 price
    next_day_df = df[df['time'] == '09:15:00']
    if not next_day_df.empty:  
        next_day_price = next_day_df.iloc[0]['open']
        if next_day_price < initial_sl:
            return next_day_df.iloc[0]['time'], next_day_price  # Exit hedge trade at 9:15 
    
    for i in range(0, len(df), 3):  
        three_min_lows = df.iloc[i:i+3]['low'].tolist()
        min_three_min = min(three_min_lows)
        
        if min_three_min > sl:
            initial_sl = min_three_min  # Update SL only if new low is higher
        
        
        for j in range(i, min(i+3, len(df))):
            if df.iloc[j]['low'] < sl:
                exit_time = df.iloc[j]['time']
                exit_price = df.iloc[j]['low']
                return exit_time, exit_price  # Exit hedge 
    
    if '09:45:00' in df['time'].values:
        exit_row = df[df['time'] == '09:45:00'].iloc[0]
        exit_time = exit_row['time']
        exit_price = exit_row['close']
        return exit_time, exit_price
    
    return None, None  


hedge_exit_time, hedge_exit_price = hedge_trail_sl_and_exit(hedge_filtered_df, hedge_price)
print(f"Hedge Exit Time: {hedge_exit_time}, Hedge Exit Price: {hedge_exit_price}")


pnl= (atm_price - exit_price) + ( hedge_exit_price - hedge_price)
print(f"Total P&L: {pnl}")
