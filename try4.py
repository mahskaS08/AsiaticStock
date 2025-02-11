import sqlite3
import pandas as pd 
from datetime import datetime, time, timedelta

def get_all_dates():
    db_path = 'SPOT.db'
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
   
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    dates = [date[0] for date in cursor.fetchall()]
    
    # for sorting dates in pairs
    date_pairs = []
    for date in dates:
        try:
            
            day = date[:2]
            month = date[2:4]
            year = date[4:]
           
            sortable_date = year + month + day
            date_pairs.append((date, sortable_date))
        except ValueError:
            continue
    
    
    date_pairs.sort(key=lambda x: x[1])
    
    # Get back original date format strings in correct order
    sorted_dates = [pair[0] for pair in date_pairs]
    
    # Create pairss
    trading_pairs = []
    for i in range(len(sorted_dates)-1):
        # Convert current and next date to datetime for comparison
        current = datetime.strptime(sorted_dates[i], '%d%m%Y')
        next_date = datetime.strptime(sorted_dates[i+1], '%d%m%Y')
        # Only create pair if next_date is after current date
        if next_date > current:
            trading_pairs.append((sorted_dates[i], sorted_dates[i+1]))
    
    conn.close()
    return trading_pairs

def execute_strategy(entry_date, next_day_date):
    #SPOT
    db_path = 'SPOT.db'  
    conn = sqlite3.connect(db_path)

    query = f'SELECT * FROM "{entry_date}"'
    df = pd.read_sql_query(query, conn)

    filtered_df = df[(df['time'] >= '09:15:00') & (df['time'] <= '15:25:00')]

    start_price = filtered_df.iloc[0]['open']  
    end_price = filtered_df.iloc[-1]['close']
    market_movement = end_price - start_price

    if market_movement > 0:
        option_type = 'PE'  
    else:
        option_type = 'CE'

    atm = round(end_price / 100) * 100

    if option_type == 'PE':
        hedge_strike = atm * (1 + 0.02)  
    else:
        hedge_strike = atm * (1 - 0.02)  
            
    hedge_strike = round(hedge_strike / 100) * 100

    #OPTIONS
    options_db_path = 'OPT.db'  
    conn_options = sqlite3.connect(options_db_path)

    query_options = f'SELECT * FROM "{entry_date}"'  
    df_next_day_options = pd.read_sql_query(query_options, conn_options)

    #expiry
    entry_date_dt = datetime.strptime(entry_date, '%d%m%Y')

    df_next_day_options['expiry'] = pd.to_datetime(df_next_day_options['expiry'], format='%d-%m-%Y')
    nearest_expiry = min(df_next_day_options['expiry'], key=lambda x: abs(x - entry_date_dt))

    filtered_options = df_next_day_options[(df_next_day_options['expiry'] == nearest_expiry) & 
                                         (df_next_day_options['time'] == '15:25:00')]

    def get_option_price(options_data, strike, option_type):
        option = options_data[(options_data['strike'] == strike) & 
                            (options_data['instrument_type'] == option_type)]
        if not option.empty:
            return (option.iloc[0]['symbol'], option.iloc[0]['close'])  
        else:
            return None

    atm_symbol, atm_price = get_option_price(filtered_options, atm, option_type)
    if atm_symbol is None:
        print(f"No ATM option found for date {entry_date}")
        return None

    hedge_symbol, hedge_price = get_option_price(filtered_options, hedge_strike, option_type)
    if hedge_symbol is None:
        print(f"No hedge option found for date {entry_date}")
        return None

    def apply_slippage(price, is_buy):
        slippage = 0.005  # 0.5%
        return price * (1 + slippage) if is_buy else price * (1 - slippage)

    atm_price = apply_slippage(atm_price, False)  # Selling ATM
    hedge_price = apply_slippage(hedge_price, True)

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

    def atm_trail_sl_and_exit(df, atm_price):
        initial_sl = atm_price * 1.05 # 5% above cuz sold
        exit_time = None
        exit_price = None
        
        next_day_df = df[df['time'] == '09:15:00']
        if not next_day_df.empty:
            next_day_price = next_day_df.iloc[0]['open']
            if next_day_price > initial_sl:
                return next_day_df.iloc[0]['time'], next_day_price
        
        stored_high = atm_price
        
        for i in range(0, len(df), 3):
            three_min_highs = df.iloc[i:i+3]['high'].tolist()
            three_min_prices = df.iloc[i:i+3]['close'].tolist()
            max_three_min = max(three_min_highs)
            
            if max_three_min < stored_high:
                stored_high = max_three_min
            
            for j in range(i, min(i+3, len(df))):
                if df.iloc[j]['high'] > stored_high:
                    exit_time = df.iloc[j]['time']
                    exit_price = df.iloc[j]['high']
                    return exit_time, exit_price
        
        if '09:45:00' in df['time'].values:
            exit_row = df[df['time'] == '09:45:00'].iloc[0]
            exit_time = exit_row['time']
            exit_price = exit_row['close']
            return exit_time, exit_price
        
        return exit_time, exit_price

    def hedge_trail_sl_and_exit(df, hedge_price):
        initial_sl = hedge_price * 0.95
        stored_low = hedge_price 
        sl = initial_sl
        
        next_day_df = df[df['time'] == '09:15:00']
        if not next_day_df.empty:  
            next_day_price = next_day_df.iloc[0]['open']
            if next_day_price < initial_sl:
                return next_day_df.iloc[0]['time'], next_day_price
        
        for i in range(0, len(df), 3):  
            three_min_lows = df.iloc[i:i+3]['low'].tolist()
            min_three_min = min(three_min_lows)
            
            if min_three_min > sl:
                initial_sl = min_three_min
            
            for j in range(i, min(i+3, len(df))):
                if df.iloc[j]['low'] < sl:
                    exit_time = df.iloc[j]['time']
                    exit_price = df.iloc[j]['low']
                    return exit_time, exit_price
        
        if '09:45:00' in df['time'].values:
            exit_row = df[df['time'] == '09:45:00'].iloc[0]
            exit_time = exit_row['time']
            exit_price = exit_row['close']
            return exit_time, exit_price
        
        return None, None

    exit_time, exit_price = atm_trail_sl_and_exit(filtered_df, atm_price)
    if exit_time is None:
        print(f"No ATM exit found for date {next_day_date}")
        return None

    hedge_exit_time, hedge_exit_price = hedge_trail_sl_and_exit(hedge_filtered_df, hedge_price)
    if hedge_exit_time is None:
        print(f"No hedge exit found for date {next_day_date}")
        return None

    exit_price = apply_slippage(exit_price, True)
    hedge_exit_price = apply_slippage(hedge_exit_price, False)

    pnl = (atm_price - exit_price) + (hedge_exit_price - hedge_price)
    
    trade_data = {
        'entry_date': entry_date,
        'exit_date': next_day_date,
        'option_type': option_type,
        'atm_strike': atm,
        'hedge_strike': hedge_strike,
        'atm_entry': atm_price,
        'atm_exit': exit_price,
        'hedge_entry': hedge_price,
        'hedge_exit': hedge_exit_price,
        'atm_exit_time': exit_time,
        'hedge_exit_time': hedge_exit_time,
        'pnl': pnl
    }
    
    conn.close()
    conn_options.close()
    
    return trade_data

# Get all trading date pairs
trading_pairs = get_all_dates()

# Store all trades
all_trades = []

# Execute strategy for each pair of dates
for entry_date, next_day_date in trading_pairs:
    try:
        print(f"Processing trade for entry date: {entry_date}")
        trade_result = execute_strategy(entry_date, next_day_date)
        if trade_result:
            all_trades.append(trade_result)
    except Exception as e:
        print(f"Error processing dates {entry_date}-{next_day_date}: {str(e)}")
        continue


if all_trades:
    trades_df = pd.DataFrame(all_trades)
    

    
    # Export to Excel
    with pd.ExcelWriter('strategy_results.xlsx') as writer:
        # Parameters sheet
        pd.DataFrame([{
            'Entry Time': '15:25:00',
            'Exit Time': '09:45:00',
            'Strike Selection': 'ATM',
            'Hedge Strike': '2% away',
            'Slippage': '0.5%'
        }]).to_excel(writer, sheet_name='Parameters', index=False)
        
        # Daily PnL sheet
        trades_df[['entry_date', 'pnl']].to_excel(writer, 
                                                                   sheet_name='Daily PnL', 
                                                                   index=False)
        
        # All trades sheet
        trades_df.to_excel(writer, sheet_name='All Trades', index=False)
    
    print("\nStrategy Results:")
    print(f"Total PnL: {trades_df['pnl'].sum():.2f}")
    print(f"Win Rate: {(len(trades_df[trades_df['pnl'] > 0]) / len(trades_df) * 100):.2f}%")
    