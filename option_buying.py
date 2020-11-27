import logging
from kiteconnect import KiteTicker
from kiteconnect import KiteConnect
import pandas as pd
import datetime
import os, signal
pid = os.getpid()

access_token = accesstoken # Update access token
bnf_fut_insttoken = 11983362
nf_fut_insttoken = 11984386
capital = 25000
instrument = 'BANKNIFTY'
upside_break = 29640
downside_break = 29635
target_percent = 0.02
stoploss_percent = 0.05
expiry_year = 2020 # Integer
expiry_month = 12  # Integer
expiry_day = 3     # Integer value and add only 1 if the day is 01
nifty_base = 50 # need not change
banknifty_base = 100 # need not change

# Initialise
api_key = apikey # update api key
kite = KiteConnect(api_key=api_key)
kite.set_access_token(access_token)
kws = KiteTicker(api_key, access_token)

# Fetch instruments
instruments = kite.instruments(exchange='NFO')
df = pd.DataFrame(instruments)
df = (df[df['name'] == instrument]) # filter with banknifty
df = (df[df['expiry'] == datetime.date(year=expiry_year,month=expiry_month,day=expiry_day)]) # filter with expiry

# To return strike value of nifty
def nifty_strike(x):
    return int(nifty_base * round(float(x)/nifty_base))

# To return strike value of banknifty
def banknifty_strike(x):
    return int(banknifty_base * round(float(x)/banknifty_base))

# to get the strike based on instrument
def roundup(x,instrument):
    if (instrument == 'BANKNIFTY'):
        return banknifty_strike(x)
    else:
        return nifty_strike(x)

# Fetch CE trading symbol
CE_tradingsymbol = df[(df['strike'] == roundup(upside_break,instrument)) & (df['instrument_type'] == 'CE')].tradingsymbol
CE_tradingsymbol = CE_tradingsymbol.values[0]
# CE_tradingsymbol = 'BANKNIFTY20D0331000CE'
# print(CE_tradingsymbol)

# Fetch CE instrument token
CE_instrument_token = df[(df['strike'] == roundup(upside_break,instrument)) & (df['instrument_type'] == 'CE')].instrument_token
CE_instrument_token = CE_instrument_token.values[0]
# print(CE_instrument_token)

# Fetch PE trading symbol
PE_tradingsymbol = df[(df['strike'] == roundup(downside_break,instrument)) & (df['instrument_type'] == 'PE')].tradingsymbol
PE_tradingsymbol = PE_tradingsymbol.values[0]
# print(PE_tradingsymbol)

# Fetch PE instrument token
PE_instrument_token = df[(df['strike'] == roundup(downside_break,instrument)) & (df['instrument_type'] == 'PE')].instrument_token
PE_instrument_token = PE_instrument_token.values[0]
# print(PE_instrument_token)

trigger ={1: {}}

def on_ticks(ws, ticks):
    # Callback to receive ticks.
    logging.debug("Ticks: {}".format(ticks))
    for instrument_futures in ticks:
        # Capture LTP from market ticks
        inst_ltp = instrument_futures['last_price']
        print(inst_ltp)

        # If the condition is met upside
        if ((inst_ltp >= upside_break) and ("bought" not in trigger[1].values())):
            print('BREAKOUT UPSIDE at price:', inst_ltp)
            trigger['status'] = 'bought' # This appends bought when condition is met and stops duplicate orders being placed
            qty = calculate_qty(CE_instrument_token) # Calculate Quantity
            order_number = place_order_market_buy(CE_tradingsymbol,qty) # Place order at market price
            # order_number = 201127002770261
            avg_buy_price = get_avg_buy_price_orderbook(order_number) # calculate avg buy price
            target_price = calculate_target_price(avg_buy_price) # Calculate target price
            target_order_number = place_target_order(CE_tradingsymbol,qty,target_price) # Place target order
            stoploss_price = calculate_sl_price(avg_buy_price) # Calculate stoploss price
            stoploss_order_number = place_stoploss_order(CE_tradingsymbol,qty,stoploss_price) # Place stoploss order
            check_target_sl_order_trigger(target_order_number,stoploss_order_number) # Check if target hit or sl hit and exit

        # If the condition is met downside
        elif ((inst_ltp <= downside_break) and ("sold" not in trigger[1].values())):
            print("BREAKOUT DOWNSIDE at price:", inst_ltp)
            trigger['status'] = 'sold'  # This appends sold when condition is met and stops duplicate orders being placed
            qty = calculate_qty(PE_instrument_token) # Calculate Quantity
            order_number = place_order_market_buy(PE_instrument_token, qty)  # Place order at market price
            # order_number = 201127002770261
            avg_buy_price = get_avg_buy_price_orderbook(order_number)  # calculate avg buy price
            target_price = calculate_target_price(avg_buy_price)  # Calculate target price
            target_order_number = place_target_order(PE_instrument_token, qty, target_price)  # Place target order
            stoploss_price = calculate_sl_price(avg_buy_price)  # Calculate stoploss price
            stoploss_order_number = place_stoploss_order(PE_instrument_token, qty, stoploss_price)  # Place stoploss order
            check_target_sl_order_trigger(target_order_number,stoploss_order_number)  # Check if target hit or sl hit and exit


def calculate_qty(inst_token):
    PE_info = kite.ltp(inst_token)
    PE_ltp = PE_info[str(inst_token)]['last_price']
    qty = (round((capital / PE_ltp) / 25)) * 25
    return qty

def place_order_market_buy(tradingsymbol,qty):
    place_order = kite.place_order('regular', 'NFO', tradingsymbol, 'BUY', qty, 'MIS', 'MARKET')
    print(place_order + " Buy order placed for " + tradingsymbol + " Qty " + str(qty) + " at price " + str(get_avg_buy_price_orderbook(tradingsymbol)))
    return place_order

# def get_avg_buy_price_positions(tradingsymbol):
#     postions = kite.positions()
#     df = pd.DataFrame(postions['net'])
#     CE_buy_avg_price = df[df['tradingsymbol'] == tradingsymbol].average_price
#     avg_buy_price = CE_buy_avg_price.values[0]
#     return avg_buy_price

def get_avg_buy_price_orderbook(order_number):
    df_order_details = pd.DataFrame(kite.order_trades(order_number), columns=['average_price'])
    CE_buy_avg_price = df_order_details.mean(axis=0)[0]
    avg_buy_price = round(CE_buy_avg_price, 1)
    return avg_buy_price

def calculate_target_price(avg_buy_price):
    target_price = round(avg_buy_price + (round(target_percent * avg_buy_price, 1)))
    return target_price

def calculate_sl_price(avg_buy_price):
    stoploss_price = round(avg_buy_price - (round(stoploss_percent * avg_buy_price, 1)))
    return stoploss_price

def place_target_order(tradingsymbol,qty,price):
    order_number = kite.place_order('regular', 'NFO', tradingsymbol, 'SELL', qty, 'MIS', 'LIMIT', price)
    print(order_number + " Target order placed for " + tradingsymbol + " Sell Qty " + str(qty) + " at price " + str(qty))
    return order_number

def place_stoploss_order(tradingsymbol,qty,sl_price):
    order_number = kite.place_order('regular', 'NFO', tradingsymbol, 'SELL', qty, 'MIS', kite.ORDER_TYPE_SL, sl_price,trigger_price=(sl_price + 0.5))
    print(order_number + " Stoploss order placed for " + tradingsymbol + " Sell Qty " + str(qty) + " at price " + str(qty))
    return order_number

def check_target_sl_order_trigger(place_order_target,place_order_stoploss):
    df_order_details = pd.DataFrame(kite.orders(), columns=['order_id', 'status'])
    filt1 = (df_order_details['order_id'] == place_order_target) & (df_order_details['status'] == 'OPEN')
    filt2 = (df_order_details['order_id'] == place_order_stoploss) & (df_order_details['status'] == 'TRIGGER PENDING')
    print('Target and Stoploss order pending')

    while (df_order_details[filt1].count()[0] == df_order_details[filt2].count()[0]) | (
            df_order_details[filt1].count()[0] != df_order_details[filt2].count()[0]):
        df_order_details = pd.DataFrame(kite.orders(), columns=['order_id', 'status'])
        filt1 = (df_order_details['order_id'] == place_order_target) & (df_order_details['status'] == 'OPEN')
        filt2 = (df_order_details['order_id'] == place_order_stoploss) & (
                    df_order_details['status'] == 'TRIGGER PENDING')

        # If Target or SL order is triggered then cancel the other order which is Open
        if df_order_details[filt1].count()[0] != df_order_details[filt2].count()[0]:
            if df_order_details[filt1].count()[0] == 0:
                print('Target hit')
                kite.cancel_order('regular', place_order_stoploss)
                os.kill(pid, signal.SIGTERM)
            else:
                print('Stoploss hit')
                kite.cancel_order('regular', place_order_target)
                os.kill(pid, signal.SIGTERM)


def on_connect(ws, response):
    # Callback on successful connect.
    if instrument == 'BANKNIFTY':
        ws.subscribe([bnf_fut_insttoken])
        ws.set_mode(ws.MODE_LTP, [bnf_fut_insttoken])
    else:
        ws.subscribe([nf_fut_insttoken])
        ws.set_mode(ws.MODE_LTP, [nf_fut_insttoken])

def on_close(ws, code, reason):
    # On connection close stop the main loop
    # Reconnection will not happen after executing `ws.stop()`
    ws.stop()

# Assign the callbacks.
kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.on_close = on_close

# Infinite loop on the main thread. Nothing after this will run.
# You have to use the pre-defined callbacks to manage subscriptions.
kws.connect()