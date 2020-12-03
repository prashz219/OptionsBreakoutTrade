import logging
from kiteconnect import KiteTicker
from kiteconnect import KiteConnect
import pandas as pd
import datetime
import os
import signal
pid = os.getpid()


access_token = 'AnYB'  # Update everyday
bnf_fut_insttoken = 11983362  # Change token every month
nf_fut_insttoken = 11984386  # Change token every month
capital = 20000
instrument = 'BANKNIFTY'  # for NIFTY change value here
upside_break = 29761
downside_break = 29643
target_percent = 0.02
stoploss_percent = 0.05
expiry_year = 2020
expiry_month = 12  # Integer value and add only 1 if the day is 01
expiry_day = 3     # Integer value and add only 1 if the day is 01
nifty_base = 50
banknifty_base = 100

# Initialise
api_key = '85j'
kite = KiteConnect(api_key=api_key)
kite.set_access_token(access_token)
kws = KiteTicker(api_key, access_token)

# Fetch instruments
instruments = kite.instruments(exchange='NFO')
df = pd.DataFrame(instruments)
df = (df[df['name'] == instrument])  # filter with instrument
df = (df[df['expiry'] == datetime.date(year=expiry_year, month=expiry_month, day=expiry_day)])  # filter with expiry


# To return strike value of nifty
def nifty_strike(x):
    return int(nifty_base * round(float(x)/nifty_base))


# To return strike value of banknifty
def banknifty_strike(x):
    return int(banknifty_base * round(float(x)/banknifty_base))


# to get the strike based on instrument
def roundup(x, instrument_):
    if instrument_ == 'BANKNIFTY':
        return banknifty_strike(x)
    else:
        return nifty_strike(x)


# Fetch CE trading symbol
CE_tradingsymbol = df[(df['strike'] == roundup(upside_break, instrument))
                      & (df['instrument_type'] == 'CE')].tradingsymbol
CE_tradingsymbol = CE_tradingsymbol.values[0]
# CE_tradingsymbol = 'BANKNIFTY20D0330000CE'
# print(CE_tradingsymbol)

# Fetch CE instrument token
CE_instrument_token = df[(df['strike'] == roundup(upside_break, instrument))
                         & (df['instrument_type'] == 'CE')].instrument_token
CE_instrument_token = CE_instrument_token.values[0]
# CE_instrument_token = 10464002
# print(CE_instrument_token)

# Fetch PE trading symbol
PE_tradingsymbol = df[(df['strike'] == roundup(downside_break, instrument))
                      & (df['instrument_type'] == 'PE')].tradingsymbol
PE_tradingsymbol = PE_tradingsymbol.values[0]
# PE_tradingsymbol = 'BANKNIFTY20D0329500PE'
# print(PE_tradingsymbol)

# Fetch PE instrument token
PE_instrument_token = df[(df['strike'] == roundup(downside_break, instrument))
                         & (df['instrument_type'] == 'PE')].instrument_token
PE_instrument_token = PE_instrument_token.values[0]
# PE_instrument_token = 9264898
# print(PE_instrument_token)

trigger = {'status': {}}


def on_ticks(ws, ticks):
    logging.debug("Ticks: {}".format(ticks))
    for instrument_futures in ticks:
        inst_ltp = instrument_futures['last_price']  # Capture LTP from market ticks
        print(inst_ltp)

        # If the condition is met upside
        if (inst_ltp >= upside_break) and ("bought" not in trigger['status']):
            print('BREAKOUT UPSIDE at price:', inst_ltp)
            # This appends bought when condition is met and stops duplicate orders being placed
            trigger['status'] = 'bought'
            qty = calculate_qty(CE_instrument_token, instrument)  # Calculate Quantity
            # Place order at market price
            order_number = place_order_limit_buy(CE_tradingsymbol, CE_instrument_token, qty)
            while True:
                order_status = kite.order_history(order_number)
                order_status = order_status[-1]['status']
                if order_status == 'COMPLETE':
                    # calculate avg buy price
                    avg_buy_price = get_avg_buy_price_orderbook(order_number)
                    print('Buy avg price : ' + str(avg_buy_price))
                    # Calculate target price
                    target_price = calculate_target_price(avg_buy_price)
                    # Place target order
                    target_order_number = place_target_order(CE_tradingsymbol, qty, target_price)
                    # Calculate stoploss price
                    stoploss_price = calculate_sl_price(avg_buy_price)
                    # Place stoploss order
                    stoploss_order_number = place_stoploss_order(CE_tradingsymbol, qty, stoploss_price)
                    # Check if target hit or sl hit and exit
                    check_target_sl_order_trigger(target_order_number, stoploss_order_number)

        # If the condition is met downside
        elif (inst_ltp <= downside_break) and ("sold" not in trigger['status']):
            print("BREAKOUT DOWNSIDE at price:", inst_ltp)
            # This appends sold when condition is met and stops duplicate orders being placed
            trigger['status'] = 'sold'
            # Calculate Quantity
            qty = calculate_qty(PE_instrument_token, instrument)
            # Place order at market price
            order_number = place_order_limit_buy(PE_tradingsymbol, PE_instrument_token, qty)
            while True:
                order_status = kite.order_history(order_number)
                order_status = order_status[-1]['status']
                if order_status == 'COMPLETE':
                    # calculate avg buy price
                    avg_buy_price = get_avg_buy_price_orderbook(order_number)
                    print('Buy avg price : ' + str(avg_buy_price))
                    # Calculate target price
                    target_price = calculate_target_price(avg_buy_price)
                    # Place target order
                    target_order_number = place_target_order(PE_tradingsymbol, qty, target_price)
                    # Calculate stoploss price
                    stoploss_price = calculate_sl_price(avg_buy_price)
                    # Place stoploss order
                    stoploss_order_number = place_stoploss_order(PE_tradingsymbol, qty, stoploss_price)
                    # Check if target hit or sl hit and exit
                    check_target_sl_order_trigger(target_order_number, stoploss_order_number)


def calculate_qty(inst_token, instrument_):
    if instrument_ == 'BANKNIFTY':
        lotsize = 25
    else:
        lotsize = 75
    pe_info = kite.ltp(inst_token)
    pe_ltp = pe_info[str(inst_token)]['last_price']
    qty = (round((capital / pe_ltp) / lotsize)) * lotsize
    return qty


def place_order_limit_buy(tradingsymbol, instrument_token, qty):
    # place_order = kite.place_order('regular', 'NFO', tradingsymbol, 'BUY', qty, 'MIS', 'MARKET')
    place_order = kite.place_order('regular', 'NFO', tradingsymbol, 'BUY', qty, 'MIS', 'LIMIT',
                                   price=get_limit_price(instrument_token))
    print(place_order + " Buy order placed for " + tradingsymbol + " Qty "
          + str(qty))
    return place_order


def get_limit_price(instrument_token):
    current_date = datetime.datetime.now()
    current_time = current_date.strftime("%Y-%m-%d %H:%M:%S")
    previous_date = current_date - datetime.timedelta(minutes=1)
    previous_time = previous_date.strftime("%Y-%m-%d %H:%M:%S")
    previous_candle_close = kite.historical_data(instrument_token, from_date=previous_time,
                                                 to_date=current_time, interval='minute')
    previous_candle_close = previous_candle_close[0]
    previous_candle_close = previous_candle_close['close']
    limit_price = round((previous_candle_close + (previous_candle_close * 0.002)), 1)
    return limit_price


# def get_avg_buy_price_positions(tradingsymbol):
#     postions = kite.positions()
#     df = pd.DataFrame(postions['net'])
#     ce_buy_avg_price = df[df['tradingsymbol'] == tradingsymbol].average_price
#     avg_buy_price = ce_buy_avg_price.values[0]
#     return avg_buy_price

def get_avg_buy_price_orderbook(order_number):
    df_order_details = pd.DataFrame(kite.order_trades(order_number), columns=['average_price'])
    ce_buy_avg_price = df_order_details.mean(axis=0)[0]
    avg_buy_price = round(ce_buy_avg_price, 1)
    return avg_buy_price


def get_avg_buy_price_orderbook1(order_number):
    df_order_details = pd.DataFrame(kite.order_trades(order_number), columns=['average_price'])
    print(df_order_details)
    ce_buy_avg_price = df_order_details.mean(axis=0)[0]
    avg_buy_price = round(ce_buy_avg_price, 1)
    return avg_buy_price


def calculate_target_price(avg_buy_price):
    target_price = round(avg_buy_price + (round(target_percent * avg_buy_price, 1)))
    return target_price


def calculate_sl_price(avg_buy_price):
    stoploss_price = round(avg_buy_price - (round(stoploss_percent * avg_buy_price, 1)))
    return stoploss_price


def place_target_order(tradingsymbol, qty, price):
    order_number = kite.place_order('regular', 'NFO', tradingsymbol, 'SELL', qty, 'MIS', 'LIMIT', price)
    print(order_number + " Target order placed for " + tradingsymbol + " Sell Qty "
          + str(qty) + " at price " + str(price))
    return order_number


def place_stoploss_order(tradingsymbol, qty, sl_price):
    order_number = kite.place_order('regular', 'NFO', tradingsymbol, 'SELL', qty, 'MIS',
                                    kite.ORDER_TYPE_SL, sl_price, trigger_price=(sl_price + 0.2))
    print(order_number + " Stoploss order placed for " + tradingsymbol + " Sell Qty "
          + str(qty) + " at price " + str(sl_price))
    return order_number


def check_target_sl_order_trigger(place_order_target, place_order_stoploss):
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
    elif instrument == 'NIFTY':
        ws.subscribe([nf_fut_insttoken])
        ws.set_mode(ws.MODE_LTP, [nf_fut_insttoken])
    else:
        print('Instrument is either NIFTY or BANKNIFTY, please update instrument')


def on_close(ws, code, reason):
    # On connection close stop the main loop
    # Reconnection will not happen after executing `ws.stop()`
    ws.stop()


# Assign the callbacks.
kws.on_ticks = on_ticks
kws.on_connect = on_connect
# kws.on_close = on_close

kws.connect()