import logging
import pandas as pd
from OptionsBreakoutTrade import login
from kiteconnect import KiteTicker
import time
import os, signal
import OptionsBreakoutTrade.DataAccessLayer as dal

pid = os.getpid()
nifty_base = 50
banknifty_base = 100
CE = 'CE'
PE = 'PE'

capital = 25000 # needs update
instrument ='BANKNIFTY' # needs update
expiry = '20N19' # needs update
upside_break = 28001 # needs update
downside_break = 24123 # needs update
target_percent = 0.03 # Set target %
stoploss_percent = 0.03 # Set stoploss %

# To return strike value of nifty
def nifty_strike(x):
    return int(nifty_base * round(float(x)/nifty_base))

# To return strike value of banknifty
def banknifty_strike(x):
    return int(banknifty_base * round(float(x)/banknifty_base))

# to get the strike based on instrument
def roundup(x,instrument_):
    if (instrument_ == 'BANKNIFTY'):
        return banknifty_strike(x)
    else:
        return nifty_strike(x)

# Fetch CE trading symbol
CE_tradingsymbol = instrument+expiry+str(roundup(upside_break,instrument))+CE

# Fetch CE instrument token
CE_inst_token = dal.get_instrument_token_instrument(CE_tradingsymbol)

# Fetch CE trading symbol
PE_tradingsymbol = instrument+expiry+str(roundup(downside_break,instrument))+PE

# Fetch CE instrument token
PE_inst_token = dal.get_instrument_token_instrument(PE_tradingsymbol)


token_details = {260105: {'name': instrument}, CE_inst_token: {'name': CE_tradingsymbol}, PE_inst_token: {'name': PE_tradingsymbol}}
tokens = [260105]
kite = login.set_kite_obj()
kws = KiteTicker(login.key_secret[0], login.access_token)


def on_ticks(ws, ticks):
    logging.debug("Ticks: {}".format(ticks))
    for bnf in ticks:

        # Capture LTP from market ticks
        inst_token = bnf['instrument_token']
        name = token_details[inst_token]['name']
        ltp = bnf['last_price']
        print(ltp)

        # If the condition is met in the upside then orders are processed
        if (ltp >= upside_break) and ("bought" not in token_details[inst_token].values()):
            print(name, "BREAKOUT UPSIDE at price:", ltp)

            # This appends bought when condition is met and stops duplicate orders being placed
            token_details[inst_token]['status'] = "bought"

            # Calculate Quantity
            CE_info = kite.ltp(CE_inst_token)
            CE_ltp = CE_info[str(CE_inst_token)]['last_price']
            qty = (round((capital / CE_ltp) / 25)) * 25

            # Place order at market price
            place_order = kite.place_order('regular', 'NFO', CE_tradingsymbol, 'BUY', qty, 'MIS', 'MARKET')
            # print(time.ctime() + place_order +" Order placed for " + instrument + expiry + CE_strike + " Qty " + str(qty) + " at price "+ str(CE_ltp))

            # Fetch average buy price
            df_order_details = pd.DataFrame(kite.order_trades(place_order), columns=['average_price'])
            avg_price_list = df_order_details.values.tolist()
            avg_buy_price = avg_price_list[0][0]
            print(time.ctime() + str(place_order) + " Order placed for " + CE_tradingsymbol + " Qty " + str(qty) + " at price " + str(avg_buy_price))

            # Calculate target and stoploss
            target_price = round(CE_ltp + (round(target_percent * avg_buy_price, 1)))
            stoploss_price = round(CE_ltp - (round(stoploss_percent * avg_buy_price, 1)))

            # Place target order
            place_order_target = kite.place_order('regular', 'NFO', CE_tradingsymbol, 'SELL', qty, 'MIS', 'LIMIT', target_price)
            print(time.ctime() + place_order_target +" Target Order Placed at " + str(target_price) + " Qty " + str(qty))

            # Place Stoploss order
            time.sleep(1)
            place_order_stoploss = kite.place_order('regular', 'NFO', CE_tradingsymbol, 'SELL', qty, 'MIS', kite.ORDER_TYPE_SL, stoploss_price, trigger_price=(stoploss_price + 0.5))
            print(time.ctime() + place_order_stoploss +" Stoploss Order Placed at " + str(stoploss_price) + " Qty " + str(qty))

            # Check if Target or SL order is triggered
            df_order_details = pd.DataFrame(kite.orders(), columns=['order_id', 'status'])
            filt1 = (df_order_details['order_id'] == place_order_target) & (df_order_details['status'] == 'OPEN')
            filt2 = (df_order_details['order_id'] == place_order_stoploss) & (
                    df_order_details['status'] == 'TRIGGER PENDING')
            print('Target and Stoploss order pending')

            while (df_order_details[filt1].count()[0] == df_order_details[filt2].count()[0]) | (df_order_details[filt1].count()[0] != df_order_details[filt2].count()[0]):
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


        # If the condition is met in the downside then orders are processed
        elif (ltp <= downside_break) and ("sold" not in token_details[inst_token].values()):
            print(name, "BREAKOUT DOWNSIDE at price:", ltp)

            # This appends sold when condition is met and stops duplicate orders being placed
            token_details[inst_token]['status'] = "sold"

            # Place order at market price
            PE_info = kite.ltp(PE_inst_token)
            PE_ltp = PE_info[str(PE_inst_token)]['last_price']
            qty = (round((capital/PE_ltp)/25))*25
            place_order = kite.place_order('regular', 'NFO', PE_tradingsymbol, 'BUY', qty, 'MIS', 'MARKET')
            # print(time.ctime() + place_order + "Order placed for " + instrument + expiry + PE_strike + " Qty " + str(qty) + " at price " + str(PE_ltp))

            # Fetch average buy price
            df_order_details = pd.DataFrame(kite.order_trades(place_order), columns=['average_price'])
            avg_price_list = df_order_details.values.tolist()
            avg_buy_price = avg_price_list[0][0]
            print(time.ctime() + str(place_order) + "Order placed for " + PE_tradingsymbol + " Qty " + str(qty) + " at price " + str(avg_buy_price))

            # Calculate target and stoploss
            target_price = round(PE_ltp + (round(target_percent * avg_buy_price, 1)))
            stoploss_price = round(PE_ltp - (round(stoploss_percent * avg_buy_price, 1)))


            # Place target order
            place_order_target = kite.place_order('regular', 'NFO', PE_tradingsymbol, 'SELL', qty, 'MIS', 'LIMIT', target_price)
            print(time.ctime() + place_order_target +" Target Order Placed at " + str(target_price) + " Qty " + str(qty))

            # Place Stoploss order
            time.sleep(1)
            place_order_stoploss = kite.place_order('regular', 'NFO', PE_tradingsymbol, 'SELL', qty, 'MIS', kite.ORDER_TYPE_SL, stoploss_price, trigger_price=(stoploss_price+0.5))
            print(time.ctime() + place_order_stoploss +" Stoploss Order Placed at " + str(stoploss_price) + " Qty " + str(qty))

            # Check if Target or SL order is triggered
            df_order_details = pd.DataFrame(kite.orders(), columns=['order_id', 'status'])
            filt1 = (df_order_details['order_id'] == place_order_target) & (df_order_details['status'] == 'OPEN')
            filt2 = (df_order_details['order_id'] == place_order_stoploss) & (
                        df_order_details['status'] == 'TRIGGER PENDING')
            print('Target and Stoploss order pending')
            while (df_order_details[filt1].count()[0] == df_order_details[filt2].count()[0]) | (df_order_details[filt1].count()[0] != df_order_details[filt2].count()[0]):
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
    logging.debug("on connect: {}".format(response))
    ws.subscribe(tokens)
    ws.set_mode(ws.MODE_LTP, tokens)

def on_close(ws, code, reason):
    # On connection close stop the main loop
    # Reconnection will not happen after executing `ws.stop()`
    ws.stop()



kws.on_ticks = on_ticks
kws.on_connect=on_connect
kws.connect()