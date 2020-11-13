import OptionsBreakoutTrade.connecttodatabase as db

def get_instrument_token_instrument(tradingsymbol_):
    sql_query = "SELECT instrument_token FROM {}.{} where tradingsymbol = '{}' ;".format(db.creds.PGSCHEMA, "instrument",str(tradingsymbol_))
    data = db.getsqldata(sql_query)
    result = data['instrument_token'][0]
    return result