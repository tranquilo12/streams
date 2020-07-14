polygon_trades_cols = """event_type, symbol_ticker, trade_id, exchange_id, price, trade_size, trade_conditions, trade_timestamp, tape"""
insert_into_polygon_trades = f"""INSERT INTO polygon_stocks_trades({polygon_trades_cols}) 
                                 VALUES (%(ev)s, %(sym)s, %(i)s, %(x)s, %(p)s, %(s)s, %(c)s, %(t)s, %(z)s) 
                                 ON CONFLICT (event_type, symbol_ticker, exchange_id, trade_timestamp) DO NOTHING """

polygon_quotes_cols = """event_type, symbol_ticker, quote_condition, bid_exchange_id, ask_exchange_id, bid_price, ask_price, bid_size, ask_size, quote_timestamp, tape"""
insert_into_polygon_quotes = f"""INSERT INTO polygon_stocks_quotes({polygon_quotes_cols}) 
                                 VALUES (%(ev)s, %(sym)s, %(c)s, %(bx)s, %(ax)s, %(bp)s, %(ap)s, %(bs)s, %(as)s, %(t)s, %(z)s) 
                                 ON CONFLICT (event_type, symbol_ticker, bid_exchange_id, ask_exchange_id, quote_timestamp) DO NOTHING """

polygon_agg_cols = """event_type, symbol_ticker, tick_volume, accumulated_volume, official_opening_price, vwap, open, close, high, low, tick_average, tape, n, start_timestamp, end_timestamp"""
insert_into_polygon_agg = f"""INSERT INTO polygon_stocks_agg({polygon_agg_cols}) 
                              VALUES (%(ev)s, %(sym)s, %(v)s, %(av)s, %(op)s, %(vw)s, %(o)s, %(c)s, %(h)s, %(l)s, %(a)s, %(z)s, %(n)s, %(s)s, %(e)s) 
                              ON CONFLICT (event_type, symbol_ticker, start_timestamp, end_timestamp) DO NOTHING """

polygon_stocks_bbo_quotes_cols = """ticker, sip_timestamp, exchange_timestamp, trf_timestamp, sequence_number, conditions, indicators, bid_price, bid_exchange_id, bid_size, ask_price, ask_exchange_id, ask_size, tape"""
insert_into_polygon_stocks_bbo = f"""INSERT INTO polygon_stocks_bbo_quotes({polygon_stocks_bbo_quotes_cols})
                                     VALUES ((%(timestamp)s, %(sip_timestamp)s, %(exchange_timestmap)s, %(trf_timestamp)s, %(sequence_number)s, %(conditions)s, %(indicators)s, %(bid_price)s, %(bid_exchange_id)s, %(bid_size)s, %(ask_price)s, %(ask_exchange_id)s, %(ask_size)s, %(tape)s)
                                     ON CONFLICT (ticker, sip_timestamp, exchange_timestamp, trf_timestamp, sequence_number) DO NOTHING """

polygon_stocks_agg_candles = """ticker, volume, open, close, high, low, timestamp, n_items"""
insert_into_polygon_stocks_agg_candles = f"""INSERT INTO polygon_stocks_agg_candles({polygon_stocks_agg_candles})
                                             VALUES (%(ticker)s, %(volume)s, %(open)s, %(close)s, %(high)s, %(low)s, %(timestamp)s, %(n_items)s)
                                             ON CONFLICT (ticker, volume, timestamp) DO NOTHING """
