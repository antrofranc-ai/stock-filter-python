import datetime
import json
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from flask import Flask, jsonify, render_template
from requests.utils import quote
import html5lib
from flask_socketio import SocketIO, send, emit
import eventlet
import traceback

import concurrent.futures

# ----------------------
# Columns in stock_info
# ----------------------
# open
# high
# low
# ltP
# ptsC
# per
# prevClose
# gapPer

app = Flask(__name__)
socketio = SocketIO(app)

session_data = {}

def get_prev_close(symbol):
    url = 'https://www.nseindia.com/live_market/dynaContent/live_watch/get_quote/GetQuote.jsp?symbol=' + \
        quote(symbol)
    r = requests.get(url)
    prev_close = json.loads(BeautifulSoup(r.text, 'html5lib').find(
        id="responseDiv").get_text())['data'][0]['previousClose']
    return (symbol, prev_close)

def add_col_prevClose(data, load_static_data):
    if 'prevClose' not in data:
        data['prevClose'] = np.nan
    if load_static_data:
        with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
            futures = [executor.submit(get_prev_close, symbol)
                    for symbol in data.index.values]
            for future in concurrent.futures.as_completed(futures):
                symbol, prev_close_str = future.result()
                prev_close = float(clean_numeric_data(prev_close_str))
                data.at[symbol, 'prevClose'] = prev_close
                print(symbol, prev_close)

def add_col_gapPer(data):
    data['gapPer'] = (data['open'] - data['prevClose']) * 100.0 / data['prevClose']

def clean_numeric_data(num_txt):
    if num_txt == '-':
        return "0"
    return num_txt.replace(',', '')

@app.route("/")
def index():
    print("%s Home accessed" % (datetime.datetime.now())) 
    return render_template('index.html')

def do_filter(params, stock_info):

    mask = None

    # Filter-1 : Min-Max price
    min_price = params['min_price']
    min_price = float(min_price) if min_price else float('-inf')
    max_price = params['max_price']
    max_price = float(max_price) if max_price else float('inf')
    mask = (stock_info['ltP'] >= min_price) & (stock_info['ltP'] <= max_price)

    # Filter-2 : Gap Up & Down %
    gap_up_per = params['gap_up_per']
    gap_up_mask = None
    gap_down_mask = None
    if gap_up_per:
        print('gap up % is enabled')
        gap_up_per = float(gap_up_per)
        gap_up_mask = (stock_info['gapPer'] >= 0) & (stock_info['gapPer'] <= gap_up_per)
    gap_down_per = params['gap_down_per']
    if gap_down_per:
        print('gap down % is enabled')
        gap_down_per = float(gap_down_per) * -1.0
        gap_down_mask = (stock_info['gapPer'] <= 0) & (stock_info['gapPer'] >= gap_down_per)
    if gap_up_per and gap_down_per:
        mask &= gap_up_mask | gap_down_mask
    elif gap_up_per:
        mask &= gap_up_mask
    elif gap_down_per:
        mask &= gap_down_mask

    # Filter-3 : Open price same as Low Price by x%
    open_low_same_per = params['open_low_same_per']
    if open_low_same_per:
        mask &= (((stock_info['open'] - stock_info['low']) * 100.0) / stock_info['low']) <= float(open_low_same_per)

    # Filter-4 : Open price same as High Price by x%
    open_high_same_per = params['open_high_same_per']
    if open_high_same_per:
        mask &= (((stock_info['high'] - stock_info['open']) * 100.0) / stock_info['high']) <= float(open_high_same_per)

    return stock_info[mask]

@socketio.on('echo_test')
def handle_message(message):
    print('echo_test received, sending back : ' + str(message))
    emit('echo_test', message)

@socketio.on('filter')
def handle_filter(params):
    print('%s : Filtering with params: %s' % (datetime.datetime.now(), str(params)) )
    filtered_stock_info_json = ''

    try:
        if 'stock_info' in session_data:
            stock_info = session_data['stock_info']
            filtered_stock_info = do_filter(params, stock_info)
            filtered_stock_info_json = filtered_stock_info.to_json(orient='records')
        emit('stock_data', filtered_stock_info_json)
    except Exception as e:
        error_details = traceback.format_exc()
        print(error_details)
        emit('error_data', error_details)

@socketio.on('load_all_data')
def load_all_data(form_params):
    print('%s : load_all_data with form_params: %s' % (datetime.datetime.now(), str(form_params)) )
    update_stock_data(form_params, load_static_data=True)

@socketio.on('refresh_prices')
def refresh_prices(form_params):
    print('%s : refresh_prices with form_params: %s' % (datetime.datetime.now(), str(form_params)) )
    update_stock_data(form_params, load_static_data=False)

def update_stock_data(form_params, load_static_data=False):
    try:

        new_stock_info = get_nse_fo_data()

        stock_info = None
        if ('stock_info' not in session_data) or load_static_data:
            stock_info = new_stock_info
        else:
            stock_info = session_data['stock_info']
            for column in new_stock_info.columns:
                stock_info[column] = new_stock_info[column]
        
        # Add new columns
        add_col_prevClose(stock_info, load_static_data)

        # Add derived/calculated columns
        add_col_gapPer(stock_info)

        # Update session data
        session_data['stock_info'] = stock_info

        # Filter data
        filtered_stock_info = do_filter(form_params, stock_info)

        # Emit data
        filtered_stock_info_json = filtered_stock_info.to_json(orient='records')
        emit('stock_data', filtered_stock_info_json)
    except Exception as e:
        error_details = traceback.format_exc()
        print(error_details)
        emit('error_data', error_details)

def get_nse_fo_data():
    resp = requests.get(
        'https://www.nseindia.com/live_market/dynaContent/live_watch/stock_watch/foSecStockWatch.json')
    nse_json = resp.json()
    stock_info_list = nse_json['data']

    # clean all number columns
    for data_dict in stock_info_list:
        data_dict.update({k: clean_numeric_data(v)
                        for k, v in data_dict.items()})

    # Set numeric types for columns
    stock_info = pd.DataFrame.from_dict(nse_json['data']).astype(dtype={
        "open": np.float32,
        "high": np.float32,
        "low": np.float32,
        "ltP": np.float32,
        "ptsC": np.float32,
        "per": np.float32,
        "trdVol": np.float32,
        "trdVolM": np.float32,
        "ntP": np.float32,
        "mVal": np.float32,
        "wkhi": np.float32,
        "wklo": np.float32,
        "wkhicm_adj": np.float32,
        "wklocm_adj": np.float32,
        "yPC": np.float32,
        "mPC": np.float32
    })
    stock_info.set_index('symbol', inplace=True, drop=False)
    return stock_info

if __name__ == "__main__":
    print('Starting server')
    socketio.run(app, host='0.0.0.0', port=8080, use_reloader=True, extra_files=['templates/index.html'])
