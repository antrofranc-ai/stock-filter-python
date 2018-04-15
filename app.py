import json
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from flask import Flask, jsonify, render_template, request
from requests.utils import quote
import html5lib
from flask_socketio import SocketIO, send, emit
import gevent

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


def add_col_prevClose(data):
    data['prevClose'] = np.nan
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

def get_form_param(key, default_value):
    value = request.form[key]
    return value if value else default_value


@app.route("/")
def index():
    return render_template('index.html')


@app.route("/filter", methods=['POST'])
def filter_data():
    
    model_map = {}

    if 'stock_info' in session_data:
        stock_info = session_data['stock_info']
        params = request.form
        filtered_stock_info = do_filter(params, stock_info)
        model_map['stock_info'] = filtered_stock_info.to_json(orient='records')
    else:
        model_map['stock_info'] = {}
    
    return jsonify(model_map)

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

    return stock_info[mask]

@app.route("/update_data", methods=['POST'])
def update_data():
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

    # ADD COLUMN : prevClose
    add_col_prevClose(stock_info)

    # ADD COLUMN : gapPer
    add_col_gapPer(stock_info)

    # Update session data
    session_data['stock_info'] = stock_info

    model_map = {}
    model_map['stock_info'] = session_data['stock_info'].to_json(
        orient='records')

    return jsonify(model_map)

@socketio.on('echo_test')
def handle_message(message):
    print('echo_test received, sending back : ' + str(message))
    emit('echo_test', message)

@socketio.on('filter')
def handle_filter(params):
    print('filter: ' + str(params))

@socketio.on('update')
def handle_update(params):
    print('update: ' + str(params))

if __name__ == "__main__":
    print('Starting server')
    socketio.run(app, host='0.0.0.0', port=8080, use_reloader=True, log_output=True, extra_files=['templates/index.html'])
