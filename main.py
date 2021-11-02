import argparse
import os
from datetime import datetime
from dataclasses import dataclass
from pprint import pprint
import time
import requests
import json

API_KEY = os.environ.get('ETHERSCAN_API_KEY')
API_BASE_URL = "https://api.etherscan.io/api"


@dataclass
class Position:
    buy_token: str
    buy_size: float
    sell_token: str
    sell_size: float

# the underscore convention here is used to avoid namespace collisions w/ "hash" and "in" etc...
class Transaction:
    def __init__(self, json_repr: dict):
        self._hash = json_repr.get("hash")
        self._from = json_repr.get("from")
        self._to = json_repr.get("to")
        self._contract = json_repr.get("contractAddress")
        self._token = json_repr.get("tokenSymbol")
        self._size = round(int(json_repr.get("value")) / 10**(int(json_repr.get("tokenDecimal"))),2)

    def __repr__(self):
        return f"transfer {self._size} {self._token}\
                from {print_hash(self._from, 42)}\
                to {print_hash(self._to, 42)}\
                at {print_hash(self._hash, 66)}"

class Swap:
    def __init__(self, _in: Transaction, _out: Transaction):
        self._in = _in
        self._out = _out
        # assert(str(self._in._from) == str(self._out._to))
        # fix and re-impl

        self.taker = self._in._from
        self.maker = self._in._to
        self.position = Position(
                self._out._token,
                self._out._size,
                self._in._token,
                self._in._size)
        if self.position.sell_size != 0:
            self.price = round(self.position.buy_size / self.position.sell_size, 4)
        else:
            self.price = 0

    def __repr__(self):
        return f"{print_hash(self.taker)} BUY {self.position.buy_size} {self.position.buy_token}\
                WITH {self.position.sell_size} {self.position.sell_token} (@{self.price})\
                AGAINST {print_hash(self.maker)}"

class TimeFrame:
    def __init__(self, transactions: list):
        self.txs = transactions
        self.swaps = []
        self.oi = {}

        for i in range(len(self.txs) - 1):
        # if 2 consecutive txs were done in the same txhash, try to create swap object
            if self.txs[i]._hash == self.txs[i + 1]._hash:
                self.swaps.append(Swap(self.txs[i], self.txs[i+1]))
        
        for swap in self.swaps:
            if swap.taker not in self.oi:
                self.oi.update({swap.taker: dict()})

            if swap.position.buy_token in self.oi[swap.taker]:
                self.oi[swap.taker][swap.position.buy_token] += swap.position.buy_size
            else:
                self.oi[swap.taker].update({swap.position.buy_token: swap.position.buy_size})

            if swap.position.sell_token in self.oi[swap.taker]:
                self.oi[swap.taker][swap.position.sell_token] -= swap.position.sell_size
            else:
                self.oi[swap.taker].update({swap.position.sell_token: -1 * swap.position.sell_size})

        for k, v in self.oi.items():
            for k2, v2 in v.items():
                self.oi[k][k2] = round(v2, 4)

    def prune(self, token: str, min_tokens: float):
        unders = []
        for k, v in self.oi.items():
            total = 0
            for k2, v2 in v.items():
                if k2 == token:
                    total += v2
            if abs(total) < min_tokens:
                unders.append(k)
        for under in unders:
            del self.oi[under]

    def net_flows(self) -> dict:
        totals = {}
        for k, v in self.oi.items():
            for k2, v2 in v.items():
                if not totals.get(k2):
                    totals.update({k2: round(v2,2)})
                totals[k2] += round(v2,2)
        return totals
                

    def __repr__(self):
        return f"{len(self.swaps)} in swap timeframe"
        

def print_hash(_hash: str, length: int) -> str:
    return _hash.strip('0x')[:4] + '..' + _hash.strip('0x')[(length-5):]

def etherscan_call(module: str, **kwargs):
    url = API_BASE_URL + f"?module={module}"
    for k, v in kwargs.items():
        url += f"&{k}={v}"
    url += f"&apikey={API_KEY}"
    
    resp = requests.get(url)
    data = resp.json()
    if data.get('message') == "OK" and data.get('result'):
        return data.get('result')
    else:
        return data


def get_block_from_time(timestamp: str) -> int:
    unixtime = int(time.mktime(timestamp.timetuple()))

    params = {"action": "getblocknobytime",
              "timestamp": unixtime,
              "closest": "before"}
    return etherscan_call("block", **params)

def get_transfers_by_addr(start_date: str, end_date: str, addr: str) -> dict:
    start_block = get_block_from_time(start_date)
    end_block = get_block_from_time(end_date)

    params = {"action": "tokentx",
              "address": addr, 
              "startblock": start_block,
              "endblock": end_block,
              "sort": "asc"}
    return etherscan_call("account", **params)

def valid_date(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        msg = "not a valid date: {0!r}".format(s)
        raise argparse.ArgumentTypeError(msg)

def valid_addr(s):
    try:
        assert(len(s) == 42 and s[:2] == '0x')
    except AssertionError:
        msg = "not a valid ethereum address!"
        raise argparse.ArgumentTypeError(msg)


def main():
    parser = argparse.ArgumentParser(description='scan contract for accumulators')
    parser.add_argument("-s", 
    "--start", 
    help="The Start Date - format YYYY-MM-DD", 
    required=False, 
    type=valid_date)

    parser.add_argument("-e", 
    "--end", 
    help="The End Date - format YYYY-MM-DD", 
    required=False, 
    type=valid_date)

    parser.add_argument("-p",
    "--pool",
    help="the target liquidity pool",
    required=False,
    type=str)

    args = parser.parse_args()
    # print(get_block_from_time(args.start))
    api_response = get_transfers_by_addr(args.start, args.end, args.pool)
    # print(resp)
    txs = []
    for event in api_response:
        txs.append(Transaction(event))
    tf = TimeFrame(txs)
    print(tf.net_flows())
    # tf.prune("SLP", 500000)
    pprint(tf)
    pprint(tf.oi)
    pprint(tf.net_flows())

if __name__ == "__main__":
    main()

