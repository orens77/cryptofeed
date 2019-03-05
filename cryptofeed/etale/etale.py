import asyncio
import json
import logging

from datetime import datetime, timedelta
from decimal import Decimal
from pytz import UTC
from time import sleep, time

from sortedcontainers import SortedDict as sd

from cryptofeed.feed import Feed
from cryptofeed.defines import ETALE, L2_BOOK, TRADES, CANDLES, BUY, SELL, BID, ASK, FEE
from cryptofeed.standards import pair_exchange_to_std

wss_url = 'wss://api-uat.etale.com/api'
auth = json.dumps({'type':'LOGIN','username':'orens77@gmail.com','password':'i5P5lSyq0K'})

LOG = logging.getLogger('feedhandler')

def etale_pair_exchanges(auth):
    ''' 
    not the best way to get pairs
    we have to send auth to just get the api to respond so when it does respond 
    it automatically sends all your balances which we have to ignore
    '''
    import json
    from websocket import create_connection
    wss_url = 'wss://api-uat.etale.com/api'
    ws = create_connection(wss_url)
    ws.send(auth)
    ws.send(json.dumps({"type":"MARKET_DATA_CONFIG_REQUEST"}))
    while ws.connected:
        message =  ws.recv()
        msg = json.loads(message)
        msg_type = msg['type']
        if msg_type == 'MARKET_DATA_CONFIG':
            ws.close()
            return {info['pair'] : sorted(info['exchanges']) for info in msg['pairs']}
    raise RuntimeError("could not get marget data config from api-uat.etale.com")


class Etale(Feed):
    id = ETALE
    _pair_exchanges = None

    @staticmethod
    def pair_exchanges():
        _pair_exchanges = Etale._pair_exchanges
        if _pair_exchanges is None:
            _pair_exchanges = Etale._pair_exchanges = etale_pair_exchanges(auth)
        return _pair_exchanges


    def __init__(self, pairs=None, channels=None, callbacks=None, filter=None, track_balances=False, separate_feeds=False, **kwargs):
        '''
        filter = dictionary of iterables
            {
                pair : [exchanges],
                exch : [pairs],
                ... }
        track_balances = True or False [default] to track balances on this feed.
        '''
        super().__init__(wss_url, pairs=pairs, channels=channels, callbacks=callbacks, **kwargs)
        self._ws = None
        if filter is None:
            filter = { pair:['Bitfinex'] for pair, exchs in self._pair_exchanges.items()
                       if pair.split('-')[1] in ('CAD', 'GBP', 'EUR', 'JPY', 'KRW', 'USD') }
        self._filter = filter
        self._separate_feeds = separate_feeds
        self._track_balances = track_balances
        if pairs is not None:
            for chan in self.channels:
                self.config[chan] = self.pairs
        self._reset()

    def _reset(self):
        if self._ws is not None:
            if self._ws.open:
                self._ws.close()
        self.l2_book = {}
        self.balances = {}
        self.subscriptions = {}
        self.candlesticks = []
        self.consolidateds = []
        self.events = {}

    def _filtered_exchanges(self, pair):
        exchanges = self._pair_exchanges[pair]
        if self._filter:
            if pair in self._filter:
                return [exch for exch in exchanges if exch not in self._filter[pair]]
            return [exch for exch in exchanges if pair not in self._filter.get(exch, [])]

    async def subscribe(self, ws):
        self._reset()
        self._ws = ws
        await ws.send(auth)
        for pair in set(self.config.get(TRADES, []) + self.config.get(L2_BOOK, [])):
            exchs = self._filtered_exchanges(pair)
            if self._separate_feeds:
                for exch in exchs:
                    subscribeId = self.subscriptions.setdefault(('CONSOLIDATED', pair, exch), len(self.consolidateds))
                    self.consolidateds.append((pair, [exch]))
                    msg = json.dumps({
                        "type" : "CONSOLIDATED_MARKET_DATA_SUBSCRIBE",
                        "subscribeId" : f'cmd{subscribeId}',
                        "symbol" : pair,
                        "exchanges" : [exch],
                    })
                    LOG.debug(f"{self.id} sending { msg }")
                    await ws.send(msg)
            else:
                subscribeId = self.subscriptions.setdefault(('CONSOLIDATED', pair, ':'.join(exchs)), len(self.consolidateds))
                self.consolidateds.append((pair, exchs))
                msg = json.dumps({
                    "type" : "CONSOLIDATED_MARKET_DATA_SUBSCRIBE",
                    "subscribeId" : f'cmd{subscribeId}',
                    "symbol" : pair,
                    "exchanges" : exchs,
                })
                LOG.debug(f"{self.id} sending { msg }")
                await ws.send(msg)
        LOG.debug(f"{self.id} subscribed { len(self.consolidateds) } consolidated feeds")
        for pair in self.config.get(CANDLES, []):
            for exch in self._filtered_exchanges(pair):
                subscribeId = self.subscriptions.setdefault(('CANDLESTICK', pair, exch), len(self.candlesticks))
                self.candlesticks.append((pair, exch))
                msg = json.dumps({
                    "type" : "CANDLESTICK_MARKET_DATA_SUBSCRIBE",
                    "subscribeId" : f'bar{subscribeId}',
                    "symbol" : pair,
                    "exchange" : exch,
                    "minutes" : "1"
                })
                LOG.debug(f"{self.id} sending { msg }")
                await ws.send(msg)
        LOG.info(f"{self.id} subscribed { len(self.candlesticks) } candlesticks")

    async def message_handler(self, message):
        # msg = json.loads(message, parse_float=Decimal)
        msg = json.loads(message)
        msg['recv'] = datetime.now(tz=UTC)
        LOG.debug(msg)
        msg_type = msg['type']
        if msg_type == 'BALANCE_INFO':
            if self._track_balances:
                await self._process_balances(msg)
        elif msg_type == 'info':
            LOG.info(f'{ self.id } { msg }')
        elif msg_type == 'MARKET_DATA_CONFIG':
            Etale._pair_exchanges = {info['pair'] : sorted(info['exchanges']) for info in msg['pairs']}
        elif msg_type == 'CONSOLIDATED_MARKET_DATA':
            await self._process_book(msg)
        elif msg_type == 'TRADE_DATA':
            await self._process_trade(msg)
        elif msg_type == 'CANDLESTICK_MARKET_DATA':
            await self._process_candlestick(msg)
        elif msg_type == 'ACCOUNT_UPDATE_MESSAGE':
            await self._process_account_update(msg)
        else:
            LOG.warning(f'{ self.id } unhandled message type { msg_type }  : { msg }')

    async def _process_book(self, msg):
        '''
        {
            'type': 'CONSOLIDATED_MARKET_DATA',
            'subscribeId': 'cmd1',
            'mcp': 0.035675,
            'bids': [
                {'fee': 0.0042305161274415, 'exchange': 'CoinbasePro', 'price': 0.03567, 'qty': 39.53383915},
                {'fee': 6.42233034e-05, 'exchange': 'CoinbasePro', 'price': 0.03566, 'qty': 0.60033},
                ...,],
            'asks': [
                {'fee': 0.0008992817017776002, 'exchange': 'CoinbasePro', 'price': 0.03568, 'qty': 8.40136119},
                {'fee': 0.00075516471, 'exchange': 'CoinbasePro', 'price': 0.03569, 'qty': 7.053},
                ...,],
        }'''
        pair, exchs = self.consolidateds[int(msg['subscribeId'][3:])]
        self.l2_book[pair] = { exch: { BID : sd(), ASK : sd() } for exch in exchs }
        for level in msg['bids']:
            self.l2_book[pair][level['exchange']][BID][level['price']] = (level['qty'], level['fee'])
        for level in msg['asks']:
            self.l2_book[pair][level['exchange']][ASK][level['price']] = (level['qty'], level['fee'])
        #     BID : [(level['price'], level['qty'], level['fee'], level['exchange']) for level in msg['bids']],
        #     ASK : [(level['price'], level['qty'], level['fee'], level['exchange']) for level in msg['asks']]}
        for exch in exchs:
            await self.callbacks[L2_BOOK](feed=self.id, pair=pair, exchange=exch, book=self.l2_book[pair][exch], index=msg['recv'], mcp=msg['mcp'])

    async def _process_trade(self, msg):
        '''
        {
            "type":"TRADE_DATA"
            "subscribeId":"cmd1",
            "exchange":"Gdax",
            "symbol":"BTC-USD",
            "side":"BID",
            "price":6483.75,
            "qty":0.1016,
            "timestamp":1536946514850,
        }'''
        await self.callbacks[TRADES](
            feed = self.id,
            pair = msg['symbol'],
            side = BUY if msg['side'] == ASK else SELL,
            amount = msg['qty'],
            price = msg['price'],
            order_id = None,
            index = msg['recv'],
            timestamp = msg['timestamp']/1000.0,
            exchange = msg['exchange'],
        )


    async def _process_candlestick(self, msg):
        '''
        {
            "type":"CANDLESTICK_MARKET_DATA", "Minutes":"1", "subscribeId":"bar12", 
            "bidsOHLC":{
                "Open":6394.99,
                "High":6394.99,
                "Low":6394.99,
                "Close":6394.99,
                "openTimestamp":1540565259155,
                "closeTimestamp":1540565319010,
                "empty":false
            },
            "asksOHLC":{
                "Open":6395.0,
                "High":6395.0,
                "Low":6395.0,
                "Close":6395.0,
                "openTimestamp":1540565259155,
                "closeTimestamp":1540565319010,
                "Empty":false
            },
            "tradesOHLC":{
                "Open":6395.0,
                "High":6395.0,
                "Low":6394.99,
                "Close":6395.0,
                "openTimestamp":1540565259655,
                "closeTimestamp":1540565318640,
                "Empty":false
            },
            "tradeVolumes":{
                "numeratorVolume":8.570482639999998,
                "denominatorVolume":54808.223822029126,
                "tradeCount":36,
                "Empty":false
            } 
        }'''

        (pair, exch) = self.candlesticks[int(msg['subscribeId'][3:])]
        recv = msg['recv']
        # bids = msg['bidsOHLC']
        # if bids['empty']:
        #     bids = {'index':recv, 'empty':1}
        # else:
        #     bids['index'] = recv
        #     bids['empty'] = int(bids['empty'])
        # self.bids_data.append(bids)
        # asks = msg['asksOHLC']
        # if asks['empty']:
        #     asks = {'index':recv, 'empty':1}
        # else:
        #     asks['index'] = recv
        #     asks['empty'] = int(asks['empty'])
        # self.asks_data.append(asks)
        trades = msg['tradesOHLC']
        if trades['empty']:
            trades = {'index':recv, 'empty':1}
        else:
            trades['index'] = recv
            trades['empty'] = int(trades['empty'])
            trades['numeratorVolume'] = msg['tradeVolumes']['numeratorVolume']
            trades['denominatorVolume'] = msg['tradeVolumes']['denominatorVolume']
            trades['tradeCount'] = msg['tradeVolumes']['tradeCount']

        await self.callbacks[CANDLES](
            feed=self.id,
            exchange=exch,
            pair=pair,
            **trades)

    def interrupt(self):
        raise NotImplementedError()

    async def get_balances(self):
        msg = {"type":"BALANCE_REQUEST"}
        evt = self.events.get('balance_request', None)
        if evt is None:
            self.events['balance_request'] = evt = asyncio.Event()
        else:
            evt.clear()
        await self._ws.send(json.dumps(msg))
        await evt.wait()
        return self.balances

    def _read_balances(self, msg):
        '''
        {
            'type': 'BALANCE_INFO', 'recv': datetime.datetime(2019, 2, 28, 15, 6, 59, 502207, tzinfo=<UTC>)
            'balances': {
                'Hitbtc': [
                    {'currency': {'name': 'BTC'}, 'amount': 50.0},
                    {'currency': {'name': 'USDT'}, 'amount': 100000.0} ],
                'Binance': [
                    {'currency': {'name': 'BTC'}, 'amount': 50.0},
                    {'currency': {'name': 'USDT'}, 'amount': 100000.0} ],
                'Blockchain': [],
                ...},
        }'''
        all_exchanges = 0
        new_exchanges = 0
        all_amounts = 0
        new_amounts = 0
        chg_amounts = 0
        data = msg['balances']
        for exch, info in data.items():
            exch_balances = self.balances.get(exch)
            all_exchanges += 1
            if exch_balances is None:
                self.balances[exch] = dict((item['currency']['name'], item['amount'])
                                           for item in info)
                new_exchanges += 1
            else:
                for item in info:
                    cur = item['currency']['name']
                    amt = exch_balances.get(cur)
                    if amt is None:
                        LOG.info(f"Got balance for {cur}: {amt}")
                        new_amounts += 1
                    elif amt != item['amount']:
                        LOG.info(f"Got new balance for {cur}: {amt} -> {item['amount']}")
                        chg_amounts += 1
                    exch_balances[cur] = item['amount']
                    all_amounts += 1
        self.balances['last_update'] = time()
        self.balances['last_update_stats'] = {
            'all_exchanges':all_exchanges,
            'new_exchanges':new_exchanges,
            'all_amounts':all_amounts,
            'new_amounts':new_amounts,
            'chg_amounts':chg_amounts,
        }
        LOG.info(f'{self.id } { msg }')
        LOG.info(f'read balances for {all_exchanges} ({new_exchanges} new); {all_amounts} ccys ({new_amounts} new, {chg_amounts} chg)')
        evt = self.events.get('balance_request', None)
        if evt is not None:
            evt.set()

    async def _process_balances(self, msg):
        self._read_balances(msg)
        # insert async callbacks for reading balances

    async def _process_account_update(self, msg):
        '''
        {
            'type': 'ACCOUNT_UPDATE_MESSAGE',
            'recv': datetime.datetime(2019, 2, 28, 15, 10, 59, 513023, tzinfo=<UTC>),
            'updates': [
                {
                    'pathToAccount': ['ROOT', 'DEFAULT_ACC'],
                    'currency': 'BTC',
                    'qty': 200.0,
                    'realizedPnl': 0.0,
                    'unrealizedPnl': -35286.39999999996,
                    'dollarValue': 764713.6000000001
                },
                {
                    'pathToAccount': ['ROOT', 'DEFAULT_ACC'],
                    'currency': 'USD',
                    'qty': 200000.0,
                    'realizedPnl': 0.0,
                    'unrealizedPnl': 0.0,
                    'dollarValue': 200000.0
                },
                {
                    'pathToAccount': ['ROOT', 'DEFAULT_ACC'],
                    'currency': 'USDT', 'qty': 200000.0, 'realizedPnl': 0.0, 'unrealizedPnl': 0.0, 'dollarValue': 200000.0}
            ],
        }'''
        LOG.info(msg)
        pass


    async def send_order(self, side, pair, limitPrice, quantity, exchanges,
                         useFeeAdjustment="false", routingAlgo="DMA", timeInForce="GTC",
                         stableCoins=[ {"stableCoin":"TUSD","discountFactor":0.99},
                                       {"stableCoin":"USDC","discountFactor":0.99},
                                       {"stableCoin":"USDT","discountFactor":0.99} ]):
        '''
        {
            "type":"ORDER",
            "clOrdId":<unique client order id>, "side":"SELL",
            "pair":"ETH-USD",
            "limitPrice":407,
            "quantity":1, "exchanges":["Gemini"], "useFeeAdjustment": false, "routingAlgo":"CERA",
            "timeInForce": "GTC",
            "stableCoins": [
                {"stableCoin":"TUSD","discountFactor":1},
                {"stableCoin":"USDT","discountFactor":1} ]
        }'''
        clOrdId = -1
        msg = json.dumps({
            "type" : "ORDER",
            "clOrdId": clOrdId,
            "side" : side,
            "pair" : pair,
            "limitPrice" : limitPrice,
            "quantity" : quantity,
            "exchanges" : exchanges,
            "useFeeAdjustment" : useFeeAdjustment,
            "routingAlgo" : routingAlgo,
            "timeInForce" : timeInForce,
            "stableCoins" : stableCoins
        })
        log.INFO(f'{ self.id } sending { msg }')
        raise NotImplementedError()
        await self._ws.send(msg)

    async def send_wap_order(self, side, pair, limitPrice, quantity, exchanges, startTime, endTime,
                             participationRate, useFeeAdjustment="false", routingAlgo="TWAP"):
        '''
        {
            "type":"WAP_ORDER",
            "clOrdId":<unique client order id>,
            "side":"SELL",
            "pair":"ETH-USD",
            "limitPrice":407,
            "quantity":1,
            "exchanges":["Gemini", "Gdax"],
            "useFeeAdjustment": false,
            "routingAlgo":"TWAP"
            "startTime":"2018-08-03 17:01 GMT+04:00",
            "endTime":"2018-08-03 17:19 GMT+04:00",
            "participationRate": 0.4
        }'''
        raise NotImplementedError()
        await self._ws.send(msg)

# For the PEG and LUCY order types the order message has the following format:
# {
# "type":"PEG_ORDER", "routingAlgo":"PEG",
# "clOrdId":<unique client order id>, "side":"SELL",
# "pair":"ETH-USD",
# "limitPrice":407,
# "quantity":1,
# "exchanges":["Gemini", "CoinbasePro"], "orderPlacementExchange":"CoinbasePro", "referencePriceType":"PRIMARY", "priceOffset":0.01,
#  "tolerance":0.05, "useFeeAdjustment": false }
# The fields definitions are as follows:
#     clOrdId
#   Unique ID assigned by the user to identify this order.
#      side
#    BUY or SELL
#      routingAlgo
#    DMA, TWAP, SOR, CERA, PEG, LUCY, or PARTICIPATION
#  pair
#     The symbol being traded.
#      limitPrice
#  The limit price for the order. Only valid for DMA, SOR, CERA, and Participation Rate orders.
#      quantity
#    The quantity to trade
#  timeInForce
#     GTC or IOC. Only used by DMA orders.
#    exchanges
#   The list of exchanges to which this order may be routed. Note: for DMA orders this must be a single exchange. Also, for Portfolio TWAP orders, this must be a single exchange.
#     stableCoins
#  Used for CERA orders. This is a map of stable coin pairs with associated discount factors. The format of the map is:
# [
# {"stableCoin":"TUSD","discountFactor":1} , {"stableCoin":"USDT","discountFactor":1}
# ]
#      useFeeAdjustment
#    "True" if fees should be considered when making routing decisions, false otherwise. Ignored for DMA orders.
#      startTime
#    The start time of the order. Only valid for TWAP and Participation Rate orders.
#      endTime
#    The end time of the order. Only valid for TWAP and Participation Rate orders.
#      participationRate
#    The fraction of the current volume that the algorithm should maintain. Only valid for Participation Rate orders.
#  orderPlacementExchange
#    The exchange at which to place the peg order. Only valid for PEG orders.
 
#      referencePriceType
#   The reference price to peg to.One of PRIMARY (the favorable side of the market), MARKET(the aggressive, or marketable side of the market) or MCP(the market clearing price). Only valid for PEG orders.
#      priceOffset
#    The offset from the reference price to peg the order to. Only valid for PEG orders. Must be 0 or negative for PRIMARY and MCP peg orders. Only valid for PEG orders.
#      tolerance
#    The price drift threshold above which a peg order is cancelled and replaced by a new peg order at an updated price. Only valid for PEG orders.
#  agression
#     Aggressiveness setting for the order. Used for LUCY orders.
#    passiveDurationSecs
#  How long the order should hunt for passive liquidity before turning aggressive. Used for LUCY orders.
#  After an order is placed, Etale will respond with an order acknowledgment message:
# {
# "type":"ORDER_ACK",
# "clOrdId": <unique client order id>, "status": <"accepted", "rejected">, "reason": "order status info"
# }
# Order fills are reported with fill messages as child orders complete. The fill message format is as follows:
# {
# "type":"ORDER_EXECUTION_REPORT", "clOrdId: "...", "originatingOrderClOrdId" : "...", "orderId: "...",
# "symbol": "ETH-USD",
# "side": <"BUY", "SELL"> "orderQty": 10,
# "filledQty": 1,
# "unfilledQty": 9,
# "price": 6700,
# "status": <"active", "done"> "lastQty": 1,
# "lastPrice": 6500,
# "exchange": "Gdax",
#  "exchangeId": ".....", "execId": ".....", "reason": "....."
# }
# For WAP orders, a separate WAP order update will also be published along with each execution report. It will contain a summary of the status of the order.
# {
# "id":"1539805694642", "clOrdId":"clOrdIdx-1070", "wapType":"TWAP", "percentComplete":20, "startMs":1539806280000, "endMs":1539806580000, "reserveCurrency":"", "status":"ACTIVE", "description":"...", "type":"WAP_UPDATE"
# }
# For PEG and LUCY orders, a separate order update will also be published along with each execution report. It will contain a summary of the status of the order.
# {
# "id":"1550838031822", "clOrdId":"1-ID0009939", "pegType":"PEG", "receivedTimeMs":1550862123215, "originalQty":5,
# "filledQty":0, "status":"ACTIVE", "numOrdersPlaced":1, "description":"...", "type":"PEG_UPDATE"
# }
# To cancel an active order send a cancel message>
# {
# "type": "ORDER_CXL", "clOrdId":"...."
# }
# In response to a cancel request, a cancel response message will be sent:
# {
# "type":"ORDER_EXECUTION_REPORT", "clOrdId":<unique client order id>, "status": "done",
# "reason": "User cancelled"
# }


# {
# "type":"ACTIVE_ORDER_REQUEST"
# }
# The response to this message is :
# { "type":"ACTIVE_ORDERS", "orders": [
# { "type":"ORDER_STATUS", "clOrdId: "...", "orderId: "...", "symbol": "ETH-USD",
# "side": <"BUY", "SELL"> "orderQty": 10,
# "filledQty": 1, "unfilledQty": 9,
# "price": 6700,
# "status": <"active", "done">
# },... ]
# }

# {
# "type": " BCDM_SUBSCRIBE ", "subscribeId": "BCDM-1"
# }
# To unsubscribe send the following message:
# {
# "type": " BCDM_UNSUBSCRIBE ", "subscribeId": "BCDM-1"
# }
# Updates messages are in the following format:
# {
# "subscribeId":"BCDM-1", "transfers":[
# {
# "index":1537219982010, "date":"17/09/18", "timestamp":"17:52:11", "qty":19.95767434586466, "coin":"ZRX", "fromAddress":"0x384896375...", "toAddress":"0x4cd49ef1...", "tx":"NA"
# }, {
# } ],
# "index":1537219982011, "date":"17/09/18", "timestamp":"17:52:12", "qty":1.48574446,
# "coin":"LTC", "fromAddress":"504ea591e3477c...", "toAddress":"LN8DXdQWvT3GL...", "tx":"504ea591e34..."
# "type":"BCDM_UPDATE" }

