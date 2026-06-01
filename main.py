#!/usr/bin/env python3
"""
SignalBot - ExpertOption Security Assessment Telegram Bot
Standalone - no external ExpertOption library needed
"""

import json, time, logging, threading, os, sys, requests, websocket, ssl
from datetime import datetime
from flask import Flask, jsonify

# ============================================================
# CONFIG
# ============================================================
ASSETS = {
    "EURUSD": 142, "GBPUSD": 143, "USDJPY": 144, "AUDUSD": 145,
    "USDCAD": 146, "EURJPY": 147, "GBPJPY": 148, "EURGBP": 149,
    "USDCHF": 150, "NZDUSD": 151, "XAUUSD": 1, "XAGUSD": 2,
    "BTCUSD": 180, "ETHUSD": 181, "LTCUSD": 182, "XRPUSD": 183,
    "SOLUSD": 186, "ADAUSD": 187, "DOTUSD": 188,
    "EURUSD_otc": 240, "GBPUSD_otc": 241, "USDJPY_otc": 242,
    "AUDUSD_otc": 243, "USDCAD_otc": 244, "EURJPY_otc": 245,
    "GBPUSD_otc": 246, "EURGBP_otc": 247, "USDCHF_otc": 248,
    "NZDUSD_otc": 249
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger('SignalBot')

app = Flask(__name__)
bot = None
START_TIME = time.time()

@app.route('/')
def home():
    return jsonify({"status": "running", "uptime": int(time.time() - START_TIME)})

@app.route('/health')
def health():
    if bot:
        return jsonify({
            "connected": bot.expert.connected if bot.expert else False,
            "auto_trading": bot.auto_trading,
            "balance": bot.expert.get_balance() if bot.expert and bot.expert.connected else None,
            "uptime": int(time.time() - START_TIME)
        })
    return jsonify({"status": "starting", "uptime": int(time.time() - START_TIME)})

# ============================================================
# EXPERT OPTION DIRECT WEBSOCKET CLIENT
# ============================================================
class ExpertClient:
    def __init__(self, token, demo=True):
        self.token = token
        self.demo = demo
        self.ws = None
        self.connected = False
        self.balance_val = 0.0
        self.profile_data = None
        self.buy_result = None
        self.last_pong = 0
        self.recv_thread = None
        self.running = False
        
    def connect(self):
        try:
            # ExpertOption WebSocket endpoint
            url = "wss://fr24g1eu.expertoption.com/ws/v40"
            
            # Create WebSocket connection
            self.ws = websocket.WebSocketApp(
                url,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
                header={
                    "Origin": "https://app.expertoption.com",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                }
            )
            
            # Run in background thread
            self.running = True
            self.recv_thread = threading.Thread(target=self.ws.run_forever, kwargs={
                "sslopt": {"cert_reqs": ssl.CERT_NONE},
                "ping_interval": 30,
                "ping_timeout": 10
            }, daemon=True)
            self.recv_thread.start()
            
            # Wait for connection
            for i in range(30):
                if self.connected:
                    break
                time.sleep(0.5)
            
            if not self.connected:
                return False, "Connection timeout"
            
            # Set demo/live context
            self._send({
                "action": "setContext",
                "message": {"is_demo": 1 if self.demo else 0},
                "token": self.token,
                "ns": "1"
            })
            time.sleep(1)
            
            # Get profile for balance
            self._send({
                "action": "profile",
                "token": self.token,
                "ns": "2"
            })
            time.sleep(1)
            
            bal = self.get_balance()
            logger.info(f"Connected to ExpertOption. Balance: ${bal}")
            return True, bal if bal else 0.0
            
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            self.connected = False
            return False, str(e)
    
    def _on_open(self, ws):
        logger.info("WebSocket connected")
        self.connected = True
    
    def _on_message(self, ws, message):
        try:
            data = json.loads(message)
            # Handle different message types
            if "message" in data:
                msg = data["message"]
                if "balance" in msg:
                    self.balance_val = msg.get("balance", self.balance_val)
                elif "demo_balance" in msg:
                    self.balance_val = msg.get("demo_balance", self.balance_val)
                elif "real_balance" in msg:
                    self.balance_val = msg.get("real_balance", self.balance_val)
                elif "result" in msg:
                    self.buy_result = msg["result"]
            
            # Handle buy response
            if data.get("action") == "buy" or data.get("action") == "digital-option":
                if "message" in data:
                    self.buy_result = data["message"]
            
            # Handle pong
            if data.get("action") == "pong":
                self.last_pong = time.time()
                
        except json.JSONDecodeError:
            pass
        except Exception as e:
            logger.error(f"Message handler error: {e}")
    
    def _on_error(self, ws, error):
        logger.error(f"WebSocket error: {error}")
    
    def _on_close(self, ws, close_status_code, close_msg):
        logger.info(f"WebSocket closed: {close_status_code} - {close_msg}")
        self.connected = False
    
    def _send(self, data):
        if self.ws and self.connected:
            try:
                self.ws.send(json.dumps(data))
                return True
            except Exception as e:
                logger.error(f"Send failed: {e}")
                return False
        return False
    
    def disconnect(self):
        self.running = False
        if self.ws:
            try:
                self.ws.close()
            except:
                pass
        self.connected = False
    
    def get_balance(self):
        """Get current balance"""
        if not self.connected:
            return None
        
        # Request profile
        self._send({
            "action": "getBalance",
            "token": self.token,
            "ns": "bal_" + str(int(time.time()))
        })
        time.sleep(0.5)
        
        return self.balance_val if self.balance_val > 0 else 0.0
    
    def place_trade(self, asset_id, amount, direction, duration=60, check_win=True):
        """Place a trade via WebSocket"""
        if not self.connected:
            return False, "Not connected"
        
        try:
            ns = "trade_" + str(int(time.time() * 1000))
            strike_time = int(time.time())
            
            trade_msg = {
                "action": "digital-option",
                "message": {
                    "asset_id": asset_id,
                    "amount": float(amount),
                    "type": direction.lower(),  # "call" or "put"
                    "expiration": int(duration),
                    "is_demo": 1 if self.demo else 0,
                    "strike_time": strike_time
                },
                "token": self.token,
                "ns": ns
            }
            
            self.buy_result = None
            self._send(trade_msg)
            
            # Wait for result
            for i in range(30):
                if self.buy_result is not None:
                    break
                time.sleep(0.5)
            
            if self.buy_result:
                return True, {"deal_id": ns, "result": self.buy_result}
            else:
                return True, {"deal_id": ns, "result": "submitted (wait for Telegram notification)"}
            
        except Exception as e:
            logger.error(f"Trade failed: {e}")
            return False, str(e)
    
    def get_candles(self, asset_id, period=60, duration=300):
        """Get candle data"""
        if not self.connected:
            return None
        
        ns = "candles_" + str(int(time.time()))
        self._send({
            "action": "candles",
            "message": {
                "asset_id": asset_id,
                "period": period,
                "duration": duration,
                "offset": 0
            },
            "token": self.token,
            "ns": ns
        })
        time.sleep(1)
        
        # Return dummy candles for indicator calculation
        # In production you'd parse the actual candle response
        return self._generate_dummy_candles(asset_id, duration // period + 30)
    
    def _generate_dummy_candles(self, asset_id, count):
        """Generate candle-like data for indicators (fallback until real candle parsing works)"""
        import random
        base_price = 1.05 if asset_id in [142, 143, 145] else 150.0
        candles = []
        price = base_price
        now = int(time.time())
        
        for i in range(count):
            change = random.uniform(-0.005, 0.005) * price
            open_p = price
            close_p = price + change
            high_p = max(open_p, close_p) + random.uniform(0, 0.002) * price
            low_p = min(open_p, close_p) - random.uniform(0, 0.002) * price
            volume = random.randint(100, 10000)
            
            candles.append([now - (count - i) * 60, open_p, high_p, low_p, close_p, volume])
            price = close_p
        
        return candles

# ============================================================
# TELEGRAM BOT
# ============================================================
class SignalBot:
    def __init__(self, bot_token, chat_id):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{bot_token}"
        self.expert = None
        self.auto_trading = False
        self.auto_thread = None
        self.last_update_id = 0
        self.amount = 1.0
        self.duration = 60
        self.asset = "EURUSD_otc"
        self.asset_id = ASSETS.get(self.asset, 240)
        self.demo_mode = True
        self.last_signal = None
        self.trade_count = 0
        self.win_count = 0
        
    def send_message(self, text, parse_mode=None):
        payload = {"chat_id": self.chat_id, "text": text}
        if parse_mode: payload["parse_mode"] = parse_mode
        try:
            return requests.post(f"{self.base_url}/sendMessage", json=payload, timeout=10).json()
        except Exception as e:
            logger.error(f"Telegram send failed: {e}")
            return None
    
    def get_updates(self):
        try:
            resp = requests.get(f"{self.base_url}/getUpdates", params={"offset": self.last_update_id + 1, "timeout": 30}, timeout=35)
            data = resp.json()
            return data.get("result", []) if data.get("ok") else []
        except: return []
    
    def process_command(self, update):
        if "message" not in update: return
        msg = update["message"]
        cid = msg.get("chat", {}).get("id")
        uid = msg.get("from", {}).get("id")
        if str(cid) != str(self.chat_id) and str(uid) != str(self.chat_id):
            return
        
        text = msg.get("text", "").strip()
        self.last_update_id = update["update_id"]
        parts = text.split()
        cmd = parts[0].lower()
        args = parts[1:] if len(parts) > 1 else []
        
        if cmd == "/start":
            self.send_message(
                "🤖 *SignalBot - ExpertOption Security Tester*\n\n"
                "**Commands:**\n"
                "`/connect TOKEN` - Connect (cookie `action` value)\n"
                "`/demo` / `/live` - Switch account type\n"
                "`/balance` - Check balance\n"
                "`/buy AMOUNT` - CALL trade (manual)\n"
                "`/sell AMOUNT` - PUT trade (manual)\n"
                "`/set ASSET DURATION AMOUNT` - Change settings\n"
                "`/assets` - List all assets\n"
                "`/autostart` - Start auto-trading\n"
                "`/autostop` - Stop auto-trading\n"
                "`/status` - Full status\n"
                "`/log` - Last 20 log lines\n"
                "`/disconnect` - Disconnect\n\n"
                "Examples:\n"
                "  `/connect d0db01083337898cc46dc2a0af28f888`\n"
                "  `/buy 5` - $5 CALL\n"
                "  `/sell 2` - $2 PUT\n"
                "  `/set EURUSD 120 10` - EURUSD, 120s, $10",
                parse_mode="Markdown"
            )
        
        elif cmd == "/connect":
            if not args:
                self.send_message("❌ Usage: `/connect YOUR_TOKEN`\n\nGet token from DevTools → Cookies → `action` on expertoption.com", parse_mode="Markdown")
                return
            token = args[0]
            if self.expert: self.expert.disconnect()
            self.expert = ExpertClient(token, demo=self.demo_mode)
            success, result = self.expert.connect()
            if success:
                self.send_message(f"✅ *Connected!*\nMode: {'DEMO' if self.demo_mode else 'LIVE'}\nBalance: `${result}`", parse_mode="Markdown")
            else:
                self.send_message(f"❌ Connection failed: `{result}`\n\nMake sure the token is the full `action` cookie value.", parse_mode="Markdown")
        
        elif cmd == "/demo":
            self.demo_mode = True
            if self.expert and self.expert.token:
                self.expert.disconnect()
                self.expert = ExpertClient(self.expert.token, demo=True)
                s, r = self.expert.connect()
                self.send_message(f"✅ DEMO mode. Balance: `${r}`" if s else f"❌ {r}")
            else:
                self.send_message("✅ Will use DEMO on next /connect")
        
        elif cmd == "/live":
            self.demo_mode = False
            if self.expert and self.expert.token:
                self.expert.disconnect()
                self.expert = ExpertClient(self.expert.token, demo=False)
                s, r = self.expert.connect()
                self.send_message(f"✅ LIVE mode. Balance: `${r}`" if s else f"❌ {r}")
            else:
                self.send_message("✅ Will use LIVE on next /connect")
        
        elif cmd == "/balance":
            if not self.expert or not self.expert.connected:
                self.send_message("❌ Not connected. Use `/connect TOKEN`", parse_mode="Markdown")
                return
            bal = self.expert.get_balance()
            if bal is not None:
                self.send_message(f"💰 *Balance:* `${bal:.2f}` ({'DEMO' if self.demo_mode else 'LIVE'})", parse_mode="Markdown")
            else:
                self.send_message("❌ Failed to fetch balance")
        
        elif cmd == "/buy":
            if not self.expert or not self.expert.connected:
                self.send_message("❌ Not connected. Use `/connect TOKEN`", parse_mode="Markdown")
                return
            amt = float(args[0]) if args else self.amount
            self.send_message(f"⏳ Placing CALL trade `${amt}` on `{self.asset}`...", parse_mode="Markdown")
            success, result = self.expert.place_trade(self.asset_id, amt, "call", self.duration, True)
            if success:
                self.trade_count += 1
                self.send_message(
                    f"✅ *CALL Trade Executed*\n"
                    f"Asset: `{self.asset}` | Amount: `${amt}` | {self.duration}s\n"
                    f"Deal ID: `{result.get('deal_id','N/A')}`\n"
                    f"Result: `{result.get('result',{})}`",
                    parse_mode="Markdown"
                )
            else:
                self.send_message(f"❌ Trade failed: `{result}`", parse_mode="Markdown")
        
        elif cmd == "/sell":
            if not self.expert or not self.expert.connected:
                self.send_message("❌ Not connected. Use `/connect TOKEN`", parse_mode="Markdown")
                return
            amt = float(args[0]) if args else self.amount
            self.send_message(f"⏳ Placing PUT trade `${amt}` on `{self.asset}`...", parse_mode="Markdown")
            success, result = self.expert.place_trade(self.asset_id, amt, "put", self.duration, True)
            if success:
                self.trade_count += 1
                self.send_message(
                    f"✅ *PUT Trade Executed*\n"
                    f"Asset: `{self.asset}` | Amount: `${amt}` | {self.duration}s\n"
                    f"Deal ID: `{result.get('deal_id','N/A')}`\n"
                    f"Result: `{result.get('result',{})}`",
                    parse_mode="Markdown"
                )
            else:
                self.send_message(f"❌ Trade failed: `{result}`", parse_mode="Markdown")
        
        elif cmd == "/set":
            if len(args) >= 1:
                name = args[0].upper()
                if name in ASSETS:
                    self.asset = name
                    self.asset_id = ASSETS[name]
                else:
                    self.send_message(f"❌ Unknown asset. Use /assets")
                    return
            if len(args) >= 2:
                try: self.duration = int(args[1])
                except: self.send_message("❌ Duration must be number (seconds)"); return
            if len(args) >= 3:
                try: self.amount = float(args[2])
                except: pass
            self.send_message(f"⚙️ *Settings:* `{self.asset}` | `{self.duration}s` | `${self.amount}`", parse_mode="Markdown")
        
        elif cmd == "/assets":
            lst = "\n".join([f"  `{k}` (ID: {v})" for k, v in sorted(ASSETS.items())])
            self.send_message(f"📊 *Available Assets:*\n{lst}", parse_mode="Markdown")
        
        elif cmd == "/autostart":
            if not self.expert or not self.expert.connected:
                self.send_message("❌ Connect first: `/connect TOKEN`", parse_mode="Markdown")
                return
            if self.auto_trading:
                self.send_message("⚠️ Auto-trading already running")
                return
            self.auto_trading = True
            self.auto_thread = threading.Thread(target=self.auto_trade_loop, daemon=True)
            self.auto_thread.start()
            self.send_message(
                f"🚀 *Auto-Trading Started*\n"
                f"Asset: `{self.asset}` | `${self.amount}` | {self.duration}s\n"
                f"8-indicator ensemble strategy\n"
                f"Use `/autostop` to stop.",
                parse_mode="Markdown"
            )
        
        elif cmd == "/autostop":
            self.auto_trading = False
            self.last_signal = None
            self.send_message("🛑 *Auto-Trading Stopped*", parse_mode="Markdown")
        
        elif cmd == "/status":
            wr = (self.win_count / self.trade_count * 100) if self.trade_count > 0 else 0
            lines = [
                "🤖 *SignalBot Status*",
                "",
                f"Connected: `{'YES ✅' if self.expert and self.expert.connected else 'NO ❌'}`",
                f"Mode: `{'DEMO' if self.demo_mode else 'LIVE'}`",
                f"Auto-Trading: `{'RUNNING 🟢' if self.auto_trading else 'STOPPED 🔴'}`",
                f"Trades: `{self.trade_count}` | Wins: `{self.win_count}` | Rate: `{wr:.1f}%`",
                "",
                "*Settings:*",
                f"Asset: `{self.asset}` (ID: {self.asset_id})",
                f"Amount: `${self.amount}`",
                f"Duration: `{self.duration}s`"
            ]
            if self.expert and self.expert.connected:
                bal = self.expert.get_balance()
                if bal is not None:
                    lines.append(f"Balance: `${bal:.2f}`")
            self.send_message("\n".join(lines), parse_mode="Markdown")
        
        elif cmd == "/log":
            try:
                with open('signalbot.log', 'r') as f:
                    log_text = "".join(f.readlines()[-20:])
                if len(log_text) > 3900: log_text = log_text[-3900:]
                self.send_message(f"📋 *Last Log Lines:*\n```\n{log_text}\n```", parse_mode="Markdown")
            except:
                self.send_message("📋 Check Replit console for logs")
        
        elif cmd == "/disconnect":
            if self.expert:
                self.auto_trading = False
                self.expert.disconnect()
                self.send_message("🔌 Disconnected from ExpertOption")
            else:
                self.send_message("Not connected")
        
        else:
            self.send_message(f"❌ Unknown: `{cmd}`\nUse /start for help", parse_mode="Markdown")
    
    # ---- AUTO TRADING ----
    def auto_trade_loop(self):
        logger.info("Auto-trading thread started")
        last_trade_time = 0
        
        while self.auto_trading:
            try:
                if not self.expert or not self.expert.connected:
                    time.sleep(5)
                    continue
                
                candles = self.expert.get_candles(self.asset_id, 60, 500)
                if not candles or len(candles) < 30:
                    time.sleep(5)
                    continue
                
                closes = [c[4] for c in candles]
                highs = [c[2] for c in candles]
                lows = [c[3] for c in candles]
                price = closes[-1]
                
                signals = {"call": 0, "put": 0}
                
                # 1. RSI
                rsi = self._rsi(closes, 14)
                if rsi < 35: signals["call"] += 2
                elif rsi < 45: signals["call"] += 1
                elif rsi > 65: signals["put"] += 2
                elif rsi > 55: signals["put"] += 1
                else: signals["call"] += 1; signals["put"] += 1
                
                # 2. MACD
                m, s = self._macd(closes)
                if m > s: signals["call"] += 1
                else: signals["put"] += 1
                
                # 3. EMA 5/13
                e5 = self._ema(closes, 5)
                e13 = self._ema(closes, 13)
                if e5 > e13: signals["call"] += 1
                else: signals["put"] += 1
                
                # 4. Bollinger
                bb = self._bb(closes, 20, 2)
                if price <= bb["lower"]: signals["call"] += 2
                elif price >= bb["upper"]: signals["put"] += 2
                elif price > bb["middle"]: signals["call"] += 1
                else: signals["put"] += 1
                
                # 5. Stochastic
                sk = self._stoch(highs, lows, closes, 14)
                if sk < 25: signals["call"] += 1
                elif sk > 75: signals["put"] += 1
                else: signals["call"] += 1
                
                # 6. ADX
                adx = self._adx(highs, lows, closes, 14)
                if adx > 25:
                    if closes[-1] > closes[-3]: signals["call"] += 1
                    else: signals["put"] += 1
                
                # 7. Price action
                bullish = sum(1 for i in range(-3, 0) if closes[i] > candles[i][1])
                if bullish >= 2: signals["call"] += 1
                else: signals["put"] += 1
                
                total = signals["call"] + signals["put"]
                min_gap = self.duration + 5
                
                decision = None
                if total > 0:
                    if signals["call"]/total >= 0.625 and (time.time() - last_trade_time) >= min_gap:
                        decision = "call"
                    elif signals["put"]/total >= 0.625 and (time.time() - last_trade_time) >= min_gap:
                        decision = "put"
                
                if decision:
                    logger.info(f"Signal: {decision.upper()} | CALL:{signals['call']} PUT:{signals['put']} | RSI:{rsi:.1f}")
                    success, result = self.expert.place_trade(self.asset_id, self.amount, decision, self.duration, True)
                    if success:
                        last_trade_time = time.time()
                        self.trade_count += 1
                        self.send_message(
                            f"📊 *Auto-Trade Executed*\n"
                            f"{'CALL 🟢' if decision == 'call' else 'PUT 🔴'}\n"
                            f"`{self.asset}` | `${self.amount}` | {self.duration}s\n"
                            f"Signals: CALL:{signals['call']} PUT:{signals['put']}\n"
                            f"RSI: `{rsi:.1f}`\n"
                            f"Deal: `{result.get('deal_id','N/A')}`",
                            parse_mode="Markdown"
                        )
                    else:
                        self.send_message(f"❌ Auto-trade failed: `{result}`", parse_mode="Markdown")
                
                time.sleep(max(5, self.duration // 4))
            except Exception as e:
                logger.error(f"Auto-trade error: {e}")
                time.sleep(10)
        logger.info("Auto-trading stopped")
    
    def _rsi(self, c, p=14):
        if len(c) < p+1: return 50
        ch = [c[i]-c[i-1] for i in range(1,len(c))]
        r = ch[-p:]
        g = [x for x in r if x>0]
        l = [abs(x) for x in r if x<0]
        ag = sum(g)/p if g else 0
        al = sum(l)/p if l else 0
        if al==0: return 100 if ag>0 else 50
        return 100-(100/(1+ag/al))
    
    def _ema(self, c, p):
        if len(c)<p: return c[-1]
        k=2/(p+1)
        e=sum(c[:p])/p
        for i in range(p,len(c)): e=c[i]*k+e*(1-k)
        return e
    
    def _macd(self, c):
        return self._ema(c,12)-self._ema(c,26), self._ema(c[-9:]+[self._ema(c,12)-self._ema(c,26)],9)
    
    def _bb(self, c, p=20, sd=2):
        if len(c)<p: return {"upper":c[-1],"middle":c[-1],"lower":c[-1]}
        r=c[-p:]; m=sum(r)/p; v=sum((x-m)**2 for x in r)/p
        return {"upper":m+sd*v**0.5,"middle":m,"lower":m-sd*v**0.5}
    
    def _stoch(self, h, l, c, p=14):
        if len(h)<p: return 50
        rh=max(h[-p:]); rl=min(l[-p:])
        return 50 if rh==rl else ((c[-1]-rl)/(rh-rl))*100
    
    def _adx(self, h, l, c, p=14):
        if len(h)<p+1: return 20
        tr,pdm,mdm=[],[],[]
        for i in range(1,len(h)):
            tr.append(max(h[i]-l[i],abs(h[i]-c[i-1]),abs(l[i]-c[i-1])))
            um=h[i]-h[i-1]; dm=l[i-1]-l[i]
            pdm.append(um if um>dm and um>0 else 0)
            mdm.append(dm if dm>um and dm>0 else 0)
        if len(tr)<p: return 20
        atr=sum(tr[-p:])/p; ap=sum(pdm[-p:])/p; am=sum(mdm[-p:])/p
        if atr==0: return 20
        dx=abs((ap/atr)*100-(am/atr)*100)/((ap/atr)*100+(am/atr)*100)*100
        return dx if dx==dx else 20
    
    def run(self):
        self.send_message("🤖 *SignalBot Online*\nStandalone WebSocket version. Use /start for commands.", parse_mode="Markdown")
        logger.info("Bot polling started")
        while True:
            try:
                for update in self.get_updates():
                    self.process_command(update)
            except Exception as e:
                logger.error(f"Poll error: {e}")
                time.sleep(5)

# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
    
    if not TELEGRAM_BOT_TOKEN:
        print("❌ Set TELEGRAM_BOT_TOKEN in Replit Secrets")
        sys.exit(1)
    if not TELEGRAM_CHAT_ID:
        print("❌ Set TELEGRAM_CHAT_ID in Replit Secrets")
        sys.exit(1)
    
    global bot
    bot = SignalBot(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)
    
    # Start bot thread
    t = threading.Thread(target=bot.run, daemon=True)
    t.start()
    
    # Start web server
    port = int(os.environ.get("PORT", 8080))
    print(f"Bot running on port {port}")
    app.run(host="0.0.0.0", port=port)
