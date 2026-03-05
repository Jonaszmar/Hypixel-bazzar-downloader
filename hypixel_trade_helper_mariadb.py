import appdaemon.plugins.hass.hassapi as hass
import pymysql
import pandas as pd
from datetime import datetime, timedelta
import pytz


class HypixelTradeHelperMariaDB(hass.Hass):
    """
    Wersja pod bazę z mapowaniem item_id -> item_key

    Schemat:
      CREATE TABLE items (
        item_key SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,
        item_id  VARCHAR(100) NOT NULL,
        PRIMARY KEY (item_key),
        UNIQUE KEY uq_item_id (item_id)
      ) ENGINE=InnoDB;

      CREATE TABLE bazaar_price_history (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        item_key SMALLINT UNSIGNED NOT NULL,
        buy_price DECIMAL(10,1),
        sell_price DECIMAL(10,1),
        buy_volume INT UNSIGNED,
        sell_volume INT UNSIGNED,
        fetched_at DATETIME(3),
        INDEX idx_item_time (item_key, fetched_at),
        CONSTRAINT fk_item
          FOREIGN KEY (item_key) REFERENCES items(item_key)
      ) ENGINE=InnoDB;
    """

    def initialize(self):
        # --- DB config ---
        db = self.args.get("db", {})
        self.db_host = db.get("host", "127.0.0.1")
        self.db_port = int(db.get("port", 3306))
        self.db_user = db.get("user", "root")
        self.db_pass = db.get("password", "")
        self.db_name = db.get("database", "hypixel_db")

        # --- Tables ---
        self.table = self.args.get("table", "bazaar_price_history")
        self.items_table = self.args.get("items_table", "items")

        # --- Entities ---
        self.query_entity = self.args.get("query_entity", "input_text.hypixel_item_query")
        self.pick_entity = self.args.get("pick_entity", "input_select.hypixel_item_pick")

        # --- Params ---
        self.days = int(self.args.get("days", 7))
        self.min_points = int(self.args.get("min_points", 200))

        self.suggest_limit = int(self.args.get("suggest_limit", 20))
        self.min_query_len = int(self.args.get("min_query_len", 2))

        self.min_buy_volume = int(self.args.get("min_buy_volume", 0))
        self.min_sell_volume = int(self.args.get("min_sell_volume", 0))

        self.buy_pctl = float(self.args.get("buy_percentile", 0.15))
        self.sell_pctl = float(self.args.get("sell_percentile", 0.85))

        self.source_tz = pytz.timezone(self.args.get("source_tz", "UTC"))
        self.display_tz = pytz.timezone(self.args.get("display_tz", "Europe/Warsaw"))

        self.refresh_interval = int(self.args.get("refresh_interval", 60))

        # --- Cache: item_id <-> item_key ---
        # item_id -> item_key
        self.item_key_cache = {}
        # item_key -> item_id (opcjonalnie, jakbyś chciał)
        self.item_id_cache = {}

        # Listen
        self.listen_state(self.on_query_change, self.query_entity)
        self.listen_state(self.on_pick_change, self.pick_entity)

        self.run_every(self.scheduled_refresh, self.datetime() + timedelta(seconds=5), self.refresh_interval)

        self.log("HypixelTradeHelperMariaDB (item_key) ready.")

    # ---------------- DB helpers ----------------
    def db_conn(self):
        return pymysql.connect(
            host=self.db_host,
            port=self.db_port,
            user=self.db_user,
            password=self.db_pass,
            database=self.db_name,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,
        )

    # ---------------- Item mapping ----------------
    def get_item_key(self, item_id: str):
        """Zwraca item_key dla item_id (z cache, a jak brak to z DB)."""
        if item_id in self.item_key_cache:
            return self.item_key_cache[item_id]

        sql = f"SELECT item_key, item_id FROM {self.items_table} WHERE item_id = %s LIMIT 1"
        with self.db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (item_id,))
                row = cur.fetchone()

        if not row:
            return None

        k = int(row["item_key"])
        self.item_key_cache[row["item_id"]] = k
        self.item_id_cache[k] = row["item_id"]
        return k

    # ---------------- Autocomplete ----------------
    def on_query_change(self, entity, attribute, old, new, kwargs):
        q = (new or "").strip()
        if len(q) < self.min_query_len:
            self._set_pick_options(["-"])
            return

        like = self.escape_like(q)
        pattern = f"%{like}%"

        # Autocomplete z tabeli słownikowej - szybko i bez DISTINCT na historii
        sql = f"""
            SELECT item_id
            FROM {self.items_table}
            WHERE item_id LIKE %s ESCAPE '\\\\'
            ORDER BY item_id
            LIMIT %s
        """

        try:
            with self.db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (pattern, self.suggest_limit))
                    rows = cur.fetchall()

            items = [r["item_id"] for r in rows]
            if not items:
                items = ["(brak wyników)"]

            self._set_pick_options(items)

        except Exception as e:
            self.log(f"Autocomplete DB error: {e}", level="WARNING")

    def _set_pick_options(self, options):
        self.call_service(
            "input_select/set_options",
            entity_id=self.pick_entity,
            options=options
        )

    @staticmethod
    def escape_like(s: str) -> str:
        return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")

    # ---------------- Analysis trigger: selection ----------------
    def on_pick_change(self, entity, attribute, old, new, kwargs):
        item_id = (new or "").strip()
        if not item_id or item_id in ["-", "(brak wyników)"]:
            return
        self.recompute_item(item_id)

    # ---------------- Auto-refresh ----------------
    def scheduled_refresh(self, kwargs):
        item_id = (self.get_state(self.pick_entity) or "").strip()
        if not item_id or item_id in ["-", "(brak wyników)"]:
            return
        self.recompute_item(item_id)

    # ---------------- Core compute ----------------
    def recompute_item(self, item_id: str):
        try:
            item_key = self.get_item_key(item_id)
            if item_key is None:
                self.publish_error(item_id, f"Nie znaleziono itemu w tabeli {self.items_table}.")
                return

            df = self.load_history(item_key, self.days)
            if df is None or len(df) < self.min_points:
                self.publish_error(item_id, f"Za mało danych: {0 if df is None else len(df)} rekordów (min {self.min_points}).")
                return

            r = self.analyze(df)
            self.publish_result(item_id, r)

        except Exception as e:
            self.publish_error(item_id, f"Błąd analizy: {e}")

    def load_history(self, item_key: int, days: int) -> pd.DataFrame:
        # Uwaga: filtr po item_key + indeks (item_key, fetched_at) => szybko
        sql = f"""
            SELECT fetched_at, buy_price, sell_price, buy_volume, sell_volume
            FROM {self.table}
            WHERE item_key = %s
              AND fetched_at >= (NOW(3) - INTERVAL %s DAY)
            ORDER BY fetched_at ASC
        """

        with self.db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (item_key, days))
                rows = cur.fetchall()

        if not rows:
            return None

        df = pd.DataFrame(rows)

        # DECIMAL -> numeric
        df["buy_price"] = pd.to_numeric(df["buy_price"], errors="coerce")
        df["sell_price"] = pd.to_numeric(df["sell_price"], errors="coerce")
        df["buy_volume"] = pd.to_numeric(df["buy_volume"], errors="coerce").fillna(0).astype(int)
        df["sell_volume"] = pd.to_numeric(df["sell_volume"], errors="coerce").fillna(0).astype(int)

        df = df.dropna(subset=["buy_price", "sell_price"])
        if df.empty:
            return None

        df["dt"] = pd.to_datetime(df["fetched_at"])
        df["dt"] = df["dt"].apply(lambda x: self.source_tz.localize(x)).apply(lambda x: x.astimezone(self.display_tz))

        if self.min_buy_volume > 0:
            df = df[df["buy_volume"] >= self.min_buy_volume]
        if self.min_sell_volume > 0:
            df = df[df["sell_volume"] >= self.min_sell_volume]
        if df.empty:
            return None

        df["minute_of_day"] = df["dt"].dt.hour * 60 + (df["dt"].dt.minute // 10) * 10
        return df

    def analyze(self, df: pd.DataFrame) -> dict:
        buy_profile = df.groupby("minute_of_day")["buy_price"].median()
        sell_profile = df.groupby("minute_of_day")["sell_price"].median()

        best_buy_max_minute = int(buy_profile.idxmax())
        best_sell_min_minute = int(sell_profile.idxmin())

        predicted_buy_max = float(buy_profile.loc[best_buy_max_minute])
        predicted_sell_min = float(sell_profile.loc[best_sell_min_minute])

        current_buy = float(df["buy_price"].iloc[-1])
        current_sell = float(df["sell_price"].iloc[-1])

        now = datetime.now(self.display_tz)
        next_buy_max_dt = self.next_occurrence(now, best_buy_max_minute)
        next_sell_min_dt = self.next_occurrence(now, best_sell_min_minute)

        buy_hi = float(df["buy_price"].quantile(self.sell_pctl))
        sell_lo = float(df["sell_price"].quantile(1 - self.sell_pctl))

        buy_now = "BUY wysoko (dobrze sprzedawać)" if current_buy >= buy_hi else "BUY nie jest wysoko"
        sell_now = "SELL nisko (dobrze kupować)" if current_sell <= sell_lo else "SELL nie jest nisko"

        return {
            "best_buy_time": self.minute_to_hhmm(best_buy_max_minute),
            "best_sell_time": self.minute_to_hhmm(best_sell_min_minute),

            "predicted_buy_price": predicted_buy_max,
            "predicted_sell_price": predicted_sell_min,

            "next_best_buy_dt": next_buy_max_dt.isoformat(),
            "next_best_sell_dt": next_sell_min_dt.isoformat(),

            "current_buy": current_buy,
            "current_sell": current_sell,

            "buy_threshold": buy_hi,
            "sell_threshold": sell_lo,

            "buy_now": buy_now,
            "sell_now": sell_now,

            "points": int(len(df)),
            "covered_minutes": int(df["minute_of_day"].nunique()),
            "window_days": self.days,
        }

    def next_occurrence(self, now_dt, minute_of_day: int):
        h = minute_of_day // 60
        m = minute_of_day % 60
        candidate = now_dt.replace(hour=h, minute=m, second=0, microsecond=0)
        if candidate <= now_dt:
            candidate = candidate + timedelta(days=1)
        return candidate

    @staticmethod
    def minute_to_hhmm(minute_of_day: int) -> str:
        h = minute_of_day // 60
        m = minute_of_day % 60
        return f"{h:02d}:{m:02d}"

    # ---------------- Publish to HA ----------------
    def publish_error(self, item_id: str, msg: str):
        self.set_state(
            "sensor.hypixel_trade_advice",
            state="error",
            attributes={"item": item_id, "error": msg}
        )
        self.log(f"[{item_id}] {msg}", level="WARNING")

    def publish_result(self, item_id: str, r: dict):
        state = (
            f"KUP: {r['best_buy_time']} (~{r['predicted_buy_price']:.1f}) | "
            f"SPRZ: {r['best_sell_time']} (~{r['predicted_sell_price']:.1f})"
        )

        attrs = {
            "item": item_id,
            **r,

            "predicted_buy_price": round(r["predicted_buy_price"], 1),
            "predicted_sell_price": round(r["predicted_sell_price"], 1),
            "current_buy": round(r["current_buy"], 1),
            "current_sell": round(r["current_sell"], 1),
            "buy_threshold": round(r["buy_threshold"], 1),
            "sell_threshold": round(r["sell_threshold"], 1),

            "refresh_interval_s": self.refresh_interval,
            "source_tz": str(self.source_tz),
            "display_tz": str(self.display_tz),
        }

        self.set_state("sensor.hypixel_trade_advice", state=state, attributes=attrs)
        self.set_state("sensor.hypixel_buy_now_signal", state=r["buy_now"], attributes={"item": item_id})
        self.set_state("sensor.hypixel_sell_now_signal", state=r["sell_now"], attributes={"item": item_id})

        nb_buy = datetime.fromisoformat(r["next_best_buy_dt"]).astimezone(self.display_tz)
        nb_sell = datetime.fromisoformat(r["next_best_sell_dt"]).astimezone(self.display_tz)

        self.set_state(
            "sensor.hypixel_next_best_buy_pl",
            state=nb_buy.strftime("%d.%m.%Y %H:%M"),
            attributes={"item": item_id}
        )
        self.set_state(
            "sensor.hypixel_next_best_sell_pl",
            state=nb_sell.strftime("%d.%m.%Y %H:%M"),
            attributes={"item": item_id}
        )

        self.set_state(
            "sensor.hypixel_buy_price",
            state=round(r["current_buy"], 1),
            attributes={
                "item": item_id,
                "unit_of_measurement": "coins",
                "friendly_name": "Hypixel Buy Price",
            }
        )
        self.set_state(
            "sensor.hypixel_sell_price",
            state=round(r["current_sell"], 1),
            attributes={
                "item": item_id,
                "unit_of_measurement": "coins",
                "friendly_name": "Hypixel Sell Price",
            }
        )

        self.log(f"[{item_id}] {state} | {r['buy_now']} / {r['sell_now']}")