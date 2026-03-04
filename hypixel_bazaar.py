import appdaemon.plugins.hass.hassapi as hass
import requests
import pymysql
from datetime import datetime, timezone

API_URL = "https://api.hypixel.net/v2/skyblock/bazaar"


class HypixelBazaar(hass.Hass):

    def initialize(self):
        self.log("Hypixel Bazaar collector (item_key) started")

        # --- DB config (z args, żeby nie było hardcode) ---
        db = self.args.get("db", {})
        self.db_host = db.get("host", "core-mariadb")
        self.db_port = int(db.get("port", 3306))
        self.db_user = db.get("user", "hypixel")
        self.db_pass = db.get("password", "hypixel_prices")
        self.db_name = db.get("database", "hypixel")

        self.items_table = self.args.get("items_table", "items")
        self.history_table = self.args.get("history_table", "bazaar_price_history")

        # co ile sekund zbierać
        self.interval = int(self.args.get("interval", 600))

        # cache item_id -> item_key
        self.item_map = {}

        # wczytaj mapę na start
        self.refresh_item_map()

        # Uruchamiaj cyklicznie
        self.run_every(self.collect, "now", self.interval)

    def db_conn(self):
        return pymysql.connect(
            host=self.db_host,
            port=self.db_port,
            user=self.db_user,
            password=self.db_pass,
            database=self.db_name,
            autocommit=False,
            charset="utf8mb4",
        )

    def refresh_item_map(self):
        """Wczytaj całe mapowanie item_id -> item_key do RAM (szybko, mało rekordów)."""
        try:
            conn = self.db_conn()
            cur = conn.cursor()
            cur.execute(f"SELECT item_id, item_key FROM {self.items_table}")
            self.item_map = {row[0]: int(row[1]) for row in cur.fetchall()}
            cur.close()
            conn.close()
            self.log(f"Item map loaded: {len(self.item_map)} items")
        except Exception as e:
            self.log(f"❌ refresh_item_map error: {e}", level="ERROR")
            self.item_map = {}

    def ensure_items_exist(self, item_ids):
        """
        Dodaj brakujące item_id do tabeli items (INSERT IGNORE),
        potem odśwież mapę.
        """
        missing = [iid for iid in item_ids if iid not in self.item_map]
        if not missing:
            return

        try:
            conn = self.db_conn()
            cur = conn.cursor()

            # INSERT IGNORE żeby nie wywalało się przy wyścigu / duplikacie
            cur.executemany(
                f"INSERT IGNORE INTO {self.items_table} (item_id) VALUES (%s)",
                [(iid,) for iid in missing]
            )
            conn.commit()
            cur.close()
            conn.close()

            self.log(f"Added missing items to {self.items_table}: {len(missing)}")
            self.refresh_item_map()

        except Exception as e:
            self.log(f"❌ ensure_items_exist error: {e}", level="ERROR")

    def collect(self, kwargs):
        try:
            response = requests.get(API_URL, timeout=10)
            data = response.json()

            products = data.get("products", {})
            if not products:
                self.log("❌ Bazaar: brak products w odpowiedzi API", level="ERROR")
                return

            item_ids = list(products.keys())

            # dopisz nowe itemy do items (jeśli kiedykolwiek się pojawią)
            self.ensure_items_exist(item_ids)

            now = datetime.now(timezone.utc)

            rows = []
            skipped = 0

            for item_id, item in products.items():
                item_key = self.item_map.get(item_id)
                if item_key is None:
                    skipped += 1
                    continue

                qs = item.get("quick_status", {})
                # zabezpieczenia na brak pól
                buy_price = qs.get("buyPrice")
                sell_price = qs.get("sellPrice")
                buy_volume = qs.get("buyVolume")
                sell_volume = qs.get("sellVolume")

                rows.append((
                    item_key,
                    buy_price,
                    sell_price,
                    buy_volume,
                    sell_volume,
                    now
                ))

            conn = self.db_conn()
            cur = conn.cursor()

            cur.executemany(f"""
                INSERT INTO {self.history_table}
                (item_key, buy_price, sell_price, buy_volume, sell_volume, fetched_at)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, rows)

            conn.commit()
            cur.close()
            conn.close()

            self.log(f"✔ Bazaar zapisano: {len(rows)} rekordów (pominięto: {skipped})")

        except Exception as e:
            self.log(f"❌ Bazaar error: {e}", level="ERROR")