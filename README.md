# Hypixel-bazzar-downloader
Downloading prices from hypixel bazzar and storing them on MariaDB database

Aplication in Homeassisatnt.

To oparete you need to create database and write in apps.yaml:

hypixel_bazaar:
  module: hypixel_bazaar
  class: HypixelBazaar

hypixel_trade_helper_mariadb:
  module: hypixel_trade_helper_mariadb
  class: HypixelTradeHelperMariaDB

  table: bazaar_price_history

  query_entity: input_text.hypixel_item_query
  pick_entity: input_select.hypixel_item_pick

  days: 7
  min_points: 200

  suggest_limit: 20
  min_query_len: 2

  # Jeśli fetched_at trzymasz w UTC :
  source_tz: UTC
  display_tz: Europe/Warsaw

  # Opcjonalnie odfiltruj martwy wolumen:
  min_buy_volume: 0
  min_sell_volume: 0

  buy_percentile: 0.15
  sell_percentile: 0.85
  refresh_interval: 600

  

  db:
    host: core-mariadb
    port: [port]]
    user: [user]]
    password: [password]]
    database: [database]]


    type: history-graph
title: "Bazaar – ostatnie 24h"
hours_to_show: 24
refresh_interval: 60
entities:
  - entity: sensor.hypixel_buy_price
    name: Buy
  - entity: sensor.hypixel_sell_price
    name: Sell
