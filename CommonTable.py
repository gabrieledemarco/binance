from datetime import datetime

import pandas as pd
from tqdm import tqdm

import DateFunction as dT
from BinanceService import BinanceService
from CreateTables import engine_fin
from DbService import DbService
from InsertValueInTable import InsertValueInTable


class CommonTable: # Update_csn (Crypto, Symbols, Networks)

    def __init__(self, DbService:DbService):
        self.db = DbService
        self.df = pd.read_sql_table('users', engine_fin, index_col='id_user').reset_index()
        engine_fin.dispose()
        self.df_symbols = pd.read_sql_table(table_name="symbols", con=engine_fin)
        engine_fin.dispose()
        self.api_key = self.df.loc[self.df['id_user'] == 1, 'api_key'].values[0]
        self.api_secret = self.df.loc[self.df['id_user'] == 1, 'api_secret'].values[0]
        self.ser_bin = BinanceService(api_key=self.api_key, api_secret=self.api_secret,DbService=DbService)
        self.ins_tab = InsertValueInTable(api_key=self.api_key, api_secret=self.api_secret, DbService=self.db)

    def first_insert_common_table(self):
        self.ins_tab.insert_Crypto()
        self.ins_tab.insert_symbols()
        self.ins_tab.insert_networks()

    def update_update_table(self, name_table: str, end_date: datetime):
        if self.db.count_records(name_table="update_table") != 10:
            self.db.insert(name_table='update_table', list_record=[name_table, end_date])
        else:
            self.db.delete_where_condition(name_table='update_table', where_columns="name_table",
                                           values_column=name_table)
            self.db.insert(name_table='update_table', list_record=[name_table, end_date])

    def update_crypto(self):
        end_date = dT.now_date()
        crypto_db = self.db.get_all_value_in_column(name_column='coin', name_table='crypto')
        coins_bin = [coin['coin'] for coin in self.ser_bin.get_coins()]
        crypto_to_add = list(set(coins_bin) - set(crypto_db))
        crypto_to_del = list(set(crypto_db) - set(coins_bin))

        if crypto_to_del:
            for crypto in crypto_to_del:
                self.db.delete_where_condition(name_table='coin', where_columns='coin', values_column=crypto)

        add_list = []
        if crypto_to_add:
            for coin in tqdm(coins_bin, desc="Crypto's table upsert"):
                if coin['coin'] in crypto_to_add:
                    add_list.append((coin['coin'], coin['name'], coin['withdrawAllEnable'], coin['trading'],
                                     coin['networkList'][0]['withdrawEnable']))

        if add_list:
            self.db.insert(name_table="crypto", list_record=add_list)
            self.update_update_table(name_table="crypto", end_date=end_date)

    def update_symbols(self):
        end_date = dT.now_date()
        symbols_db = self.db.get_all_value_in_column(name_column='symbol', name_table='symbols')
        symbol_data = self.ser_bin.get_symbols()
        symbols_bin = [symbol['symbol'] for symbol in symbol_data]

        symbols_to_add = list(set(symbols_bin) - set(symbols_db))
        symbols_to_del = list(set(symbols_db) - set(symbols_bin))

        if symbols_to_del:
            for symbol in symbols_to_del:
                self.db.delete_where_condition(name_table='symbols', where_columns='symbol', values_column=symbol)

        add_symbols = []
        if symbols_to_add:
            for i in range(len(symbols_bin)):
                if symbols_bin[i] in symbols_to_add:
                    add_symbols.append((symbol_data[i]['symbol'], symbol_data[i]['baseAsset'],
                                        symbol_data[i]['quoteAsset']))

        if add_symbols:
            self.db.insert(name_table='symbols', list_record=add_symbols)
            self.update_update_table(name_table="symbols", end_date=end_date)

    def get_valid_ticker(self, coin: str, quote: str) -> str:
        ticker = ""
        try:
            ticker = self.df_symbols.loc[(self.df_symbols['base_asset'] == coin) &
                                         (self.df_symbols['quote_asset'] == quote), 'symbol'].values[0]
        except IndexError as ex:
            try:
                ticker = self.df_symbols.loc[(self.df_symbols['base_asset'] == quote) &
                                             (self.df_symbols['quote_asset'] == coin), 'symbol'].values[0]
            except IndexError as ex:
                print(ex)
        return ticker

