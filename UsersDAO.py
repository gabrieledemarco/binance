from datetime import datetime

from DbService import DbService
from InsertValueInTable import InsertValueInTable


class UsersDAO:

    def __init__(self,
                 DbService: DbService,
                 api_key: str = None,
                 api_secret: str = None,
                 nick_name: str = None,
                 pass_word: str = None):
        self.db_ser = DbService
        self.api_key = api_key
        self.api_secret = api_secret
        self.nick_name = nick_name
        self.pass_word = pass_word

    def get_Api_of_usr(self):
        Api = self.db_ser.get_select_with_where(select_columns=['api_key', 'api_secret'],
                                                where_columns='nickname',
                                                values_column=self.nick_name,
                                                name_table='users')
        return Api

    def is_user_registered(self) -> bool:
        user = self.db_ser.get_select_with_where(select_columns='nickname',
                                                 name_table='users',
                                                 where_columns='nickname',
                                                 values_column=self.nick_name)
        value = 0
        if len(user) == 0:
            value = False
        elif len(user) > 0:
            value = True
        else:
            print("something goes wrong")

        return value

    def insert_user(self):
        return self.db_ser.insert(name_table='users', list_record=[(self.api_key,
                                                                    self.api_secret,
                                                                    self.nick_name,
                                                                    self.pass_word)])


    def insert_new_user_and_data(self):
        self.insert_user()
        insert_value = InsertValueInTable(api_key=self.api_key, api_secret=self.api_secret)
        insert_value.insert_dividends()
        insert_value.insert_orders()
        insert_value.insert_trades()
        insert_value.insert_deposit_withdraw()
