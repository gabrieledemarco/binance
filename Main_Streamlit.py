from UsersDAO import UsersDAO
from DbService import DbService
from HomePage import Home_Page
from SideLog import Sign
import schedule
from UpdateClientTable import UpdateClientTable
import time


def main(Dbs: DbService):
    nick = Sign(Dbs)
    if nick is not None:
        api = UsersDAO(DbService=Dbs, nick_name=nick).get_Api_of_usr()
        Home_Page(api_key=api[0][0], api_secret=api[0][1], DbService=Dbs)
    else:
        Home_Page(DbService=Dbs)


#try:
Dbs = DbService()
#update_t = UpdateClientTable(DbService=Dbs)
#schedule.every().day.at("13:15").do(update_t.update_all_table())

main(Dbs)

