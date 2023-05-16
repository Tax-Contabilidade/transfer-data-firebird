import fdb


class Connect():

    def __init__(self, db: str = 'AC'):
        self.con = fdb.connect(
            host='192.168.1.100',
            database=f'C:\\Banco\\{db}.FDB',
            port=53052,
            user='sysdba',
            password='masterkey',
            charset='ISO8859_1'
        )
        self.cur = self.con.cursor()

    def execute_query(self, sql, *args):
        cur = self.cur
        lista = tuple(list(*args))

        cur.execute(sql, lista)
        return cur.fetchall()
