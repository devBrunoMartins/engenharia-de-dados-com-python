class DBConnect:
    def __init__(self, connector, **kwargs):
        self._connector = connector
        self._kwargs = kwargs
        self._db_conn = None
        self._db_cur = None


    def set_conn(self):
        self._db_conn = self._connector.connect(**self._kwargs)

    def set_cursor(self):
        self._db_cur = self._db_conn.cursor()

    def db_connect(self):
        return self._db_conn

    def db_cursor(self):
        return self._db_cur

    def __enter__(self):
        self.set_conn()
        self.set_cursor()
        return self   
         
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self._db_conn.rollback()
            print(f'Rollback executado por causa do erro {exc_val}')
        else:
            self._db_conn.commit()
            self._db_cur.close()
            print("Transaçãao realizada com sucesso.")
        self._db_conn.close()
        return False



if __name__ == "__main__":
    
    import psycopg2
    print("Teste de conexão:")
    try:
        with DBConnect(connector = psycopg2, dbname="python", user="postgres", password="postgres", host="192.168.0.112", port="5432") as db_connect:
            cursor = db_connect.db_cursor()
            cursor.execute("select 1")
            print("Conexão OK!\n")
    except Exception as e:
        print(f"A conexão falhou, verifique os parâmetros.\n {e}")
        

