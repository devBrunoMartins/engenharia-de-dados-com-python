class DBConnect:
    def __init__(self, connector, **kwargs):
        self.connector = connector
        self.kwargs = kwargs
        self.conn = None
        self.cursor = None

    def __enter__(self):

        self.conn = self.connector.connect(**self.kwargs)
        self.cursor = self.conn.cursor()
        return self.cursor
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.conn.rollback()
            print(f'Rollback executado por causa do erro {exc_val}')
        else:
            self.conn.commit()
            self.cursor.close()
            print("Transação realizada com sucesso!")
        self.conn.close()
        return False


if __name__ == "__main__":
    ...    

    # with DBConnect("postgres", "postgres", "postgres", "192.168.0.112", "5432") as conexao:
    #     ...
