from config import Config
import psycopg2
from datetime import datetime
from xml_parse import run as transform_run
import time

class PostgresLoader:
    def __init__(self):
        self.conn = None
        self.cursor = None
    
    def load_cotacoes(self, cotacoes):
        """Alias para execute(), por compatibilidade."""
        return self.execute(cotacoes)
    
    def close(self):
        """Alias para disconnect(), por compatibilidade."""
        self.disconnect()
    
    def connect(self, max_retries=5, retry_interval=2):
        """
        Conecta ao PostgreSQL de forma resiliente.
        Prioriza a POSTGRES_CONNECTION_STRING (para o Azure) e faz fallback
        para variáveis de ambiente separadas (para desenvolvimento local).
        """
        for attempt in range(max_retries):
            try:
                connection_string = Config.POSTGRES_CONNECTION_STRING
                
                if connection_string:
                    # Método 1: Usar a connection string (ideal para o Azure)
                    self.conn = psycopg2.connect(connection_string)
                else:
                    # Método 2: Fallback para variáveis separadas (para ambiente local)
                    self.conn = psycopg2.connect(
                        host=Config.POSTGRES_HOST,
                        port=Config.POSTGRES_PORT,
                        dbname=Config.POSTGRES_DB,
                        user=Config.POSTGRES_USER,
                        password=Config.POSTGRES_PASSWORD,
                        sslmode=Config.POSTGRES_SSL_MODE
                    )

                self.conn.autocommit = False
                self.cursor = self.conn.cursor()
                print("[INFO] Conexão PostgreSQL estabelecida")
                return True
            
            except psycopg2.OperationalError as e:
                if attempt < max_retries - 1:
                    print(f"[WARNING] Falha na conexão (tentativa {attempt+1}/{max_retries}): {str(e)}")
                    time.sleep(retry_interval)
                else:
                    print(f"[ERROR] Não foi possível conectar ao PostgreSQL após {max_retries} tentativas: {str(e)}")
                    raise
        return False
    
    def disconnect(self):
        # Encerra a conexão
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
    
    def truncate_table(self):
        """Esvazia a tabela cotacoes (restart identity)."""
        try:
            if not self.conn or self.conn.closed:
                self.connect()
            
            self.cursor.execute("TRUNCATE TABLE cotacoes RESTART IDENTITY CASCADE")
            self.conn.commit()
            print("[INFO] Tabela 'cotacoes' esvaziada com sucesso")
            return True
        except Exception as e:
            if self.conn:
                self.conn.rollback()
            print(f"[ERROR] Falha ao esvaziar tabela: {str(e)}")
            raise
    
    def execute(self, cotacoes):
        # Insere/atualiza cotações usando batch upsert (muito mais rápido)
        if not cotacoes:
            print("[WARNING] Nenhuma cotação para inserir")
            return 0
            
        try:
            if not self.conn or self.conn.closed:
                self.connect()
            
            # Usar ON CONFLICT para upsert em lote
            insert_query = """
                INSERT INTO cotacoes (ativo, data_pregao, abertura, fechamento, maximo, minimo, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ativo, data_pregao) 
                DO UPDATE SET
                    abertura = EXCLUDED.abertura,
                    fechamento = EXCLUDED.fechamento,
                    maximo = EXCLUDED.maximo,
                    minimo = EXCLUDED.minimo,
                    volume = EXCLUDED.volume
            """
            
            # Preparar dados para batch insert
            batch_data = [
                (
                    cotacao['ativo'],
                    cotacao['data_pregao'],
                    cotacao['abertura'],
                    cotacao['fechamento'],
                    cotacao['maximo'],
                    cotacao['minimo'],
                    cotacao['volume']
                )
                for cotacao in cotacoes
            ]
            
            # Executar em batch
            from psycopg2.extras import execute_batch
            execute_batch(self.cursor, insert_query, batch_data, page_size=500)
            
            self.conn.commit()
            print(f"[SUCCESS] Processo de carga concluído! {len(cotacoes)} registros processados em batch")
            return len(cotacoes)
            
        except Exception as e:
            if self.conn:
                self.conn.rollback()
            print(f"[ERROR] Falha ao inserir cotações: {str(e)}")
            raise
            
        finally:
            self.disconnect()

def run(cotacoes=None): 
    if cotacoes is None:
        cotacoes = transform_run()
        
    loader = PostgresLoader()
    return loader.execute(cotacoes)

if __name__ == "__main__":
    run()