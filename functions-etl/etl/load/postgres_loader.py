from etl.common.config import Config
import psycopg2
from datetime import datetime
from etl.transform.xml_parse import run as transform_run

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
        # Conecta ao PostgreSQL com retries simples
        import time
        
        for attempt in range(max_retries):
            try:
                self.conn = psycopg2.connect(
                    host=Config.POSTGRES_HOST,
                    port=Config.POSTGRES_PORT,
                    dbname=Config.POSTGRES_DB,
                    user=Config.POSTGRES_USER,
                    password=Config.POSTGRES_PASSWORD,
                    sslmode=getattr(Config, "POSTGRES_SSL_MODE", "require")
                )
                self.conn.autocommit = False
                self.cursor = self.conn.cursor()
                print("[INFO] Conexão PostgreSQL estabelecida")
                return True
            # Re-tenta em falha operacional
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
        # Insere/atualiza cotações
        if not cotacoes:
            print("[WARNING] Nenhuma cotação para inserir")
            return 0
            
        try:
            if not self.conn or self.conn.closed:
                self.connect()
                
            inserted = 0
            updated = 0
            
            for cotacao in cotacoes:
                # Monta registro
                registro = {
                    'ativo': cotacao['ativo'],
                    'data_pregao': cotacao['data_pregao'],
                    'abertura': cotacao['abertura'],
                    'fechamento': cotacao['fechamento'],
                    'maximo': cotacao['maximo'],
                    'minimo': cotacao['minimo'],
                    'volume': cotacao['volume'],
                }
                
                # Existe?
                self.cursor.execute(
                    "SELECT id FROM cotacoes WHERE ativo = %s AND data_pregao = %s",
                    (registro['ativo'], registro['data_pregao'])
                )
                
                result = self.cursor.fetchone()
                
                if result:
                    # Atualiza
                    cotacao_id = result[0]
                    update_fields = []
                    update_values = []
                    
                    for key, value in registro.items():
                        # Não atualiza chaves
                        if key != 'ativo' and key != 'data_pregao':  
                            update_fields.append(f"{key} = %s")
                            update_values.append(value)
                    
                    update_query = f"UPDATE cotacoes SET {', '.join(update_fields)} WHERE id = %s"
                    self.cursor.execute(update_query, update_values + [cotacao_id])
                    updated += 1
                else:
                    # Insere
                    columns = ', '.join(registro.keys())
                    placeholders = ', '.join(['%s'] * len(registro))
                    insert_query = f"INSERT INTO cotacoes ({columns}) VALUES ({placeholders})"
                    self.cursor.execute(insert_query, list(registro.values()))
                    inserted += 1
            
            self.conn.commit()
            print(f"[SUCCESS] Processo de carga concluído! Inseridos: {inserted}, Atualizados: {updated}")
            return inserted + updated
            
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