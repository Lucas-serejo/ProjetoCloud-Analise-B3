import psycopg2
import time
from etl.common.config import Config
from etl.transform.xml_parse import run as transform_run

class PostgresLoader:
    def __init__(self):
        self.host = Config.POSTGRES_HOST
        self.port = Config.POSTGRES_PORT
        self.database = Config.POSTGRES_DB
        self.user = Config.POSTGRES_USER
        self.password = Config.POSTGRES_PASSWORD
        self.conn = None
    
    def connect(self, max_retries=5, retry_interval=2):
        """Conecta ao PostgreSQL com tentativas em caso de falha."""
        retry_count = 0
        last_error = None
        
        while retry_count < max_retries:
            try:
                self.conn = psycopg2.connect(
                    host=self.host,
                    port=self.port,
                    database=self.database,
                    user=self.user,
                    password=self.password
                )
                print("[INFO] Conexão com o PostgreSQL estabelecida com sucesso.")
                return True
            except psycopg2.OperationalError as e:
                last_error = e
                retry_count += 1
                wait_time = retry_interval * retry_count
                print(f"[WARNING] Tentativa {retry_count}/{max_retries} falhou. Aguardando {wait_time}s...")
                time.sleep(wait_time)
        
        print(f"[ERROR] Falha ao conectar ao PostgreSQL após {max_retries} tentativas: {last_error}")
        return False
    
    def disconnect(self):
        """Fecha a conexão com o PostgreSQL."""
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def insert_cotacoes(self, cotacoes):
        """Insere ou atualiza cotações no banco de dados."""
        if not self.conn:
            if not self.connect():
                return 0
        
        inserted = 0
        updated = 0
        
        try:
            cursor = self.conn.cursor()
            
            for cotacao in cotacoes:
                # Verifica se o registro já existe
                cursor.execute(
                    "SELECT id FROM cotacoes WHERE ativo = %s AND data_pregao = %s",
                    (cotacao['ativo'], cotacao['data_pregao'])
                )
                existing = cursor.fetchone()
                
                if existing:
                    # Atualiza o registro existente
                    cursor.execute("""
                        UPDATE cotacoes 
                        SET abertura = %s, fechamento = %s, maximo = %s, minimo = %s, volume = %s
                        WHERE ativo = %s AND data_pregao = %s
                    """, (
                        cotacao['abertura'], cotacao['fechamento'], 
                        cotacao['maximo'], cotacao['minimo'], cotacao['volume'],
                        cotacao['ativo'], cotacao['data_pregao']
                    ))
                    updated += 1
                else:
                    # Insere novo registro
                    cursor.execute("""
                        INSERT INTO cotacoes 
                        (ativo, data_pregao, abertura, fechamento, maximo, minimo, volume)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (
                        cotacao['ativo'], cotacao['data_pregao'],
                        cotacao['abertura'], cotacao['fechamento'],
                        cotacao['maximo'], cotacao['minimo'], cotacao['volume']
                    ))
                    inserted += 1
            
            self.conn.commit()
            print(f"[OK] {inserted} registros inseridos, {updated} registros atualizados")
            return inserted + updated
            
        except Exception as e:
            self.conn.rollback()
            print(f"[ERROR] Falha ao inserir cotações: {e}")
            return 0
    
    def execute(self, cotacoes):
        """Executa o processo de carga completo."""
        if not cotacoes:
            print("[WARNING] Nenhuma cotação para inserir")
            return 0
        
        if self.connect():
            try:
                total = self.insert_cotacoes(cotacoes)
                print(f"[SUCCESS] Processo de carga concluído! Total de registros: {total}")
                return total
            finally:
                self.disconnect()
        
        return 0

# Script de execução
def run(cotacoes=None):
    if cotacoes is None:
        # Se não forneceu cotações, tenta obter da etapa de transformação

        cotacoes = transform_run()
    
    loader = PostgresLoader()
    result = loader.execute(cotacoes)
    return result

if __name__ == "__main__":
    run()