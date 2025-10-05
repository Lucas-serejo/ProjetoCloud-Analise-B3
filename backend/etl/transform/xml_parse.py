import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from etl.common.storage import get_container_client, download_blob_to_string, list_blobs
from etl.common.config import Config
from etl.common.helpers import yymmdd

class B3XMLParser:
    def __init__(self):
        self.container_client = get_container_client()
    
    def list_xml_files(self, date_str):
        """Lista os arquivos XML disponíveis para uma data."""
        prefix = f"xml/{date_str}/"
        blobs = list_blobs(self.container_client, name_starts_with=prefix)
        return [b.name for b in blobs]
    
    def download_xml(self, blob_name):
        """Baixa o conteúdo de um arquivo XML do blob storage."""
        content = download_blob_to_string(self.container_client, blob_name)
        return content
    
    def parse_xml(self, xml_content):
        """Extrai dados de cotações dos arquivos XML da B3."""
        try:
            root = ET.fromstring(xml_content)
            
            # Tenta diferentes namespaces/caminhos para encontrar os dados
            # Versão 1: Sem namespace
            data_element = root.find('.//TradgDt/Dt')
            if data_element is None:
                # Versão 2: Com namespace explícito
                namespace = {'bvmf': 'urn:bvmf.052.01.xsd'}
                data_element = root.find('.//bvmf:TradgDt/bvmf:Dt', namespace)
            
            if data_element is None:
                print("[WARNING] Não foi possível encontrar a data do pregão no XML.")
                # Usar data fixa para desenvolvimento
                data_pregao = datetime.now().date()
                print(f"[INFO] Usando data atual como fallback: {data_pregao}")
            else:
                data_pregao_str = data_element.text
                data_pregao = datetime.strptime(data_pregao_str, '%Y-%m-%d').date()
            
            cotacoes = []
            
            # Tenta encontrar instrumentos em diferentes caminhos
            instrumentos = []
            # Tentativa 1
            instrumentos = root.findall('.//Instmt')
            if not instrumentos:
                # Tentativa 2
                instrumentos = root.findall('.//PricRpt/*/Instmt')
            if not instrumentos:
                # Tentativa 3 com namespace
                namespace = {'bvmf': 'urn:bvmf.052.01.xsd'}
                instrumentos = root.findall('.//bvmf:Instmt', namespace)
            
            print(f"[DEBUG] Encontrados {len(instrumentos)} instrumentos")
            
            # Itera sobre os instrumentos (ações)
            for instmt in instrumentos:
                try:
                    # Tenta diferentes tags para o ticker do ativo
                    ticker_element = instmt.find('TckrSymb')
                    if ticker_element is None:
                        ticker_element = instmt.find('Symb')
                    
                    if ticker_element is None:
                        print("[WARNING] Não foi possível encontrar o ticker do ativo")
                        continue
                        
                    ativo = ticker_element.text
                    
                    # Procura os elementos de preço
                    open_element = instmt.find('OpnPx')
                    close_element = instmt.find('ClsPx')
                    high_element = instmt.find('HghstPx')
                    low_element = instmt.find('LwstPx')
                    vol_element = instmt.find('FinInstrmQty')
                    
                    if all([open_element, close_element, high_element, low_element, vol_element]):
                        cotacao = {
                            'ativo': ativo,
                            'data_pregao': data_pregao,
                            'abertura': float(open_element.text),
                            'fechamento': float(close_element.text),
                            'maximo': float(high_element.text),
                            'minimo': float(low_element.text),
                            'volume': int(float(vol_element.text))
                        }
                        cotacoes.append(cotacao)
                    else:
                        print(f"[WARNING] Dados de preço incompletos para o ativo {ativo}")
                except (AttributeError, ValueError) as e:
                    print(f"[WARNING] Erro ao processar instrumento: {e}")
                    continue
                    
            return cotacoes
            
        except Exception as e:
            print(f"[ERROR] Falha ao processar XML: {e}")
            return []
    
    def execute(self, date_str=None):
        
        # Se não forneceu data, retrocede últimos dias úteis
        if not date_str:
            selected_date = None
            xml_files = []
            for i in range(0, 10):
                dt = datetime.now() - timedelta(days=i)
                if dt.weekday() >= 5:  # pula sábado e domingo
                    continue
                candidate = yymmdd(dt)
                files = self.list_xml_files(candidate)
                if files:
                    selected_date = candidate
                    xml_files = files
                    break
            if not xml_files:
                print("[ERROR] Nenhum arquivo XML encontrado nos últimos dias úteis. Saindo.")
                return []
            date_str = selected_date
        else:
            xml_files = self.list_xml_files(date_str)
            if not xml_files:
                print(f"[INFO] Nenhum arquivo XML encontrado para {date_str}. Saindo.")
                return []
        
        print(f"[INFO] Encontrados {len(xml_files)} arquivos XML com prefixo 'xml/{date_str}/'")
        
        all_cotacoes = []
        
        # Processa cada arquivo XML
        for xml_file in xml_files:
            print(f"[INFO] Processando {xml_file}...")
            xml_content = self.download_xml(xml_file)
            if not xml_content:
                print(f"[WARNING] Não foi possível baixar o conteúdo de {xml_file}")
                continue
                
            cotacoes = self.parse_xml(xml_content)
            if cotacoes:
                all_cotacoes.extend(cotacoes)
                print(f"[OK] Extraídas {len(cotacoes)} cotações de {xml_file}")
            else:
                print(f"[WARNING] Nenhuma cotação válida extraída de {xml_file}")
        
        print(f"[INFO] Total de cotações extraídas: {len(all_cotacoes)}")
        return all_cotacoes

# Script de execução
def run():
    parser = B3XMLParser()
    cotacoes = parser.execute()
    return cotacoes

if __name__ == "__main__":
    run()