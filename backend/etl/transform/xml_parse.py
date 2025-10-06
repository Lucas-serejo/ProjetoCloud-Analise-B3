from lxml import etree as ET
from datetime import datetime, timedelta
from etl.common.storage import get_container_client, download_blob_to_string, list_blobs
from etl.common.config import Config
from etl.common.helpers import yymmdd
import re

class B3XMLParser:
    def __init__(self):
        self.container_client = get_container_client()

    # Lista arquivos XML no blob storage para uma data específica
    def list_xml_files(self, date_str):
        prefix = f"xml/{date_str}/"
        blobs = list_blobs(self.container_client, name_starts_with=prefix)
        return [b.name for b in blobs]

    def download_xml(self, blob_name):
        content = download_blob_to_string(self.container_client, blob_name)
        return content

    def parse_xml(self, xml_content):
        # Faz o parse do XML e extrai as cotações usando XPath
        try:
            data = xml_content.encode("utf-8") if isinstance(xml_content, str) else xml_content
            root = ET.fromstring(data)

            namespaces = {
                'bvmf217': 'urn:bvmf.217.01.xsd',
                'bvmf052': 'urn:bvmf.052.01.xsd',
                'head': 'urn:iso:std:iso:20022:tech:xsd:head.001.001.01'
            }

            print(f"[DEBUG] Root tag: {root.tag}")

            # Extrai a data do pregão
            data_s = root.xpath("string(.//bvmf217:TradDt/bvmf217:Dt)", namespaces=namespaces)
            if data_s:
                data_pregao = datetime.strptime(data_s, "%Y-%m-%d").date()
                print(f"[DEBUG] Data pregão encontrada: {data_pregao}")
            else:
                data_pregao = datetime.now().date()
                print(f"[DEBUG] Data pregão não encontrada, usando data atual: {data_pregao}")

            price_reports = root.xpath(".//bvmf217:PricRpt", namespaces=namespaces)
            print(f"[DEBUG] Encontrados {len(price_reports)} relatórios de preço")

            # Monta a lista de cotações
            cotacoes = []
            for i, report in enumerate(price_reports):
                try:
                    print(f"[DEBUG] Processando relatório {i+1}")

                    # Verifica se o ativo está presente, se sim extrai
                    ticker_node = report.xpath(".//*[local-name()='TckrSymb']")
                    if not ticker_node or not ticker_node[0].text:
                        continue
                    
                    ativo = ticker_node[0].text.strip()

                    # Verifica se o código de mercado está presente 
                    market_code_node = report.xpath(".//*[local-name()='MktIdrCd']")
                    market_code = market_code_node[0].text.strip() if market_code_node and market_code_node[0].text else ""
                    if market_code not in ["BVMF", "XBSP", "BOVESPA"]:
                        continue
                    
                    # Filtra ativos que não são ações ou ETFs
                    if not (re.match(r'^[A-Z]{3,5}\d{1,2}$', ativo) or re.match(r'^[A-Z]{4,5}11$', ativo)):
                        continue

                    # Verifica se existem atributos financeiros
                    attrs_node = report.xpath(".//*[local-name()='FinInstrmAttrbts']")
                    if not attrs_node:
                        continue
                    
                    # Se houver, pega o primeiro (deveria haver só um)
                    attrs = attrs_node[0]
                    fechamento = attrs.xpath(".//*[local-name()='LastPric']")
                    if not fechamento or not fechamento[0].text:
                        continue

                    preco_fechamento = float(fechamento[0].text.strip())

                    # Funções auxiliares para extração de valores
                    def extrair_float(xpath_expr):
                        val = attrs.xpath(xpath_expr)
                        return float(val[0].text.strip()) if val and val[0].text else preco_fechamento

                    def extrair_int(xpath_expr):
                        val = attrs.xpath(xpath_expr)
                        return int(val[0].text.strip()) if val and val[0].text else 0

                    cotacoes.append({
                        "ativo": ativo,
                        "data_pregao": data_pregao,
                        "abertura": extrair_float(".//*[local-name()='FrstPric']"),
                        "fechamento": preco_fechamento,
                        "maximo": extrair_float(".//*[local-name()='MaxPric']"),
                        "minimo": extrair_float(".//*[local-name()='MinPric']"),
                        "volume": extrair_int(".//*[local-name()='RglrTxsQty']")
                    })

                except Exception as e:
                    print(f"[WARNING] Erro ao processar ativo: {e}")
                    continue

            print(f"[DEBUG] Total de cotações válidas: {len(cotacoes)}")
            return cotacoes

        except Exception as e:
            print(f"[ERROR] Falha no parse XPath: {e}")
            import traceback
            traceback.print_exc()
            return []

    # Executa o processo de extração e transformação
    def execute(self, date_str=None):
        # Se date_str não for fornecida, tenta encontrar a data mais recente com arquivos XML
        if not date_str:
            selected_date = None
            xml_files = []
            for i in range(0, 10):
                dt = datetime.now() - timedelta(days=i)
                if dt.weekday() >= 5:
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
        # Se date_str for fornecida, lista arquivos XML para essa data
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
            # Verifica se as cotações foram extraídas com sucesso
            if cotacoes:
                all_cotacoes.extend(cotacoes)
                print(f"[OK] Extraídas {len(cotacoes)} cotações de {xml_file}")
            else:
                print(f"[WARNING] Nenhuma cotação válida extraída de {xml_file}")

        print(f"[INFO] Total de cotações extraídas: {len(all_cotacoes)}")
        return all_cotacoes

def run():
    parser = B3XMLParser()
    cotacoes = parser.execute()
    return cotacoes

if __name__ == "__main__":
    run()
