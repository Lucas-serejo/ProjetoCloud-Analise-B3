from lxml import etree as ET
from datetime import datetime, timedelta
from etl.common.storage import get_container_client, download_blob_to_string, list_blobs
from etl.common.config import Config
from etl.common.helpers import yymmdd
import json
import re

class B3XMLParser:
    def __init__(self):
        self.container_client = get_container_client()

    # Lista XMLs no Blob para uma data
    def list_xml_files(self, date_str):
        prefix = f"xml/{date_str}/"
        blobs = list_blobs(self.container_client, name_starts_with=prefix)
        return [b.name for b in blobs]

    def download_xml(self, blob_name):
        content = download_blob_to_string(self.container_client, blob_name)
        return content

    def parse_xml(self, xml_content):
        # Faz parse do XML e extrai cotações via XPath
        try:
            data = xml_content.encode("utf-8") if isinstance(xml_content, str) else xml_content
            root = ET.fromstring(data)

            namespaces = {
                'bvmf217': 'urn:bvmf.217.01.xsd',
                'bvmf052': 'urn:bvmf.052.01.xsd',
                'head': 'urn:iso:std:iso:20022:tech:xsd:head.001.001.01'
            }

            print(f"[DEBUG] Root tag: {root.tag}")

            # Extrai data do pregão
            data_s = root.xpath("string(.//bvmf217:TradDt/bvmf217:Dt)", namespaces=namespaces)
            if data_s:
                data_pregao = datetime.strptime(data_s, "%Y-%m-%d").date()
                print(f"[DEBUG] Data pregão encontrada: {data_pregao}")
            else:
                data_pregao = datetime.now().date()
                print(f"[DEBUG] Data pregão não encontrada, usando data atual: {data_pregao}")

            price_reports = root.xpath(".//bvmf217:PricRpt", namespaces=namespaces)
            print(f"[DEBUG] Encontrados {len(price_reports)} relatórios de preço")

            # Monta lista de cotações
            cotacoes = []
            for i, report in enumerate(price_reports):
                try:
                    # Ticker
                    ticker_node = report.xpath(".//*[local-name()='TckrSymb']")
                    if not ticker_node or not ticker_node[0].text:
                        continue
                    
                    ativo = ticker_node[0].text.strip()

                    # Código do mercado
                    market_code_node = report.xpath(".//*[local-name()='MktIdrCd']")
                    market_code = market_code_node[0].text.strip() if market_code_node and market_code_node[0].text else ""
                    if market_code not in ["BVMF", "XBSP", "BOVESPA"]:
                        continue

                    # Filtra apenas ações à vista: 3-5 letras + 1-2 dígitos
                    if not re.match(r'^[A-Z]{3,5}\d{1,2}$', ativo):
                        continue

                    # Atributos financeiros
                    attrs_node = report.xpath(".//*[local-name()='FinInstrmAttrbts']")
                    if not attrs_node:
                        continue
                    
                    # Usa o primeiro
                    attrs = attrs_node[0]
                    fechamento = attrs.xpath(".//*[local-name()='LastPric']")
                    if not fechamento or not fechamento[0].text:
                        continue

                    preco_fechamento = float(fechamento[0].text.strip())

                    # Helpers de extração
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

    # Executa extração e transformação
    def execute(self, multi_day=False, days_limit=5):
        """Processa um ou vários dias, conforme parâmetros."""
        all_cotacoes = []
        days_processed = 0
        
        # Lista de dias úteis a processar
        dates_to_process = []
        today = datetime.now().date()
        
        for i in range(0, days_limit):
            dt = (datetime.now() - timedelta(days=i)).date()
            
            if dt > today:
                continue

            if dt.weekday() >= 5:
                continue
                
            dates_to_process.append(yymmdd(datetime.combine(dt, datetime.min.time())))
        
        # single-day: para no primeiro dia com XMLs
        if not multi_day:
            for date_str in dates_to_process:
                xml_files = self.list_xml_files(date_str)
                if xml_files:
                    cotacoes = self._process_date(date_str, xml_files)
                    all_cotacoes.extend(cotacoes)
                    break
        
        # multi-day: percorre todos os dias com XMLs
        else:
            for date_str in dates_to_process:
                xml_files = self.list_xml_files(date_str)
                if not xml_files:
                    print(f"[INFO] Nenhum arquivo XML encontrado para {date_str}")
                    continue
                    
                print(f"[INFO] Processando dia: {date_str}")
                cotacoes = self._process_date(date_str, xml_files)
                all_cotacoes.extend(cotacoes)
                days_processed += 1
        
        print(f"[INFO] Total de {days_processed} dias processados")
        print(f"[INFO] Total de {len(all_cotacoes)} cotações extraídas")
        
        return all_cotacoes

    # Processa XMLs de uma data
    def _process_date(self, date_str, xml_files):
        """Processa todos os XMLs de uma data."""
        date_cotacoes = []
        
        for xml_file in xml_files:
            print(f"[INFO] Processando {xml_file}...")
            xml_content = self.download_xml(xml_file)
            if not xml_content:
                print(f"[WARNING] Não foi possível baixar o conteúdo de {xml_file}")
                continue

            cotacoes = self.parse_xml(xml_content)
            if cotacoes:
                date_cotacoes.extend(cotacoes)
                print(f"[OK] Extraídas {len(cotacoes)} cotações de {xml_file}")
            else:
                print(f"[WARNING] Nenhuma cotação válida extraída de {xml_file}")
        
        # Exporta JSON se habilitado
        if date_cotacoes and Config.EXPORT_JSON:
            json_path = Config.DATA_DIR / f"cotacoes_{date_str}.json"
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(date_cotacoes, f, ensure_ascii=False, default=str, indent=2)
            print(f"[INFO] Cotações exportadas para JSON: {json_path}")
            
        return date_cotacoes

def run():
    parser = B3XMLParser()
    cotacoes = parser.execute()
    return cotacoes

if __name__ == "__main__":
    run()
