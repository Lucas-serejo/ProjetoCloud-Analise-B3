import json
from datetime import datetime, timedelta
from lxml import etree
from etl.common.storage import get_container_client, download_blob_to_string, list_blobs
from etl.common.config import Config
from etl.common.helpers import yymmdd

class B3XMLParser:
    def __init__(self):
        self.container_client = get_container_client()
        # Definindo namespaces comuns nos XMLs da B3
        self.namespaces = {
            'bvmf': 'urn:bvmf.052.01.xsd',
            'bvmf186': 'urn:bvmf.186.01.xsd',
            'bvmf217': 'urn:bvmf.217.01.xsd',  # Adicionado namespace do arquivo específico
            'head': 'urn:iso:std:iso:20022:tech:xsd:head.001.001.01'
        }
    
    def list_xml_files(self, date_str):
        """Lista os arquivos XML disponíveis para uma data."""
        prefix = f"xml/{date_str}/"
        blobs = list_blobs(self.container_client, name_starts_with=prefix)
        return [b.name for b in blobs]
    
    def download_xml(self, blob_name):
        """Baixa o conteúdo de um arquivo XML do blob storage."""
        content = download_blob_to_string(self.container_client, blob_name)
        return content
    
    def _get_root_namespace(self, root):
        """Detecta o namespace principal do documento XML"""
        tag = root.tag
        if '}' in tag:
            return tag.split('}')[0].strip('{')
        return None
    
    def parse_xml(self, xml_content):
        """Processa o XML da B3 para extrair cotações do mercado à vista."""
        try:
            parser = etree.XMLParser(recover=True, huge_tree=True)
            root = etree.fromstring(xml_content, parser)
            
            # A MUDANÇA CRÍTICA ESTÁ AQUI: Usar o nsmap do lxml
            # Ele captura todos os namespaces, inclusive o default (que não tem prefixo)
            nsmap = root.nsmap
            if None in nsmap: # Se houver um namespace padrão...
                nsmap['ns'] = nsmap.pop(None) # ...damos a ele o prefixo 'ns' para usar no XPath
            
            print(f"[INFO] Namespaces do documento (nsmap): {nsmap}")
            
            self._debug_xml_structure(root, nsmap) # Passa o nsmap para o debug
            
            data_pregao = self._extract_date(root, nsmap) # Passa o nsmap
            
            cotacoes = []
            
            # A busca agora usará o nsmap dinâmico
            securities = self._find_securities(root, nsmap)
            
            if not securities:
                self._debug_xml_fragment(xml_content)
                return []
                
            for security in securities:
                try:
                    # Passa o nsmap para os métodos de extração
                    ticker_info = self._extract_ticker(security, nsmap)
                    if not ticker_info:
                        continue
                    
                    ativo, is_spot_market = ticker_info
                    
                    if not is_spot_market:
                        continue
                    
                    price_data = self._extract_price_data(security, nsmap)
                    if not price_data or price_data.get('fechamento') is None:
                        # print(f"[WARNING] Dados de preço incompletos para o ativo {ativo}")
                        continue
                        
                    cotacao = {
                        'ativo': ativo,
                        'data_pregao': data_pregao,
                        'abertura': price_data.get('abertura', price_data['fechamento']),
                        'fechamento': price_data.get('fechamento'),
                        'maximo': price_data.get('maximo', price_data['fechamento']),
                        'minimo': price_data.get('minimo', price_data['fechamento']),
                        'volume': int(price_data.get('volume', 0))
                    }
                    cotacoes.append(cotacao)
                    # Descomente para um log mais detalhado
                    # print(f"[OK] Cotação extraída para {ativo}: {cotacao}")
                
                except Exception as e:
                    print(f"[WARNING] Erro ao processar instrumento: {e}")
                    continue
            
            if cotacoes:
                found_tickers = [c['ativo'] for c in cotacoes[:5]]
                print(f"[DEBUG] Sucesso! Primeiros 5 ativos encontrados: {', '.join(found_tickers)}")
                
            return cotacoes
            
        except Exception as e:
            print(f"[ERROR] Falha crítica ao processar XML: {str(e)}")
            import traceback
            traceback.print_exc()
            return []
    
    def _debug_xml_structure(self, root, nsmap=None):
        """Imprime a estrutura do documento para debug"""
        print("[DEBUG] Estrutura do documento:")
        for i, child in enumerate(root[:5]):
            print(f"  Elemento {i}: {child.tag}")
            for j, grandchild in enumerate(child[:3]):
                print(f"    Subelemento {j}: {grandchild.tag}")
    
    def _debug_xml_fragment(self, xml_content):
        """Mostra um fragmento do XML para diagnóstico"""
        print("[DEBUG] Fragmento do XML para análise:")
        try:
            lines = xml_content[:2000].decode('utf-8').split('\n')
            for i, line in enumerate(lines[:20]):
                print(f"{i+1}: {line}")
        except Exception as e:
            print(f"[DEBUG] Não foi possível mostrar o fragmento do XML: {str(e)}")

    def _extract_date(self, root, nsmap=None):
        """Extrai a data do pregão usando múltiplas estratégias"""
        # Tentativas com XPath mais avançado do lxml
        xpath_patterns = [
            ".//BizDt",
            ".//TradDt",
            ".//BizDay",
            ".//*[local-name()='BizDt']",
            ".//*[local-name()='TradDt']",
            ".//b3:BizDt",
            ".//bvmf:TradgDt/bvmf:Dt",
            ".//TradgDt/Dt"
        ]
        
        for xpath in xpath_patterns:
            try:
                date_elem = root.xpath(xpath, namespaces=self.namespaces)
                if date_elem and date_elem[0].text:
                    data_pregao_str = date_elem[0].text
                    data_pregao = datetime.strptime(data_pregao_str, '%Y-%m-%d').date()
                    print(f"[INFO] Data do pregão encontrada: {data_pregao}")
                    return data_pregao
            except:
                continue
                
        # Tenta extrair do nome do arquivo
        try:
            filename = root.base
            if filename and "202" in filename:  # Procura por padrão de ano 202x
                # Extrai AAAAMMDD de posições prováveis
                for i in range(len(filename)-8):
                    date_str = filename[i:i+8]
                    if date_str.isdigit() and date_str.startswith("202"):  # Ano 202x
                        try:
                            data_pregao = datetime.strptime(date_str, '%Y%m%d').date()
                            print(f"[INFO] Data extraída do nome do arquivo: {data_pregao}")
                            return data_pregao
                        except:
                            pass
        except:
            pass
            
        # Último recurso: data atual
        data_pregao = datetime.now().date()
        print(f"[INFO] Usando data atual como fallback: {data_pregao}")
        return data_pregao
    
    def _find_securities(self, root, nsmap):
        """Localiza instrumentos financeiros no formato BVBG.186.01/BVMF.217.01."""
        print("[INFO] Buscando instrumentos financeiros no formato BVBG.186.01...")
        
        # XPath específico para o formato que vimos no XML
        xpath_patterns = [
            # Padrão específico para o formato BVMF.217.01
            ".//ns:Xchg/ns:BizGrp/ns:Document/bvmf217:PricRpt",
            # Variação com namespace dinâmico
            ".//*[local-name()='Xchg']/*[local-name()='BizGrp']/*[local-name()='Document']/*[local-name()='PricRpt']"
        ]
        
        for pattern in xpath_patterns:
            try:
                securities = root.xpath(pattern, namespaces=nsmap)
                if securities:
                    print(f"[INFO] Encontrados {len(securities)} instrumentos com padrão '{pattern}'")
                    return securities
            except Exception as e:
                print(f"[DEBUG] Erro ao aplicar XPath '{pattern}': {e}")
                continue
                
        print("[WARNING] Nenhum instrumento financeiro encontrado nos padrões BVBG.186.01")
        return []
    
    def _extract_ticker(self, security, nsmap):
        """Extrai o ticker no formato BVMF.217.01."""
        try:
            # Caminho específico para o formato que vimos
            ticker_node = security.xpath("./*[local-name()='SctyId']/*[local-name()='TckrSymb']")
            
            if ticker_node and ticker_node[0].text:
                ativo = ticker_node[0].text.strip()
                print(f"[DEBUG] Ticker encontrado: {ativo}")
                
                # Filtro para mercado à vista (ações da B3 normalmente terminam com número)
                # PETR4, VALE3, ITUB4, etc.
                import re
                is_spot_market = bool(re.match(r'^[A-Z]{4}\d$', ativo))
                
                # Se não for ação do mercado à vista, rejeitamos
                if not is_spot_market:
                    return None
                    
                return (ativo, True)
                
        except Exception as e:
            print(f"[DEBUG] Erro ao extrair ticker: {e}")
            
        return None

    def _extract_price_data(self, security, nsmap):
        """Extrai dados de preço no formato BVMF.217.01."""
        try:
            # Caminho para o elemento que contém os dados de preço
            attrs_node = security.xpath("./*[local-name()='FinInstrmAttrbts']")[0]
            
            # Mapeamento dos campos específicos para este formato
            field_map = {
                'abertura': ["./*[local-name()='FrstPric']"],
                'fechamento': ["./*[local-name()='LastPric']"],
                'maximo': ["./*[local-name()='MaxPric']"],
                'minimo': ["./*[local-name()='MinPric']"],
                'volume': ["./*[local-name()='RglrTxsQty']"]
            }
            
            price_data = {}
            
            for field, xpaths in field_map.items():
                for xpath in xpaths:
                    try:
                        nodes = attrs_node.xpath(xpath)
                        if nodes and nodes[0].text:
                            price_data[field] = float(nodes[0].text.strip())
                            break
                    except:
                        continue
            
            return price_data if 'fechamento' in price_data else None
            
        except Exception as e:
            print(f"[DEBUG] Erro ao extrair dados de preço: {e}")
            return None

    def _print_element_structure(self, element, depth=0, max_depth=3):
        """Imprime a estrutura de um elemento XML para debug"""
        if depth >= max_depth:
            return
        
        indent = "  " * depth
        tag = element.tag.split('}')[-1] if '}' in element.tag else element.tag
        
        attrs = []
        for key, value in element.attrib.items():
            key_name = key.split('}')[-1] if '}' in key else key
            attrs.append(f"{key_name}='{value}'")
        
        attr_str = " ".join(attrs)
        if attr_str:
            attr_str = f" [{attr_str}]"
        
        value = element.text.strip() if element.text and element.text.strip() else ""
        if value:
            value = f": '{value}'"
        
        print(f"{indent}{tag}{attr_str}{value}")
        
        for child in element:
            self._print_element_structure(child, depth + 1, max_depth)
    
    def execute(self, date_str=None):
        """Executa o processo de transformação para uma data específica."""
        # Se não forneceu data, usa hoje
        if not date_str:
            date_str = yymmdd(datetime.now())
        
        # Lista os arquivos XML disponíveis
        xml_files = self.list_xml_files(date_str)
        print(f"[INFO] Encontrados {len(xml_files)} arquivos XML com prefixo 'xml/{date_str}/'")
        
        # Se não encontrou para hoje, tenta até 3 dias para trás
        if not xml_files:
            for days_back in range(1, 4):  # Tenta até 3 dias para trás
                previous_date = yymmdd(datetime.now() - timedelta(days=days_back))
                print(f"[INFO] Tentando para {days_back} dia(s) atrás ({previous_date})...")
                xml_files = self.list_xml_files(previous_date)
                print(f"[INFO] Encontrados {len(xml_files)} arquivos XML com prefixo 'xml/{previous_date}/'")
                if xml_files:
                    date_str = previous_date
                    break
        
        if not xml_files:
            print("[ERROR] Nenhum arquivo XML encontrado para os últimos 3 dias. Saindo.")
            return []
        
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
        
        # Opcionalmente, exportar para JSON
        if all_cotacoes and Config.EXPORT_JSON:
            json_path = Config.DATA_DIR / f"cotacoes_{date_str}.json"
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(all_cotacoes, f, ensure_ascii=False, default=str, indent=2)
            print(f"[INFO] Cotações exportadas para JSON: {json_path}")
            
        return all_cotacoes

# Script de execução
def run():
    parser = B3XMLParser()
    cotacoes = parser.execute()
    return cotacoes

if __name__ == "__main__":
    run()