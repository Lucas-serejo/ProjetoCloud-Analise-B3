# from azure_storage import get_file_from_blob
# from lxml import etree
# import io

# DATA_FILE = "250923"
# FILE_NAME = f"BVBG186_{DATA_FILE}.xml"


# def transform():
#     xml_storage_file = get_file_from_blob(FILE_NAME)
#     xml_bytes = io.BytesIO(xml_storage_file.encode('utf-8'))
    
# #BUSCAR TckrSymb (Nome das Ações)
# #BUSCAR TradDtls (Detalhe das Negociações)
# # volume financeiro, 
# # preço minimo, 
# # preco maximo, 
# # preco abertura, 
# # preco fechamento, 
# # data da negociação

#     for _, elemXml in etree.iterparse(xml_bytes, tag="{urn:bvmf.217.01.xsd}TckrSymb", huge_tree=True):
#         print(f"Ação: {elemXml.text}")

   


# transform()
