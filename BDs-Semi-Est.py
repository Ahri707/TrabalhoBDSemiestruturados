import psycopg2
import lxml.etree as ET

# Função para extrair dados do Postgres
def extrair_pecas_do_postgres(db_name, db_user, db_password, db_host="localhost"):
    pecas = {} # Dicionário para armazenar os dados extraídos
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=db_name, user=db_user, password=db_password, host=db_host
        ) # Dados de conexão no Postgres
        cur = conn.cursor() # Criação do cursor para executar os comandos SQL
        
        cur.execute("SELECT cod_peca, pnome FROM Peca;")
        
        for cod_peca, nome in cur.fetchall(): # Itera sobre os resultados da consulta
            pecas[str(cod_peca)] = nome # Converte o código da peça para string para facilitar a junção depois
        cur.close()
    except Exception as e:
        print(f"Erro ao conectar ou consultar PostgreSQL: {e}")
    finally:
        if conn:
            conn.close()
    return pecas # Retorna o dicionário com os dados das peças extraídos do Postgres

# Função que junta os dados do XML com os do Postgres e gera o XML enriquecido
def realizar_juncao_e_gerar_xml(caminho_xml, dados_pecas):
    try:
        # Carrega o XML
        tree = ET.parse(caminho_xml)
        root = tree.getroot()
        
        # Cria o novo elemento raiz para o XML enriquecido
        root_combinado = ET.Element("DadosIntegrados")
        
        # Itera sobre todas as linhas no XML
        for row in root.findall('row'):
            
            # Extrai o código da peça do XML
            cod_peca_node = row.find('cod_peca')
            if cod_peca_node is not None:
                cod_peca = cod_peca_node.text
                nome_peca = dados_pecas.get(cod_peca, "Nome Desconhecido") # Usa .get() para evitar erro se a chave não existir
            else:
                cod_peca = "N/A"
                nome_peca = "N/A"

            # Cria o novo elemento para o XML enriquecido
            novo_item = ET.SubElement(root_combinado, "DetalheFornecimento")
            
            # Copia os campos originais do XML
            for child in row:
                novo_item.append(child)
            
            # Adiciona o novo campo com o nome da peça (o resultado do JOIN)
            nome_node = ET.SubElement(novo_item, "nome_peca")
            nome_node.text = nome_peca
        # Gera o XML final formatado
        xml_combinado = ET.tostring(root_combinado, pretty_print=True, encoding='utf-8').decode('utf-8')
        
        return xml_combinado 
    except FileNotFoundError:
        return "Erro: Arquivo Fornecimento.xml não encontrado."
    except Exception as e:
        return f"Erro durante o processamento do XML: {e}"

# Configurações para o meu ambiente

POSTGRES_DB = "BDsSemiEst"
POSTGRES_USER = "guilherme"
POSTGRES_PASSWORD = "1010"
CAMINHO_DO_FORNECIMENTO_XML = "Fornecimento.xml" 

# Extração dos dados do PostgreSQL
dados_pecas = extrair_pecas_do_postgres(
    POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
)

# Junção e geração do XML enriquecido
if dados_pecas:
    xml_resultado = realizar_juncao_e_gerar_xml(CAMINHO_DO_FORNECIMENTO_XML, dados_pecas)
    
    # Salva o arquivo
    with open("Resultado.xml", "w", encoding="utf-8") as f:
        f.write(xml_resultado)
          
    print("\nArquivo 'Resultado.xml' salvo!")