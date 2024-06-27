import json
import pandas as pd
import datetime
import logging
import os 

# Definir o arquivo de configuração
arquivo_config = "config.json"  # Ajustar o nome do arquivo de configuração

# Carregar configuração do arquivo JSON
with open(arquivo_config, "r") as f:
    config = json.load(f)

# Definir caminho da pasta de origem 
pasta_origem = config["pasta_origem"]  # Ajustar o caminho conforme necessário

# Definir caminho da pasta de destino e nome do arquivo final
pasta_destino = config["pasta_destino"]  # Obter do arquivo de configuração
nome_arquivo_final = config["nome_arquivo_final"]  # Obter do arquivo de configuração

# Definir cabeçalho do arquivo final
header = config["header"]  # Obter do arquivo de configuração

# Configurar log
logging.basicConfig(
    filename="processamento.log",
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)

# Função para validar data
def validar_data(data_str):
    try:
        data_obj = datetime.datetime.strptime(data_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False

# Função para processar primeira linha
def processar_primeira_linha(primeira_linha):
    # Extrair valores da primeira linha e armazená-los em variáveis
    data_movimento = primeira_linha[20:29]
    data_criacao = primeira_linha[28:36]
    codigo_cust = primeira_linha[2:2]

    # Converter valores para os tipos de dados adequados
    data_movimento = datetime.datetime.strptime(data_movimento, "%Y-%m-%d")
    data_criacao = datetime.datetime.strptime(data_criacao, "%Y-%m-%d")
    codigo_cust = int(codigo_cust)

    # Armazenar valores em um dicionário
    dados_primeira_linha = {
        "data_movimento": data_movimento,
        "data_criacao": data_criacao,
        "codigo_cust": codigo_cust
    }

    return dados_primeira_linha

# Função para processar arquivo
def processar_arquivo(nome_arquivo):
    try:
        # Registrar início do processamento
        logging.info(f"Iniciando processamento do arquivo: {nome_arquivo}")

        # Ler arquivo como DataFrame
        df = pd.read_csv(f"{pasta_origem}/{nome_arquivo}", dtype=object)

	# Verificar se o arquivo está vazio
        if df.empty:
            logging.warning(f"Arquivo vazio: {nome_arquivo}")
            return

      	 # Obter conteúdo da primeira linha
        primeira_linha = df.iloc[0, 0]    # df.iloc[0].to_string(index=False)


        # Processar primeira linha
        dados_primeira_linha = processar_primeira_linha(primeira_linha)

        # Validar data do movimento
        if not validar_data(dados_primeira_linha["data_movimento"]):
            logging.error(f"Erro de data no arquivo: {nome_arquivo}")
            return

        # Validar data do movimento (se necessário)
        if config["validar_data"] == "Sim":
            if dados_primeira_linha["data_movimento"] == dados_primeira_linha["data_criacao"]:
                logging.error(f"Erro de data no arquivo: {nome_arquivo}")
                return

        # Filtrar linhas válidas
        df_filtrado = df[~df[0].str.startswith("99")]
        df_filtrado = df_filtrado[~df[0].str.startswith("00")]

        # Ordenar por dois primeiros caracteres
        df_ordenado = df_filtrado.sort_values(by=[0, 1])

        # Gerar linhas do arquivo final
        linhas_arquivo_final = []
        for index, row in df_ordenado.iterrows():
            logging.info(f"Processando linha {index+1}: {row.to_string(index=False)}")
            linha_formatada = row.to_string(index=False, header=False).strip()
            linhas_arquivo_final.append(linha_formatada)

        # Substituir chaves no header e trailer
        header_substituido = header.copy()
        for chave, valor in config["header_substituicoes"].items():
            header_substituido[chave] = valor

        trailer_substituido = config["trailer"].copy()
        for chave, valor in config["trailer_substituicoes"].items():
            trailer_substituido[chave] = valor

        # Gerar conteúdo do arquivo final
        conteudo_arquivo_final = [
            f"00{header_substituido}",
            *linhas_arquivo_final,
            f"99{trailer_substituido}",
        ]

        # Salvar arquivo final
        with open(f"{pasta_destino}/{nome_arquivo_final}", "w") as f:
            f.write("\n".join(conteudo_arquivo_final))

        # Registrar finalização do processamento
        logging.info(f"Processamento do arquivo finalizado: {nome_arquivo}")

    except Exception as e:
        logging.exception(f"Erro durante o processamento do arquivo: {nome_arquivo}")

# Processar cada arquivo na pasta de origem
for nome_arquivo in os.listdir(pasta_origem):
    if nome_arquivo.endswith(tuple(config["extensao_arquivo"])):
        processar_arquivo(nome_arquivo)
