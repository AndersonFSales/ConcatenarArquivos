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

Valida_Data = ""
linhas_arquivo_final = []
MontaArquivoFinal = False

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
def processar_primeira_linha(linhas):
    linha = linhas[0]
    if linha.startswith("00"):
       # Extrair campos
        codigo_cust = linha[2:6]
        data_movimento = linha[20:28]
        data_criacao = linha[28:36]

        # Converter valores para os tipos adequados
        codigo_cust = codigo_cust
        data_movimento = data_movimento[:4] + "-" + data_movimento[4:6] + "-" + data_movimento[6:]
        #data_movimento = datetime.datetime.strptime(data_movimento, "%Y-%m-%d")
        data_criacao = data_criacao[:4] + "-" + data_criacao[4:6] + "-" + data_criacao[6:]
        #data_criacao = datetime.datetime.strptime(data_criacao, "%Y-%m-%d")

        # Criar dicionário com dados
        dados_primeira_linha = {
            "codigo_cust": codigo_cust,
            "data_movimento": data_movimento,
            "data_criacao": data_criacao
        }
        return dados_primeira_linha   

def processar_arquivo(nome_arquivo):
    try:
        # Registrar início do processamento
        logging.info(f"Iniciando processamento do arquivo: {nome_arquivo}")

        # Ler arquivo como string
        with open(f"{pasta_origem}/{nome_arquivo}", "r") as f:
            arquivo_texto = f.read()

        # Dividir o arquivo em linhas
        linhas = arquivo_texto.splitlines()

        dados_primeira_linha = processar_primeira_linha(linhas)

        # Verificar se a primeira linha com "00" foi encontrada
        if dados_primeira_linha is None:
            logging.error(f"Linha com '00' não encontrada no arquivo: {nome_arquivo}")
            return
        
          # Validar data do movimento
        if not validar_data(dados_primeira_linha["data_movimento"]):
            logging.error(f"Erro de data no arquivo: {nome_arquivo}")
            return

        
        # Validar data do movimento (se necessário)
        #print(Valida_Data)
        if Valida_Data == "sim":
            if dados_primeira_linha["data_movimento"] == dados_primeira_linha["data_criacao"]:
                logging.error(f"Erro de data no arquivo: {nome_arquivo}")
                return

        # Filtrar registros válidos (excluindo "00" e "99" no início)
        registros_validos = []
        for linha in linhas:
            if not linha.startswith("00") and not linha.startswith("99"):
                registros_validos.append(linha)

        # Gerar linhas do arquivo final
        #linhas_arquivo_final = []
        for registros in registros_validos:
            # Formatar linha com os campos extraídos
            linhas_arquivo_final.append(registros)

        #Estava aqui
       # MontaArquivoFinal = True

        # Registrar finalização do processamento
        logging.info(f"Processamento do arquivo finalizado: {nome_arquivo}")
        return True
    except Exception as e:
        logging.exception(f"Erro durante o processamento do arquivo: {nome_arquivo}")

def validate_file_in_config(nome_arquivo):
   
    # Verifica se o arquivo está nas configurações
    for file_config in config["arquivos"]:
        Valida_Data = ""
        if file_config["Nome_Arq"] == nome_arquivo:
             Valida_Data = file_config["validardata"]
             return True
        else:
            Valida_Data = ""
            logging.info(f"Arquino não está nas configurações da rotina e não será processado: {nome_arquivo}")
    return False

# Processar cada arquivo na pasta de origem
try:
    for nome_arquivo in os.listdir(pasta_origem):
        if nome_arquivo.endswith(tuple(config["extensao_arquivo"])):
            if validate_file_in_config:
               MontaArquivoFinal = processar_arquivo(nome_arquivo)
    print(MontaArquivoFinal)    
    if MontaArquivoFinal:
        # Substituir chaves no header e trailer
        header_substituido = header
        #for chave, valor in config["header_substituicoes"].items():
        # header_substituido[chave] = valor

        trailer_substituido = config["trailer"]
        #for chave, valor in config["trailer_substituicoes"].items():
        #  trailer_substituido[chave] = valor

        # Gerar conteúdo do arquivo final
        conteudo_arquivo_final = [
            f"00{header_substituido}",
            *linhas_arquivo_final,
            f"99{trailer_substituido}",
        ]

        # Salvar arquivo final
        with open(f"{pasta_destino}/{nome_arquivo_final}", "w") as f:
            f.write("\n".join(conteudo_arquivo_final))
except Exception as e:
    logging.exception(f"Erro durante o processamento do arquivo: {nome_arquivo}")
    