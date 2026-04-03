import requests
import json
import os
from datetime import datetime
import logging

# Configurações
DATAJUD_BASE_URL = "https://api-publica.datajud.cnj.jus.br/"
DATAJUD_API_KEY = "cDZHYzlZa0JadVREZDJCendQbXY6SkJlTzNjLV9TRENyQk1RdnFKZGRQdw=="

OAB_NUMERO = "11007"
OAB_UF = "MA"

SAAS_URL = os.getenv('SAAS_URL', 'https://amagojus.pythonanywhere.com/api/sync/processos')
SYNC_TOKEN = os.getenv('SYNC_TOKEN', 'amagojus2026')

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%d/%m/%Y %H:%M:%S'
)

def mapear_endpoint_cnj(numero_processo):
    """Mapear número de processo para endpoint correto da API DataJud"""
    num_limpo = ''.join(filter(str.isdigit, numero_processo))
    if len(num_limpo) != 20:
        return "tjma"
    
    justica = num_limpo[13:14]
    tr = num_limpo[14:16]
    
    uf_map = {
        "01": "ac", "02": "al", "03": "ap", "04": "am", "05": "ba", "06": "ce", "07": "dft", "08": "es",
        "09": "go", "10": "ma", "11": "mt", "12": "ms", "13": "mg", "14": "pa", "15": "pb", "16": "pr",
        "17": "pe", "18": "pi", "19": "rj", "20": "rn", "21": "rs", "22": "ro", "23": "rr", "24": "sc",
        "25": "se", "26": "sp", "27": "to"
    }
    
    if justica == '8':
        return f"tj{uf_map.get(tr, 'ma')}"
    elif justica == '4':
        return f"trf{int(tr)}"
    elif justica == '5':
        return f"trt{int(tr)}"
    elif justica == '6':
        return f"tre-{uf_map.get(tr, 'ma')}"
    elif justica == '9':
        return f"tjm{uf_map.get(tr, 'mg')}"
    elif justica == '3':
        return "stj"
    else:
        return "tjma"

def buscar_processos_por_oab(tribunal):
    """Buscar todos os processos de uma OAB em um tribunal específico"""
    all_processes = []
    from_value = 0
    size = 100
    
    url = f"{DATAJUD_BASE_URL}api_publica_{tribunal}/_search"
    
    headers = {
        'Authorization': f'ApiKey {DATAJUD_API_KEY}',
        'Content-Type': 'application/json'
    }
    
    query = {
        "query": {
            "bool": {
                "should": [
                    {"match": {"poloAtivoAdvogado.numeroOab": OAB_NUMERO}},
                    {"match": {"poloPassivoAdvogado.numeroOab": OAB_NUMERO}}
                ]
            }
        },
        "size": size,
        "from": from_value
    }
    
    logging.info(f"Buscando processos da OAB {OAB_NUMERO}/{OAB_UF} no tribunal {tribunal}...")
    
    try:
        while True:
            query["from"] = from_value
            
            response = requests.post(url, json=query, headers=headers, timeout=30, verify=False)
            response.raise_for_status()
            
            data = response.json()
            hits = data.get('hits', {}).get('hits', [])
            
            if not hits:
                logging.info(f"Nenhum processo adicional encontrado. Total: {len(all_processes)}")
                break
            
            all_processes.extend(hits)
            logging.info(f"Página {from_value // size + 1}: {len(hits)} processos. Total: {len(all_processes)}")
            
            if len(hits) < size:
                break
            
            from_value += size
            import time
            time.sleep(1)
    
    except Exception as e:
        logging.error(f"Erro ao buscar processos em {tribunal}: {e}")
    
    return all_processes

def extrair_dados_processo(hit):
    """Extrair dados relevantes de um hit do DataJud"""
    source = hit.get('_source', {})
    
    numero_processo = source.get('numeroProcesso', '')
    
    # Extrair polo ativo
    polo_ativo_list = source.get('poloAtivoAdvogado', [])
    polo_ativo = polo_ativo_list[0].get('nome', '') if polo_ativo_list else ''
    
    # Extrair polo passivo
    polo_passivo_list = source.get('poloPassivoAdvogado', [])
    polo_passivo = polo_passivo_list[0].get('nome', '') if polo_passivo_list else ''
    
    # Extrair tribunal
    tribunal = source.get('tribunal', '')
    
    # Extrair assunto
    assunto = source.get('assunto', '')
    
    # Extrair data de distribuição
    data_distribuicao = source.get('dataDistribuicao', '')
    if data_distribuicao:
        try:
            if 'T' in data_distribuicao:
                dt_obj = datetime.strptime(data_distribuicao[:10], "%Y-%m-%d")
            else:
                dt_obj = datetime.strptime(data_distribuicao[:10], "%Y-%m-%d")
            data_intimacao = dt_obj.strftime("%Y-%m-%d")
        except ValueError:
            data_intimacao = data_distribuicao[:10]
    else:
        data_intimacao = ''
    
    # Extrair últimos andamentos
    movimentos = source.get('movimentos', [])
    data_movimentacao = ''
    andamentos_salvos = ''
    
    if movimentos:
        movimentos_validos = [m for m in movimentos if m.get('dataHora')]
        movimentos_validos.sort(key=lambda x: x['dataHora'], reverse=True)
        
        if movimentos_validos:
            latest_mov = movimentos_validos[0]
            data_movimentacao = latest_mov.get('dataHora', '')[:10]
            
            andamentos_formatados = []
            for mov in movimentos_validos[:7]:
                data_mov = mov.get('dataHora', '')
                nome_mov = mov.get('nome', 'Andamento registrado')
                
                if data_mov:
                    try:
                        if 'T' in data_mov:
                            dt_obj = datetime.strptime(data_mov[:19], "%Y-%m-%dT%H:%M:%S")
                        else:
                            dt_obj = datetime.strptime(data_mov[:14], "%Y%m%d%H%M%S")
                        data_str = dt_obj.strftime("%d/%m/%Y")
                    except ValueError:
                        data_str = data_mov[:10]
                else:
                    data_str = "Data Indisponível"
                
                andamentos_formatados.append(f"{data_str}➔{nome_mov}")
            
            andamentos_salvos = "||".join(andamentos_formatados)
    
    return {
        'numero_processo': numero_processo,
        'polo_ativo': polo_ativo,
        'polo_passivo': polo_passivo,
        'tribunal': tribunal,
        'assunto': assunto,
        'data_intimacao': data_intimacao,
        'data_movimentacao': data_movimentacao,
        'andamentos_salvos': andamentos_salvos
    }

def enviar_para_saas(processos):
    """Enviar processos para o SaaS via API"""
    if not processos:
        logging.info("Nenhum processo para enviar.")
        return
    
    logging.info(f"Enviando {len(processos)} processos para o SaaS...")
    
    headers = {
        'X-Sync-Token': SYNC_TOKEN,
        'Content-Type': 'application/json'
    }
    
    payload = {
        'processos': processos
    }
    
    try:
        response = requests.post(SAAS_URL, json=payload, headers=headers, timeout=60)
        response.raise_for_status()
        
        result = response.json()
        logging.info(f"✅ SUCESSO!")
        logging.info(f"   Inseridos: {result.get('inseridos', 0)}")
        logging.info(f"   Atualizados: {result.get('atualizados', 0)}")
        logging.info(f"   Erros: {result.get('erros', 0)}")
    
    except Exception as e:
        logging.error(f"❌ ERRO ao enviar para SaaS: {e}")

def main():
    logging.info(f"\n[!] INICIANDO SINCRONIZAÇÃO VIA DATAJUD - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Lista de tribunais para buscar (principais)
    tribunais = [
        'tjma',      # Tribunal de Justiça do Maranhão
        'trf1',      # Tribunal Regional Federal 1ª Região
        'trt24',     # Tribunal Regional do Trabalho 24ª Região (Maranhão)
        'tre-ma'     # Tribunal Regional Eleitoral Maranhão
    ]
    
    todos_processos = []
    
    # Buscar em cada tribunal
    for tribunal in tribunais:
        logging.info(f"\n--- Buscando no {tribunal.upper()} ---")
        try:
            processos_hits = buscar_processos_por_oab(tribunal)
            
            if processos_hits:
                for hit in processos_hits:
                    dados = extrair_dados_processo(hit)
                    if dados['numero_processo']:
                        todos_processos.append(dados)
                
                logging.info(f"Total de processos encontrados em {tribunal}: {len(processos_hits)}")
        
        except Exception as e:
            logging.error(f"Erro ao processar tribunal {tribunal}: {e}")
        
        import time
        time.sleep(2)
    
    logging.info(f"\n[!] TOTAL DE PROCESSOS ENCONTRADOS: {len(todos_processos)}")
    
    # Enviar para SaaS
    if todos_processos:
        enviar_para_saas(todos_processos)
    else:
        logging.warning("Nenhum processo encontrado para enviar.")
    
    logging.info(f"\n[!] SINCRONIZAÇÃO FINALIZADA - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

if __name__ == "__main__":
    main()
