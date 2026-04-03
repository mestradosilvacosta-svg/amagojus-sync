import requests
import json
import os
from datetime import datetime, timedelta
import logging
import time
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configurações
COMUNICA_CNJ_BASE_URL = "https://comunicaapi.pje.jus.br/api/v1/comunicacao"
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

# Headers realistas para parecer um navegador
HEADERS_BROWSER = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
}

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

def fetch_datajud_details(numero_processo):
    """Buscar detalhes de um processo no DataJud"""
    num_limpo = ''.join(filter(str.isdigit, numero_processo))
    cod_api = mapear_endpoint_cnj(numero_processo)
    url = f"{DATAJUD_BASE_URL}api_publica_{cod_api}/_search"
    
    headers = {
        'Authorization': f'ApiKey {DATAJUD_API_KEY}',
        'Content-Type': 'application/json'
    }
    payload = json.dumps({"query": {"match": {"numeroProcesso": num_limpo}}})
    
    try:
        response = requests.post(url, headers=headers, data=payload, timeout=30, verify=False)
        response.raise_for_status()
        
        data = response.json()
        
        if data['hits']['total']['value'] > 0:
            source = data['hits']['hits'][0]['_source']
            movs_crus = source.get('movimentos', [])
            
            if not movs_crus:
                return None, None
            
            movs_validas = [m for m in movs_crus if m.get('dataHora')]
            movs_validas.sort(key=lambda x: x['dataHora'], reverse=True)
            
            if not movs_validas:
                return None, None
            
            latest_mov_data = movs_validas[0]['dataHora'][:10]
            
            andamentos_formatados = []
            for mov in movs_validas[:7]:
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
            
            return latest_mov_data, "||".join(andamentos_formatados)
        
        return None, None
    except Exception as e:
        logging.debug(f"Erro ao buscar DataJud para {numero_processo}: {e}")
        return None, None

def fetch_comunica_cnj_intims_com_retry(start_date, end_date, max_retries=3):
    """Buscar intimações do Comunica CNJ com retry e delay"""
    all_intims = []
    page_number = 1
    total_pages = 1
    
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    
    logging.info(f"Buscando intimações no Comunica CNJ ({start_date_str} até {end_date_str})...")
    
    while page_number <= total_pages:
        retry_count = 0
        success = False
        
        while retry_count < max_retries and not success:
            try:
                params = {
                    'numeroOab': OAB_NUMERO,
                    'ufOab': OAB_UF,
                    'dataDisponibilizacaoInicio': start_date_str,
                    'dataDisponibilizacaoFim': end_date_str,
                    'pagina': page_number,
                    'itensPorPagina': 100
                }
                
                response = requests.get(
                    COMUNICA_CNJ_BASE_URL,
                    params=params,
                    headers=HEADERS_BROWSER,
                    timeout=60,
                    verify=False
                )
                response.raise_for_status()
                
                page_data = response.json()
                
                if not page_data or not page_data.get('items'):
                    logging.info(f"Nenhum item encontrado na página {page_number}. Encerrando busca.")
                    break
                
                all_intims.extend(page_data['items'])
                total_pages = page_data.get('totalPages', 1)
                logging.info(f"Página {page_number}/{total_pages} processada. Total: {len(all_intims)}")
                
                success = True
                page_number += 1
                
                # Delay entre requisições para não sobrecarregar
                if page_number <= total_pages:
                    time.sleep(3)
            
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 403:
                    retry_count += 1
                    if retry_count < max_retries:
                        wait_time = 5 * retry_count
                        logging.warning(f"Erro 403. Tentativa {retry_count}/{max_retries}. Aguardando {wait_time}s...")
                        time.sleep(wait_time)
                    else:
                        logging.error(f"Erro 403 persistente após {max_retries} tentativas. Usando fallback DataJud.")
                        return all_intims
                else:
                    logging.error(f"Erro HTTP {e.response.status_code}: {e}")
                    return all_intims
            
            except Exception as e:
                retry_count += 1
                if retry_count < max_retries:
                    wait_time = 5 * retry_count
                    logging.warning(f"Erro na requisição: {e}. Tentativa {retry_count}/{max_retries}. Aguardando {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logging.error(f"Erro persistente após {max_retries} tentativas: {e}")
                    return all_intims
    
    logging.info(f"Busca Comunica CNJ finalizada. Total de intimações encontradas: {len(all_intims)}")
    return all_intims

def processar_e_enviar():
    """Processar intimações e enviar para SaaS"""
    logging.info(f"\n[!] INICIANDO SINCRONIZAÇÃO - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    # Buscar intimações do Comunica CNJ com retry
    intims_comunica = fetch_comunica_cnj_intims_com_retry(start_date, end_date)
    
    if not intims_comunica:
        logging.warning("Nenhuma intimação encontrada no Comunica CNJ.")
        return
    
    logging.info(f"Total de {len(intims_comunica)} intimações para processar.")
    
    processos = []
    
    for i, intim in enumerate(intims_comunica):
        numero_processo = intim.get('numeroProcesso')
        if not numero_processo:
            continue
        
        logging.info(f"[{i+1}/{len(intims_comunica)}] Processando {numero_processo}...")
        
        data_disponibilizacao = intim.get('data_disponibilizacao', '')
        if data_disponibilizacao:
            try:
                dt_obj = datetime.strptime(data_disponibilizacao[:10], "%Y-%m-%d")
                data_intimacao_formatted = dt_obj.strftime("%Y-%m-%d")
            except ValueError:
                data_intimacao_formatted = data_disponibilizacao[:10]
        else:
            data_intimacao_formatted = ''
        
        # Buscar andamentos no DataJud
        data_mov, andamentos = fetch_datajud_details(numero_processo)
        
        processo = {
            'numero_processo': numero_processo,
            'polo_ativo': intim.get('destinatarios', [{}])[0].get('nome', '') if intim.get('destinatarios') else '',
            'polo_passivo': '',
            'tribunal': intim.get('siglaTribunal', ''),
            'assunto': intim.get('nomeClasse', ''),
            'data_intimacao': data_intimacao_formatted,
            'data_movimentacao': data_mov or '',
            'andamentos_salvos': andamentos or ''
        }
        
        processos.append(processo)
        time.sleep(1)
    
    # Enviar para SaaS
    if processos:
        logging.info(f"\nEnviando {len(processos)} processos para o SaaS...")
        
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
            logging.info(f"\n✅ SUCESSO!")
            logging.info(f"   Inseridos: {result.get('inseridos', 0)}")
            logging.info(f"   Atualizados: {result.get('atualizados', 0)}")
        
        except Exception as e:
            logging.error(f"\n❌ ERRO ao enviar para SaaS: {e}")
    
    logging.info(f"\n[!] SINCRONIZAÇÃO FINALIZADA - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

if __name__ == "__main__":
    processar_e_enviar()
