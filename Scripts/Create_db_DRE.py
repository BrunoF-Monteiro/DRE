import calendar
from pathlib import Path
from datetime import datetime
import hmac
import hashlib
import json
import dotenv
import numpy as np
import pandas as pd
import requests
from time import time, sleep
from dotenv import load_dotenv
import os

load_dotenv()  # loads enviroment variables
path_dre = str(Path().absolute().parent)

path_cp = path_dre + r'\auxiliar\contas_a_pagar.xlsx'
path_category = path_dre + r'\auxiliar\categorizacao.xlsx'
path_payable_bi = path_dre + r'\BI-payable.csv'
path_isna = path_dre + r'\auxiliar\isna.xlsx'
path_dados_mp = path_dre + r'\BI-dados_mp.csv'
path_tk_shpee = r"C:\Users\meial\Meu Drive\Minidrinks\12. Dev\1. Precificador\Auxiliares\tk_shopee.txt"
path_BI_amazon = path_dre + r'\BI-amazon_db.csv'
path_mp_file = path_dre + r'\auxiliar\MP_file.xlsx'
path_oders_amazon = path_dre + r'\auxiliar\orders_amazon.xlsx'
path_bi_shopee = path_dre + r'\BI-Shopee_db.csv'
bi_file = path_dre + r'\BI-File.csv'
path_paghiper_db = path_dre + r'\BI-PagHiper_db.csv'
bi_cmv = path_dre + r'\BI-CMV_MiniDrinks.csv'


def ids_mktplc(marketplace, year, month_init, month_end):
    """
    Get the orders ids (except the canceled) by marketplace from 'Vendas_API.xlsx' file using the tag fields (MARCADOR 1 & MARCADOR 2) as parameter
    :param month_init: initial month range
    :param month_end: final month range
    :param year: year selection (int)
    :param marketplace: marketplace selection [Shopee, Mercado Livre, Dooca Commerce]
    :return:
    """
    try:
        saidas = pd.read_excel(
            r'C:\Users\meial\Meu Drive\Minidrinks\12. Dev\3. DataBase Pedidos\1. API Tiny\Vendas API.xlsx')
    except FileNotFoundError:
        saidas = pd.read_excel(
            r'G:\Meu Drive\MiniDrinks\12. Dev\3. DataBase Pedidos\1. API Tiny\Vendas API.xlsx')

    ids = pd.DataFrame(saidas, columns=['ID MKTPLC', 'DATA', 'MARCADOR 1', 'MARCADOR 2', 'STATUS'])
    del saidas
    ids.DATA = pd.to_datetime(ids['DATA'], format="%d/%m/%Y")
    intervalo = (ids['DATA'] >= datetime(year=year, month=month_init, day=1)) & \
                (ids['DATA'] <= datetime(year=year, month=month_end,
                                         day=calendar.monthrange(datetime.today().year, month_end)[1]))
    ids = ids[intervalo]
    del intervalo
    ids = ids.loc[(ids['MARCADOR 1'] == f'{marketplace}') | (ids['MARCADOR 2'] == f'{marketplace}')]
    ids = ids.loc[(ids['STATUS'] != 'Cancelado')]  # removendo pedidos cancelados
    ids['ID MKTPLC'].dropna(inplace=True)
    ids = ids[['ID MKTPLC', 'DATA']].drop_duplicates(subset=['ID MKTPLC']).dropna(subset=['ID MKTPLC'], axis=0)
    dates = list(ids['DATA'])
    ids = list(ids['ID MKTPLC'].dropna(axis=0))

    return ids, dates


def tiny_request_basic_contas(token, init_emiss, fim_emiss, pagina=1):
    url = f'https://api.tiny.com.br/api2/contas.pagar.pesquisa.php?token={token}&formato=json&data_ini_emissao={init_emiss}&data_fim_emissao={fim_emiss}&pagina={pagina}'
    resp = requests.get(url).json()
    return resp


def get_payables_tiny(init_emiss, fim_emiss):
    tokens = dict(Tiny_CWB=os.getenv('TINY_CWB'),
                  Tiny_SC=os.getenv('TINY_SC'),
                  Tiny_SP=os.getenv('TINY_SP'))

    id_payable = []
    fornecedor = []
    emiss = []
    venc = []
    valor = []
    historico = []
    categoria = []
    origem_tiny = []
    status = []
    for tkn in tokens:
        basic_payables = tiny_request_basic_contas(token=tokens.get(tkn), init_emiss=init_emiss,
                                                   fim_emiss=fim_emiss)
        if basic_payables['retorno']['status_processamento'] == '2':
            continue
        page = 1

        while page <= basic_payables['retorno']['numero_paginas']:
            basic_payables = tiny_request_basic_contas(token=tokens.get(tkn), init_emiss=init_emiss,
                                                       fim_emiss=fim_emiss, pagina=page)
            print(basic_payables)
            for i in basic_payables['retorno']['contas']:
                id_payable.append(i['conta']['id'])
                emiss.append(i['conta']['data_emissao'])
                venc.append(i['conta']['data_vencimento'])
                valor.append(i['conta']['valor'])
                fornecedor.append(i['conta']['nome_cliente'])
                historico.append(i['conta']['historico'])
                status.append(i['conta']['situacao'])
                origem_tiny.append(tkn)
            page += 1

    for i, tk in zip(id_payable, origem_tiny):
        url = f'https://api.tiny.com.br/api2/conta.pagar.obter.php?token={tokens.get(tk)}&id={i}&formato=json'
        resp = requests.get(url).json()
        print(resp)

        if resp['retorno']['status_processamento'] == '2':
            continue

        if resp['retorno']['status_processamento'] == 1:
            sleep(60)
            resp = requests.get(url).json()

        categoria.append(resp['retorno']['conta']['categoria'])

    df_prev_paybles = pd.read_excel(path_cp)
    df_new_payables = pd.DataFrame(
        {'ID': id_payable, 'FORNECEDOR': fornecedor, 'EMISSAO': emiss, 'VENCIMENTO': venc, 'STATUS': status,
         'VALOR': valor, 'HISTORICO': historico, 'CATEGORIA': categoria, 'TINY': origem_tiny})

    df_new_payables = pd.concat([df_prev_paybles, df_new_payables], axis=0)
    df_new_payables.to_excel(path_cp, index=False)


def create_payable():
    df_contas = pd.read_excel(path_cp)
    df_contas = pd.DataFrame(df_contas)

    # correção da categoria de amortizações
    df_contas['HISTORICO'] = df_contas['HISTORICO'].astype(str)
    df_contas['CATEGORIA'] = np.where(df_contas['HISTORICO'].str.find('Juros') >= 0, 'Juros', df_contas['CATEGORIA'])

    nan = df_contas.loc[df_contas.CATEGORIA.isna()]
    df_contas['EMISSAO'] = pd.to_datetime(df_contas['EMISSAO'], dayfirst=True)
    df_contas.rename({'EMISSAO': 'DATA'}, inplace=True, axis=1)
    # df_contas['MES'] = df_contas.EMISSAO.dt.month
    # df_contas['ANO'] = df_contas.EMISSAO.dt.year
    df_contas.sort_values('DATA', inplace=True)

    df_contas = df_contas.loc[df_contas['CATEGORIA'] != 'Bebidas']
    df_contas = df_contas.loc[df_contas.CATEGORIA.notna()]

    map_category = pd.read_excel(path_category)
    map_category = pd.DataFrame(map_category)

    DRE_contas = pd.merge(df_contas, map_category, 'left', left_on='CATEGORIA', right_on='Categoria')
    DRE_contas = DRE_contas.loc[DRE_contas['COD_DRE'] != 0]
    DRE_contas.VALOR = DRE_contas.VALOR * -1
    DRE_contas.drop(['ID', 'CATEGORIA', 'Categoria', 'Classificação Geral'], inplace=True, axis=1)
    DRE_contas.sort_values(inplace=True, by=['COD_DRE', 'DATA', 'FORNECEDOR'])
    # nan_classificacao = DRE_contas.loc[DRE_contas.Classificação.isna()]

    # DRE_final = DRE_contas.groupby([['Classificação']]).sum()['VALOR']
    DRE_final = DRE_contas.groupby(['DATA', 'COD_DRE']).sum()['VALOR']
    DRE_final = DRE_final.reset_index()

    return DRE_final


# < PAGHIPER ===========
def paghiper_pix_request(ph_key, token, init_date, final_date):
    url = f'https://pix.paghiper.com/invoice/list/'
    body = {'apiKey': f'{ph_key}',
            'token': f'{token}',
            'status': 'paid',
            'initial_date': f'{init_date}',
            'final_date': f'{final_date}',
            'filter_date': 'paid_date',
            'limit': 100}

    header = {"Accept": "application/json",
              "Content-Type": "application/json"}

    response = requests.post(url, json=body, headers=header).json()
    response = response['transaction_list_request']

    fees_list = [x['value_fee_cents'] / 100 for x in response['transaction_list']]
    dates_list = [x['create_date'].split(' ')[0] for x in response['transaction_list']]

    if response['total_page'] > response['current_page']:
        current_page = response['current_page'] + 1

        while current_page <= response['total_page']:
            url = f'https://pix.paghiper.com/invoice/list/'
            body = {'apiKey': f'{ph_key}',
                    'token': f'{token}',
                    'status': 'paid',
                    'initial_date': f'{init_date}',
                    'final_date': f'{final_date}',
                    'filter_date': 'paid_date',
                    'limit': 100,
                    'page': current_page}
            header = {"Accept": "application/json",
                      "Content-Type": "application/json"}

            response = requests.post(url, json=body, headers=header).json()
            response = response['transaction_list_request']

            for i in response['transaction_list']:
                fees_list.append(i['value_fee_cents'] / 100)
                dates_list.append(i['create_date'].split(' ')[0])

            current_page += 1

    return fees_list, dates_list


def paghiper_request(ph_key, token, init_date, final_date):
    url = f'https://api.paghiper.com/transaction/list/'
    body = {'apiKey': f'{ph_key}',
            'token': f'{token}',
            'status': 'paid',
            'initial_date': f'{init_date}',
            'final_date': f'{final_date}',
            'filter_date': 'paid_date',
            'limit': 100}

    header = {"Accept": "application/json",
              "Content-Type": "application/json"}

    response = requests.post(url, json=body, headers=header).json()
    response = response['transaction_list_request']

    fees_list = [x['value_fee_cents'] / 100 for x in response['transaction_list']]
    dates_list = [x['create_date'].split(' ')[0] for x in response['transaction_list']]

    if response['total_page'] > response['current_page']:
        current_page = response['current_page'] + 1

        while current_page <= response['total_page']:
            url = f'https://api.paghiper.com/transaction/list/'
            body = {'apiKey': f'{ph_key}',
                    'token': f'{token}',
                    'status': 'paid',
                    'initial_date': f'{init_date}',
                    'final_date': f'{final_date}',
                    'filter_date': 'paid_date',
                    'limit': 100,
                    'page': current_page}
            header = {"Accept": "application/json",
                      "Content-Type": "application/json"}

            response = requests.post(url, json=body, headers=header).json()
            response = response['transaction_list_request']

            for i in response['transaction_list']:
                fees_list.append(i['value_fee_cents'] / 100)
                dates_list.append(i['create_date'].split(' ')[0])

            current_page += 1

    return fees_list, dates_list


def get_transactions_paghiper(init_date, final_date):
    """
    valores dados em centados. Necessário dividir por 100 para valores em reais
    date format: YYYY-MM-DD
    """
    ph_pix_fees, ph_pix_dates = paghiper_pix_request(init_date=init_date, final_date=final_date,
                                                     ph_key=os.getenv('ph_key'),
                                                     token=os.getenv('ph_token'))

    ph_general_fees, ph_general_dates = paghiper_request(init_date=init_date, final_date=final_date,
                                                         ph_key=os.getenv('ph_key'),
                                                         token=os.getenv('ph_token'))

    ph_dates = ph_general_dates + ph_pix_dates
    ph_fees = ph_general_fees + ph_pix_fees

    df_paghiper = pd.DataFrame({'Data': ph_dates, 'Taxas': ph_fees})
    df_paghiper['Data'] = pd.to_datetime(df_paghiper['Data'])
    df_paghiper['ANO'] = df_paghiper.Data.dt.year
    df_paghiper['MES'] = df_paghiper.Data.dt.month
    df_paghiper_prev = pd.read_csv(path_paghiper_db)

    df_paghiper = pd.concat([df_paghiper_prev, df_paghiper], axis=0)

    return df_paghiper


# < MERCADO PAGO ===========
def get_mp_auth(code):
    response = requests.post(r'https://api.mercadopago.com/oauth/token',
                             headers={'Content-Type': 'application/json'},
                             json={"client_secret": os.getenv('CLIENT_SECRET_MP'),
                                   "client_id": os.getenv('CLIENT_ID_MP'),
                                   "grant_type": "authorization_code", "code": f"{code}",
                                   "redirect_uri": r"https://www.minidrinks.com.br"}).json()
    print(response)
    os.environ['TKN_ML'] = response['access_token']
    dotenv.set_key(key_to_set='ACCESS_TOKEN_MP', value_to_set=os.environ['ACCESS_TOKEN_MP'],
                   dotenv_path=str(Path().absolute()) + r'\1. DRE\Scripts\.env')
    return response


def mercadopago_request(ids_mp):
    mp_fee = []
    ml_fee = []
    shipping = []
    date = []
    ids = []
    for i in ids_mp:
        sleep(0.8)
        resp_mp = requests.get(
            f'https://api.mercadopago.com/v1/payments/{i}',
            headers={'Authorization': f'Bearer {os.getenv("ACCESS_TOKEN_MP")}'})
        print(i, resp_mp.json())

        if resp_mp.status_code != 200:
            continue
        else:
            resp_mp = resp_mp.json()

        date.append(resp_mp['date_created'].split('T')[0])

        if not resp_mp['charges_details']:
            ids.append(i)
            mp_fee.append('x')
            ml_fee.append('x')
            shipping.append('x')
            continue

        for charge in resp_mp['charges_details']:

            if charge.get('name') == 'ml_fee' or charge.get('name') == 'meli_fee':
                ml_fee.append(charge['amounts']['original'])

            elif charge.get('name') == 'mp_fee':
                mp_fee.append(charge['amounts']['original'])

            elif charge.get('name') == 'shp_cross_docking' or charge.get('name') == 'shp_fulfillment':
                shipping.append(charge['amounts']['original'])

            else:
                mp_fee.append('erro não esperado')
                ml_fee.append('erro não esperado')
                shipping.append('erro não esperado')
                break

        if not len(shipping) == len(mp_fee) or not len(shipping) == len(ml_fee):
            if len(ml_fee) == len(mp_fee) and len(ml_fee) > len(shipping):
                shipping.append(0)

            elif len(ml_fee) == len(mp_fee) and len(ml_fee) < len(shipping):
                ml_fee.append(0)
                mp_fee.append(0)

            elif len(shipping) == len(mp_fee) and len(shipping) > len(ml_fee):
                ml_fee.append(0)

            elif len(shipping) == len(mp_fee) and len(shipping) < len(ml_fee):
                mp_fee.append(0)
                shipping.append(0)

            else:
                mp_fee.append(0)

        if not len(shipping) == len(mp_fee) or not len(shipping) == len(ml_fee):
            print('')

        ids.append(i)
        if not len(ids) == len(mp_fee):
            print()
    fees = [a + b for a, b in zip(ml_fee, mp_fee)]

    return ids, date, fees, shipping


# < MERCADO LIVRE ===========
def format_meli_file():
    mapping = pd.read_excel(path_category)
    mp_file = pd.read_excel(path_mp_file)
    mp_final = pd.merge(mp_file, mapping, 'left', left_on='Tipo de operação', right_on='Categoria')

    is_na = mp_final.loc[mp_final['Classificação'].isna()].drop_duplicates()
    if len(is_na) > 0:
        is_na.to_excel(path_isna, index=False)

    mp_final = mp_final.loc[mp_final['Classificação'] != 'Balanço']
    mp_final.rename({'Data de pagamento': 'DATA', 'Valor': 'VALOR'}, axis=1, inplace=True)
    mp_final = mp_final[['DATA', 'VALOR', 'COD_DRE']]

    mp_final['DATA'] = mp_final['DATA'].apply(lambda x: x.split('T')[0])
    mp_final['DATA'] = pd.to_datetime(mp_final['DATA'], format="%Y/%m/%d", dayfirst=True)
    mp_final['MES'] = mp_final.DATA.dt.month
    mp_final['ANO'] = mp_final.DATA.dt.year
    mp_final.sort_values(['MES', 'VALOR'], inplace=True)

    mp_final.to_csv(path_dados_mp, index=False)

    return mp_final


def request_token_ml(code):
    header = {'accept': 'application/json',
              'content-type': 'application/x-www-form-urlencoded'}

    body = {'grant_type': 'authorization_code',
            'client_id': os.getenv('CLIENT_ID_ML'),
            'client_secret': os.getenv('CLIENT_SECRET_ML'),
            'code': f'{code}',
            'redirect_uri': 'https://www.minidrinks.com.br'}

    response = requests.post(f'https://api.mercadolibre.com/oauth/token', headers=header, params=body)
    print(response.json())
    return response.json()


def mercadolivre_refresh():
    refresh = requests.post('https://api.mercadolibre.com/oauth/token?'
                            'grant_type=refresh_token'
                            f'&client_id={os.getenv("CLIENT_ID_ML")}'
                            f'&client_secret={os.getenv("CLIENT_SECRET_ML")}'
                            f'&refresh_token={os.getenv("REFRESH_TK_ML")}')
    refresh = refresh.json()
    print(refresh)
    token_ml = refresh['access_token']
    os.environ['TKN_ML'] = token_ml
    dotenv.set_key(key_to_set='TKN_ML', value_to_set=os.environ['TKN_ML'],
                   dotenv_path=str(Path().absolute()) + r'\1. DRE\Scripts\.env')
    return token_ml


def mercadolivre_request_orders():
    ids_ml = ids_mktplc("Mercado Livre", 2022, 9, 9)
    # ids_mlfull = ids_mktplc("Fulfillment", 2022, 1, 9)
    # ids = ids_ml + ids_mlfull
    tkn = os.getenv('TKN_ML')
    erros = 0
    ml_fees = []
    ml_date = []
    for i in ids_ml:
        # sleep(0.5)
        resp_ml = requests.get(fr'https://api.mercadolibre.com/orders/{i}',
                               headers={'Authorization': f'Bearer {tkn}'})
        print(i, resp_ml.text)
        if resp_ml.status_code == 401:
            tkn = mercadolivre_refresh()
            resp_ml = requests.get(fr'https://api.mercadolibre.com/orders/{i}',
                                   headers={'Authorization': f'Bearer {tkn}'})
        if not resp_ml.status_code == 200:
            erros += 1
            ml_fees.append('')
            ml_date.append('')
            continue
        resp_ml = resp_ml.json()
        ml_date.append(resp_ml['date_created'])
        ml_fees.append(sum([x['sale_fee'] for x in resp_ml['order_items']]))

    return resp_ml


def request_orders_ml2(emiss_init, emiss_fim):
    """
    date format: ISO-8601 (e.g. 2022-01-01T10:01:50.000-04:00)
    orders requests without ID param
    """
    tkn = os.getenv('TKN_ML')
    dates = []
    fees = []
    ids_mp = []
    emiss_init = emiss_init + 'T00:00:01.000-04:00'
    emiss_fim += 'T23:59:50.000-04:00'

    resp = requests.get(
        fr'https://api.mercadolibre.com/orders/search?seller=196636614&order.date_created.from={emiss_init}&order.date_created.to={emiss_fim}',
        headers={'Authorization': f'Bearer {tkn}'})

    if resp.status_code == 401:
        tkn = mercadolivre_refresh()
        resp = requests.get(
            fr'https://api.mercadolibre.com/orders/search?seller=196636614&order.date_created.from={emiss_init}&order.date_created.to={emiss_fim}',
            headers={'Authorization': f'Bearer {tkn}'})

    elif resp.status_code != 200:
        print(resp.json())
        exit()

    resp = resp.json()
    for sale in resp['results']:
        if sale['payments'][0]['status'] != 'approved':
            continue

        for payment in sale['payments']:
            ids_mp.append(payment['id'])
            dates.append(sale['date_created'])
            fees.append(round(sum([item['sale_fee'] * item['quantity'] for item in sale['order_items']]), 2))

    if resp['paging']['total'] > 51:
        offset = 51
        while resp['paging']['total'] > resp['paging']['offset']:
            resp = requests.get(
                fr'https://api.mercadolibre.com/orders/search?seller=196636614&order.date_created.from={emiss_init}&order.date_created.to={emiss_fim}&offset={offset}',
                headers={'Authorization': f'Bearer {tkn}'})

            if resp.status_code != 200:
                print(resp.json())
                exit()

            resp = resp.json()
            for sale in resp['results']:
                if sale['payments'][0]['status'] != 'approved':
                    continue

                for payment in sale['payments']:
                    ids_mp.append(payment['id'])
                    dates.append(sale['date_created'])
                    fees.append(round(sum([item['sale_fee'] * item['quantity'] for item in sale['order_items']]), 2))

            offset += 51

    ids, date, fees, shipping = mercadopago_request(ids_mp)

    df_meli = pd.DataFrame({'IDS_MP': ids, 'Data': date, 'Taxas': fees, 'Frete': shipping})
    df_meli = df_meli.loc[df_meli.Frete != 'x']
    df_meli['Data'] = pd.to_datetime(df_meli['Data'], format="%d/%m/%Y")
    df_meli['DIA'] = df_meli.Data.dt.day
    df_meli['MES'] = df_meli.Data.dt.month
    df_meli['ANO'] = df_meli.Data.dt.year

    df_meli['Frete'] = df_meli['Frete'].astype(float)

    return df_meli


# < SHOPEE ===========
def save_tkn(response):
    with open(path_tk_shpee, 'w') as DataFileShopee:
        json.dump(response, DataFileShopee)


def get_tkn_shopee(code):
    shop_id = int(os.getenv('SHOP_ID_SHOPEE'))
    partner_id = int(os.getenv('PARTNER_ID_SHOPEE'))
    key = os.getenv('KEY_SHOPEE')

    timest = int(time())
    host = "https://partner.shopeemobile.com"
    path = "/api/v2/auth/token/get"
    body = {"code": code, "shop_id": shop_id, "partner_id": partner_id}
    tmp_base_string = "%s%s%s" % (partner_id, path, timest)
    base_string = tmp_base_string.encode()
    partner_key = key.encode()
    sign = hmac.new(partner_key, base_string, hashlib.sha256).hexdigest()
    url = host + path + "?partner_id=%s&timestamp=%s&sign=%s" % (partner_id, timest, sign)
    headers = {"Content-Type": "application/json"}
    response = requests.post(url, json=body, headers=headers)

    if response.status_code == 200:
        save_tkn(response.json())

    return response.json()


def token_shopee_local():
    try:
        tk_shopee = \
            json.load(
                open(r'C:\Users\meial\Meu Drive\Minidrinks\12. Dev\1. Precificador\Auxiliares\tk_shopee.txt', 'r'))[
                'access_token']
    except FileNotFoundError:
        tk_shopee = json.load(open(r'G:\Meu Drive\Minidrinks\12. Dev\1. Precificador\Auxiliares\tk_shopee.txt', 'r'))[
            'access_token']

    try:
        refresh_tk_shopee = \
            json.load(
                open(r'C:\Users\meial\Meu Drive\Minidrinks\12. Dev\1. Precificador\Auxiliares\tk_shopee.txt', 'r'))[
                'refresh_token']
    except FileNotFoundError:
        refresh_tk_shopee = \
            json.load(open(r'G:\Meu Drive\Minidrinks\12. Dev\1. Precificador\Auxiliares\tk_shopee.txt', 'r'))[
                'refresh_token']

    return tk_shopee, refresh_tk_shopee


def refresh_token_shopee(refresh_shopee=token_shopee_local()[1]):
    shop_id = int(os.getenv('SHOP_ID_SHOPEE'))
    partner_id = int(os.getenv('PARTNER_ID_SHOPEE'))
    key = os.getenv('KEY_SHOPEE')
    timest = int(time())

    host = 'https://partner.shopeemobile.com'
    path = "/api/v2/auth/access_token/get"
    base_string = "%s%s%s" % (partner_id, path, timest)
    sign = hmac.new(key.encode(), base_string.encode(), hashlib.sha256).hexdigest()

    query = f'?partner_id={partner_id}&timestamp={timest}&sign={sign}'
    endpoint = host + path + query

    header = {'Content-Type': 'application/json'}
    body = {'shop_id': shop_id, 'refresh_token': refresh_shopee, 'partner_id': partner_id}

    response = requests.post(endpoint, json=body, headers=header).json()
    print(response)

    if response.status_code == 200:
        save_tkn(response)

    return response['access_token']


def shopee_request_commission(year, month_init, month_end):
    commission_shopee = []
    ids_shopee, dates = ids_mktplc('Shopee', year=year, month_init=month_init, month_end=month_end)
    shop_id = int(os.getenv('SHOP_ID_SHOPEE'))
    partner_id = int(os.getenv('PARTNER_ID_SHOPEE'))
    key = os.getenv('KEY_SHOPEE')
    token = token_shopee_local()[0]
    path = "/api/v2/payment/get_escrow_detail"

    for i in ids_shopee:

        timest = int(time())
        base_string = "%s%s%s%s%s" % (partner_id, path, timest, token, shop_id)
        sign = hmac.new(key.encode(), base_string.encode(), hashlib.sha256).hexdigest()
        url = r'https://partner.shopeemobile.com' + path
        query = f'?partner_id={partner_id}&shop_id={shop_id}&timestamp={timest}' \
                f'&access_token={token}' \
                f"&sign={sign}&order_sn={i}"
        resp = requests.get(url + query)

        if resp.status_code == 403:
            token = refresh_token_shopee()
            timest = int(time())
            base_string = "%s%s%s%s%s" % (partner_id, path, timest, token, shop_id)
            sign = hmac.new(key.encode(), base_string.encode(), hashlib.sha256).hexdigest()
            query = f'?partner_id={partner_id}&shop_id={shop_id}&timestamp={timest}' \
                    f'&access_token={token}' \
                    f"&sign={sign}&order_sn={i}"
            resp = requests.get(url + query)

        if resp.status_code != 200:
            print(i, resp.json())
            exit()

        resp = resp.json()
        print(resp)
        commission_shopee.append(resp['response']['order_income']['commission_fee'])

    df_shopee = pd.DataFrame({'DATA': dates, 'ID_SHOPEE': ids_shopee, 'TAXAS': commission_shopee})
    df_shopee['DIA'] = df_shopee.DATA.dt.day
    df_shopee['MES'] = df_shopee.DATA.dt.month
    df_shopee['ANO'] = df_shopee.DATA.dt.year

    BI_Shopee_db = pd.read_csv(path_bi_shopee)
    for i in df_shopee.ID_SHOPEE:
        if i in BI_Shopee_db.ID_SHOPEE.values:
            print(f'O ID_shopee {i} já existe na base de dados e não será lançado.')
            df_shopee = df_shopee.loc[df_shopee.ID_SHOPEE != i]

    df_shopee = pd.concat([BI_Shopee_db, df_shopee], axis=0)

    return df_shopee


# < AMAZON
def format_amazon_file():
    amz = pd.read_excel(path_oders_amazon, skiprows=6, decimal=',')

    amz['data/hora'] = amz['data/hora'].astype(str).apply(lambda x: x.split('2022')[0] + '2022')
    amz['data/hora'] = amz['data/hora'].astype(str).apply(lambda x: x.replace('de', '-'))
    amz['data/hora'] = amz['data/hora'].astype(str).apply(lambda x: x.replace('.', ''))
    amz['data/hora'] = amz['data/hora'].astype(str).apply(lambda x: x.replace(' ', ''))
    # amz['data/hora'] = amz['data/hora'].astype(str).apply(lambda x: x.split('-', 1)[1])
    months = {'jan': 1, 'fev': 2, 'mar': 3, 'abr': 4, 'mai': 5, 'jun': 6, 'jul': 7, 'ago': 8, 'set': 9, 'out': 10,
              'nov': 11, 'dez': 12}
    amz['data/hora'] = amz['data/hora'].astype(str).apply(
        lambda x: x.split('-')[0] + '-' + str(months.get(x.split('-')[1])) + '-' + x.split('-')[2])

    fees = amz.groupby(['data/hora']).sum()['tarifas de venda']
    amz = amz.loc[(amz.tipo != "Pedido") & (amz.tipo != "Transferir") & (amz.tipo != "Reembolso") & (
            amz.tipo != "Solicitação de Garantia de A a Z")]
    y = amz.groupby(['data/hora', 'tipo']).sum()['total']
    y = y.reset_index()
    fees = fees.reset_index()
    fees['tipo'] = 'tarifa de venda'
    fees.rename({'tarifas de venda': 'total'}, inplace=True, axis=1)
    df_amz = pd.concat([fees, y])

    df_amz.rename({'data/hora': 'DATA'}, axis=1, inplace=True)
    df_amz['DATA'] = pd.to_datetime(df_amz['DATA'], format='%d-%m-%Y')
    df_amz.sort_values('DATA', inplace=True)

    map_category = pd.read_excel(path_category)
    BI_amazon_db = pd.merge(df_amz, map_category[['Categoria', 'COD_DRE']], 'left', left_on='tipo',
                            right_on='Categoria')
    BI_amazon_db.drop(['tipo', 'Categoria'], axis=1).to_csv(path_BI_amazon, index=False)

    return BI_amazon_db


def get_tkn_amz(refresh_tk_app):
    client_id = os.getenv('CLIENT_ID_AMZ')
    client_secret = os.getenv('CLIENT_SECRET_AMZ')
    url = fr'https://api.amazon.com/auth/o2/token?grant_type=refresh_token&refresh_token={refresh_tk_app}&client_id={client_id}&client_secret={client_secret}'

    resp = requests.post(url=url, headers={"Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"}).json()
    tk_amz = resp['access_token']
    refresh_new = resp['refresh_token']
    print(resp)
    return tk_amz, refresh_new


def get_access_amz():
    canonical_uri = 'GET\nhttps://sellingpartnerapi-na.amazon.com/orders/v0/orders\n?MarketplaceIds=A2Q3Y263D00KWC'
    canonical_header = ''
    tk = 'Atza|IwEBIPMt5c2d0SMKoRQH2GJRNE0PTwcAgRnvn29GnyF1q0GUpS1T9fS275KVncvSM8waN8jBqtczgfQKo7HcYbWJo1aqoyFe3uObcQ8xGXgyfGHM4-88zTT19jPV7rDcZomzno5cHkgRc34-scglM_tXoNOU8OUgPXLRgHpTIFCYB6_1DiMnis4v9ZL-rttYfxU3HSzxS8_YEBX53m6orwNauI8KwUFiMV2O6H3LsmBnxs7jg9A2p-bISzQKu6XwXIX6YTnH3FO5ru1AhxdMd-bwNg7l-kBkOwl8vr05Vmjm5f6HTx7yr3RJlThPdPXSdg5GhXAwvnC71nEUhlqgP5v8HJwN'
    resp = requests.get(r'https://sellingpartnerapi-na.amazon.com/orders/v0/orders?MarketplaceIds=A2Q3Y263D00KWC',
                        headers={'Authorization': '',
                                 'host': 'https://sellingpartnerapi-na.amazon.com',
                                 'x-amz-access-token': str(tk),
                                 'user-agent': 'DRE/1.0 (Language=Python/3.9.13; Platform=Windows/10)',
                                 'x-amz-date': datetime.now().strftime('%Y%m%dT%H%M%ST'),
                                 'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8'})


# ==== CREATING DATABASES =======
def date_range():
    today = datetime.today()
    if today.month == 1:
        month = 12
        year = today.year - 1
    else:
        month = today.month - 1
        year = today.year

    init = f'01/{month}/{year}'
    final = f'{calendar.monthrange(year, month)[1]}/{month}/{year}'
    init = datetime.strptime(init, '%d/%m/%Y')
    final = datetime.strptime(final, '%d/%m/%Y')
    return init, final


def create_final_databse():
    # init_date, final_date = date_range()
    # < Payable
    # get_payables_tiny(init_emiss=init_date.strftime('%d/%m/%Y'), fim_emiss=final_date.strftime('%d/%m/%Y'))
    # get_payables_tiny(init_emiss='01/09/2022', fim_emiss='31/10/2022')
    df_payables = create_payable()
    df_payables.to_csv(path_payable_bi, index=False)

    # < Shopee
    # df_shopee = shopee_request_commission(year=init_date.year, month_init=init_date.month, month_end=final_date.month)
    # df_shopee.to_csv(path_bi_shopee, index=False)

    # < PagHiper
    # df_paghiper = get_transactions_paghiper(init_date=init_date.strftime('%Y-%m-%d'), final_date=final_date.strftime('%Y-%m-%d'))
    # df_paghiper.to_csv(path_paghiper_db, index=False)

    # < Amazon
    df_amz = format_amazon_file()
    df_amz.to_csv(path_BI_amazon, index=False)

    # < Mercado Livre
    # df_mercado_l = format_meli_file()
    # df_mercado_l.to_csv(path_dados_mp, index=False)


def concat_dbs():
    CMV = pd.read_csv(bi_cmv, delimiter=',', decimal='.')
    amazon = pd.read_csv(path_BI_amazon, delimiter=',', decimal='.')
    paghiper = pd.read_csv(path_paghiper_db, delimiter=',', decimal='.')
    mercado_livre = pd.read_csv(path_dados_mp, delimiter=',', decimal='.')
    shopee = pd.read_csv(path_bi_shopee, delimiter=',', decimal='.')
    payable = pd.read_csv(path_payable_bi, delimiter=',', decimal='.')
    orders = pd.read_excel(
        r'C:\Users\meial\Meu Drive\Minidrinks\12. Dev\3. DataBase Pedidos\1. API Tiny\Vendas API.xlsx',
        usecols=['DATA', 'SKU', 'FRETE', 'RECEITA', 'DESCONTOS', 'STATUS'])

    intervalo = (orders['DATA']
                 >= datetime(year=2022, month=1, day=1)
                 ) & \
                (orders['DATA']
                 <= datetime(year=2022, month=10, day=calendar.monthrange(datetime.today().year, datetime.today().month - 1)[1])
                 )

    # CMV
    CMV = CMV.groupby(['DATA']).sum()['CMV']
    CMV = CMV.reset_index()
    CMV.rename({'CMV': 'VALOR'}, axis=1, inplace=True)
    CMV['COD_DRE'] = 10

    # AMAZON
    amazon = amazon.groupby(['DATA', 'COD_DRE']).sum()['total']
    amazon = amazon.reset_index()
    amazon.rename({'total': 'VALOR'}, axis=1, inplace=True)

    # PAGHIPER
    paghiper.rename({'Data': 'DATA'}, axis=1, inplace=True)
    paghiper = paghiper.groupby(['DATA']).sum()['Taxas'] * -1
    paghiper = paghiper.reset_index()
    paghiper.rename({'Taxas': 'VALOR'}, axis=1, inplace=True)
    paghiper['COD_DRE'] = 13

    # MERCADO LIVRE
    mercado_livre = mercado_livre.groupby(['DATA', 'COD_DRE']).sum()['VALOR']
    mercado_livre = mercado_livre.reset_index()

    # SHOPEE
    shopee = shopee.groupby(['DATA']).sum()['TAXAS'] * -1
    shopee = shopee.reset_index()
    shopee.rename({'TAXAS': 'VALOR'}, axis=1, inplace=True)
    shopee['COD_DRE'] = 13

    # PAYABLE
    payable = payable.groupby(['DATA', 'COD_DRE']).sum()['VALOR']
    payable = payable.reset_index()

    # ORDERS
    orders = orders[intervalo]
    orders.drop(['SKU'], inplace=True, axis=1), orders.rename({'RECEITA': 'VALOR'}, inplace=True, axis=1)

    receita, frete, descontos = orders[['DATA', 'VALOR', 'STATUS']], orders[['DATA', 'FRETE', 'STATUS']], orders[['DATA', 'DESCONTOS', 'STATUS']]
    cancelados = orders.loc[orders.STATUS == 'Cancelado']

    frete['COD_DRE'] = 3
    receita['COD_DRE'] = 2
    descontos['COD_DRE'] = 6
    cancelados['COD_DRE'] = 7

    frete.rename({'FRETE': 'VALOR'}, inplace=True, axis=1)
    descontos.rename({'DESCONTOS': 'VALOR'}, inplace=True, axis=1)

    cancelados['VALOR'] = cancelados['VALOR'] * -1
    frete.VALOR = np.where(frete.STATUS == 'Cancelado', frete.VALOR * -1, frete.VALOR)
    descontos.VALOR = np.where(descontos.STATUS == 'Cancelado', 0, descontos.VALOR * -1)

    frete = frete.groupby(['DATA', 'COD_DRE']).sum()['VALOR'].reset_index()
    receita = receita.groupby(['DATA', 'COD_DRE']).sum()['VALOR'].reset_index()
    cancelados = cancelados.groupby(['DATA', 'COD_DRE']).sum()['VALOR'].reset_index()

    # CONCAT
    BI_File = pd.concat([payable, shopee, mercado_livre, amazon, CMV, paghiper, frete, receita, cancelados, descontos], axis=0)
    BI_File['DATA'] = BI_File['DATA'].astype(str)
    BI_File['DATA'] = BI_File['DATA'].str[:11]
    BI_File.DATA = pd.to_datetime(BI_File['DATA'])
    BI_File = BI_File.groupby(['DATA', 'COD_DRE']).sum()['VALOR']
    BI_File = BI_File.reset_index()
    print('')

    return BI_File


create_final_databse()
BI = concat_dbs()
BI.to_csv(bi_file, index=False)
print('caboce')
