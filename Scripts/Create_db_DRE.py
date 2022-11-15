import calendar
from pathlib import Path
from datetime import datetime
import hmac
import hashlib
import json
import dotenv
import pandas as pd
import requests
from time import time, sleep
from dotenv import load_dotenv
import os

load_dotenv()  # loads enviroment variables
path_cp = r'..\auxiliar\contas_a_pagar.xlsx'
path_category = r'..\auxiliar\categorizacao.xlsx'


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


def get_payables_tiny():
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
        basic_payables = tiny_request_basic_contas(token=tokens.get(tkn), init_emiss='01/01/2022',
                                                   fim_emiss='31/08/2022')
        if basic_payables['retorno']['status_processamento'] == '2':
            continue
        page = 1

        while page <= basic_payables['retorno']['numero_paginas']:
            basic_payables = tiny_request_basic_contas(token=tokens.get(tkn), init_emiss='01/01/2022',
                                                       fim_emiss='30/09/2022', pagina=page)
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

    df = pd.DataFrame(
        {'ID': id_payable, 'FORNECEDOR': fornecedor, 'EMISSAO': emiss, 'VENCIMENTO': venc, 'STATUS': status,
         'VALOR': valor, 'HISTORICO': historico, 'CATEGORIA': categoria, 'TINY': origem_tiny})
    df.to_excel(r'G:\Meu Drive\Minidrinks\12. Dev\6. Financeiro\1. DRE\auxiliar\contas_a_pagar.xlsx', index=False)


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
    with open(r"C:\Users\meial\Meu Drive\Minidrinks\12. Dev\1. Precificador\Auxiliares\tk_shopee.txt",
              'w') as DataFileShopee:
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

    BI_Shopee_db = pd.read_csv(r'..\BI-Shopee_db.csv')

    pd.concat([BI_Shopee_db, df_shopee], axis=0).to_csv(r'..\BI-Shopee_db.csv', index=False)

    return df_shopee


# < AMAZON
def get_amazon_file():
    amz = pd.read_csv(r'..\auxiliar\orders_amazon.csv', delimiter=',', skiprows=6, decimal=',', thousands='.')

    amz['data/hora'] = amz['data/hora'].astype(str).apply(lambda x: x.split('2022')[0] + '2022')
    amz['data/hora'] = amz['data/hora'].astype(str).apply(lambda x: x.replace('de', '-'))
    amz['data/hora'] = amz['data/hora'].astype(str).apply(lambda x: x.replace('.', ''))
    amz['data/hora'] = amz['data/hora'].astype(str).apply(lambda x: x.replace(' ', ''))
    amz['data/hora'] = amz['data/hora'].astype(str).apply(lambda x: x.split('-', 1)[1])
    months = {'jan': 1, 'fev': 2, 'mar': 3, 'abr': 4, 'mai': 5, 'jun': 6, 'jul': 7, 'ago': 8, 'set': 9, 'out': 10,
              'nov': 11, 'dez': 12}
    amz['data/hora'] = amz['data/hora'].astype(str).apply(
        lambda x: str(months.get(x.split('-')[0])) + '-' + x.split('-')[1])

    fees = amz.groupby(['data/hora']).sum()['tarifas de venda']
    amz = amz.loc[(amz.tipo != "Pedido") & (amz.tipo != "Transferir") & (amz.tipo != "Reembolso") & (
            amz.tipo != "Solicitação de Garantia de A a Z")]
    y = amz.groupby(['data/hora', 'tipo']).sum()['total']
    y = y.reset_index()
    fees = fees.reset_index()
    fees['tipo'] = 'tarifa de venda'
    fees.rename({'tarifas de venda': 'total'}, inplace=True, axis=1)
    df_amz = pd.concat([fees, y])
    df_amz.sort_values('data/hora', inplace=True)
    df_amz['MES'] = df_amz['data/hora'].apply(lambda x: x.split('-')[0])
    df_amz['ANO'] = df_amz['data/hora'].apply(lambda x: x.split('-')[1])

    map_category = pd.read_excel(path_category)
    BI_amazon_db = pd.merge(df_amz, map_category[['Categoria', 'COD_DRE']], 'left', left_on='tipo',
                            right_on='Categoria')
    BI_amazon_db.drop(['tipo', 'Categoria', 'data/hora'], axis=1).to_csv(r'..\BI-amazon_db.csv', index=False)

    return df_amz


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


def concat_dbs():
    CMV = pd.read_csv(r'..\BI-CMV_MiniDrinks.csv', delimiter=',', decimal='.')
    amazon = pd.read_csv(r'..\BI-amazon_db.csv', delimiter=',', decimal='.')
    paghiper = pd.read_csv(r'..\BI-PagHiper_db.csv', delimiter=',', decimal='.')
    mercado_livre = pd.read_csv(r'..\BI-dados_mp.csv', delimiter=',', decimal='.')
    shopee = pd.read_csv(r'..\BI-Shopee_db.csv', delimiter=',', decimal='.')
    payable = pd.read_csv(r'..\BI-payable.csv', delimiter=',', decimal='.')
    orders = pd.read_excel(
        r'C:\Users\meial\Meu Drive\Minidrinks\12. Dev\3. DataBase Pedidos\1. API Tiny\Vendas API.xlsx',
        usecols=['DATA', 'SKU', 'FRETE', 'RECEITA'])

    # CMV
    CMV = CMV.groupby(['ANO', 'MES']).sum()['CMV']
    CMV = CMV.reset_index()
    CMV.rename({'CMV': 'VALOR'}, axis=1, inplace=True)
    CMV['COD_DRE'] = 10

    # AMAZON
    amazon = amazon.groupby(['ANO', 'MES', 'COD_DRE']).sum()['total']
    amazon = amazon.reset_index()
    amazon.rename({'total': 'VALOR'}, axis=1, inplace=True)

    # PAGHIPER
    paghiper = paghiper.groupby(['ANO', 'MES']).sum()['Taxas'] * -1
    paghiper = paghiper.reset_index()
    paghiper.rename({'Taxas': 'VALOR'}, axis=1, inplace=True)
    paghiper['COD_DRE'] = 13

    # MERCADO LIVRE
    mercado_livre = mercado_livre.loc[mercado_livre['Frete'] != 'erro não esperado']
    mercado_livre.Taxas = mercado_livre.Taxas.astype(float)
    ml_fees = mercado_livre.groupby(['ANO', 'MES']).sum()['Taxas'] * -1
    ml_fees = ml_fees.reset_index()
    ml_fees.rename({'Taxas': 'VALOR'}, axis=1, inplace=True)
    ml_fees['COD_DRE'] = 13

    mercado_livre.Frete = mercado_livre.Frete.astype(float)
    ml_frete = mercado_livre.groupby(['ANO', 'MES']).sum()['Frete'] * -1
    ml_frete = ml_frete.reset_index()
    ml_frete.rename({'Frete': 'VALOR'}, axis=1, inplace=True)
    ml_frete['COD_DRE'] = 14

    # SHOPEE
    shopee = shopee.groupby(['ANO', 'MES']).sum()['TAXAS'] * -1
    shopee = shopee.reset_index()
    shopee.rename({'TAXAS': 'VALOR'}, axis=1, inplace=True)
    shopee['COD_DRE'] = 13

    # PAYABLE
    payable = payable.groupby(['ANO', 'MES', 'COD_DRE']).sum()['VALOR']
    payable = payable.reset_index()
    payable['COD_DRE'] = payable['COD_DRE'] + 1

    # ORDERS
    orders.DATA = pd.to_datetime(orders['DATA'], format="%d/%m/%Y")
    orders['MES'], orders['ANO'] = orders.DATA.dt.month, orders.DATA.dt.year
    orders.drop(['DATA', 'SKU'], inplace=True, axis=1), orders.rename({'RECEITA': 'VALOR'}, inplace=True, axis=1)

    receita, frete = orders[['MES', 'ANO', 'VALOR']], orders[['MES', 'ANO', 'FRETE']]
    frete['COD_DRE'] = 3
    frete.rename({'FRETE': 'VALOR'}, inplace=True, axis=1)

    receita.loc[receita.VALOR < 0, 'COD_DRE'] = 7
    receita.loc[receita.VALOR > 0, 'COD_DRE'] = 2

    frete = frete.groupby(['ANO', 'MES', 'COD_DRE']).sum()['VALOR'].reset_index()
    receita = receita.groupby(['ANO', 'MES', 'COD_DRE']).sum()['VALOR'].reset_index()

    # CONCAT
    BI_File = pd.concat([payable, shopee, ml_frete, ml_fees, amazon, CMV, paghiper, frete, receita], axis=0)

    return BI_File


def create_payable():
    df_contas = pd.read_excel(path_cp)
    df_contas = pd.DataFrame(df_contas)

    nan = df_contas.loc[df_contas.CATEGORIA.isna()]
    df_contas['EMISSAO'] = pd.to_datetime(df_contas['EMISSAO'], format="%d/%m/%Y")
    df_contas['MES'] = df_contas.EMISSAO.dt.month
    df_contas['ANO'] = df_contas.EMISSAO.dt.year
    df_contas.sort_values('EMISSAO', inplace=True)

    df_contas = df_contas.loc[df_contas['CATEGORIA'] != 'Bebidas']
    df_contas = df_contas.loc[df_contas.CATEGORIA.notna()]

    map_category = pd.read_excel(path_category)
    map_category = pd.DataFrame(map_category)

    DRE_contas = pd.merge(df_contas, map_category, 'left', left_on='CATEGORIA', right_on='Categoria')
    DRE_contas = DRE_contas.loc[DRE_contas['COD_DRE'] != 0]
    DRE_contas.VALOR = DRE_contas.VALOR * -1
    DRE_contas.drop(['ID', 'CATEGORIA', 'Categoria', 'Classificação Geral'], inplace=True, axis=1)
    DRE_contas.sort_values(inplace=True, by=['COD_DRE', 'MES', 'FORNECEDOR'])
    # nan_classificacao = DRE_contas.loc[DRE_contas.Classificação.isna()]

    # DRE_final = DRE_contas.groupby([['Classificação']]).sum()['VALOR']
    DRE_final = DRE_contas.groupby(['COD_DRE', 'ANO', 'MES']).sum()['VALOR']
    DRE_final = DRE_final.reset_index()
    DRE_final.to_csv(r'..\BI-payable.csv', index=False)


# ==== CREATING DATABASES =======
def create_databse():
    # < PagHiper
    df_paghiper = get_transactions_paghiper(init_date='2022-01-01', final_date='2022-09-30')
    df_paghiper.to_csv(r'.\1. DRE\BI-PagHiper_db.csv', index=False)

    # < Amazon
    df_amz = get_amazon_file()
    df_amz.to_csv(r'./1. DRE/BI-amazon_db.csv', index=False)

    # < Mercado Livre
    df_mercado_l = request_orders_ml2('2022-04-01', '2022-09-30')
    df_mercado_l.to_csv('./1. DRE/BI-dados_mp.csv', index=False)

    # < Shopee
    df_shopee = shopee_request_commission(year=2022, month_init=4, month_end=4)
    df_shopee.to_csv('./1. DRE/BI-Shopee_db1.csv', index=False)


BI = concat_dbs()
BI.to_csv(r'..\BI-File.csv', index=False)
print('')

# # Final File
# concat_dbs()
