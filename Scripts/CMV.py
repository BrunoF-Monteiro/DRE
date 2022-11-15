import calendar
from datetime import datetime
import pandas as pd
import numpy as np


# ============== vendas/saídas do período ====================
def sales_from(mes_inicial=datetime.today().month - 1, mes_final=datetime.today().month - 1):
    try:
        saidas = pd.read_excel(
            r'C:\Users\meial\Meu Drive\Minidrinks\12. Dev\3. DataBase Pedidos\1. API Tiny\Vendas API.xlsx')
    except FileNotFoundError:
        saidas = pd.read_excel(r'G:\Meu Drive\MiniDrinks\12. Dev\3. DataBase Pedidos\1. API Tiny\Vendas API.xlsx')

    # filtrando intervalo de saídas (default = mês anterior)
    df_sales = pd.DataFrame(saidas)
    df_sales['DATA'] = pd.to_datetime(df_sales['DATA'], format="%d/%m/%Y")
    df_sales['QTDE'] = df_sales['QTDE'] * -1
    intervalo = (df_sales['DATA']
                 >= datetime(year=2022, month=mes_inicial, day=1)
                 ) & \
                (df_sales['DATA']
                 <= datetime(year=2022, month=mes_final, day=calendar.monthrange(datetime.today().year, mes_final)[1])
                 )
    return df_sales[intervalo]


df_saidas = sales_from(1, 9)

df_saidas = df_saidas[['DATA', 'SKU', 'QTDE', 'STATUS']]
# atribui tipo de movimentação
df_saidas['TIPO'] = 'S'
df_saidas = df_saidas.loc[df_saidas.STATUS != 'Cancelado']
df_saidas.reset_index(drop=True, inplace=True)

# ============== captura do preço de custo anterior ====================
# cm_anterior = pd.read_excel(r'./auxiliar/custo_medio_anterior.xlsx', decimal=',', thousands='.')
cm_anterior = pd.read_excel(r'C:\Users\meial\Meu Drive\Minidrinks\12. Dev\6. Financeiro\1. DRE\auxiliar\custo_medio_anterior.xlsx', decimal=',', thousands='.')
df_custos = pd.DataFrame(cm_anterior)
del cm_anterior
df_custos.loc[df_custos['ESTOQUE'] < 0, 'ESTOQUE'] = 0
df_custos['SKU'] = df_custos['SKU'].astype(str)

# ============== entradas do período ====================
# entradas = pd.read_csv(r'.\auxiliar\notas_entrada.csv', decimal=',', thousands='.', sep=',')
entradas = pd.read_excel(r'C:\Users\meial\Meu Drive\Minidrinks\12. Dev\6. Financeiro\1. DRE\auxiliar\notas_entrada.xls', decimal=',', thousands='.')
df_entradas = pd.DataFrame(entradas, columns=['Data entrada', 'Numero Nota', 'Natureza', 'Contato', 'CPF / CNPJ', 'UF',
                                              'Item Descricao', 'Item Codigo', 'Item Quantidade', 'Item Valor',
                                              'Valor Imposto ST / ICMS', 'Valor Imposto Simples / ICMS',
                                              'Valor Imposto IPI'])
df_entradas['Item Codigo'] = df_entradas['Item Codigo'].astype(str)

del entradas
df_entradas['Data entrada'] = pd.to_datetime(df_entradas['Data entrada'])

# remove informações de lote da descrição do produto e espaçamento
df_entradas['Item Descricao'] = df_entradas['Item Descricao'].apply(lambda x: x.upper().split('LOTE')[0])
df_entradas['Item Descricao'] = df_entradas['Item Descricao'].apply(lambda x: x.replace(' ', ''))

# remove movimentações/transferencias de estoque e fornecedores de caixas (a partir do CNPJ da nota)
for i in ['24.817.820/0003-09', '24.817.820/0002-10', '24.817.820/0001-39', '13.702.101/0001-56']:
    df_entradas = df_entradas.loc[df_entradas["CPF / CNPJ"].values != i]
df_entradas.reset_index(inplace=True, drop=True)

# ============== mapeamento das quantidades da nota ====================
# mapping = pd.read_excel(r'./auxiliar/mapeamento_produtos.xlsx')
mapping = pd.read_excel(r'C:\Users\meial\Meu Drive\Minidrinks\12. Dev\6. Financeiro\1. DRE\auxiliar\mapeamento_produtos.xlsx')
df_map = pd.DataFrame(mapping)
df_map['SKU'] = df_map['SKU'].astype(str)
df_map['PRODUTOS'] = df_map['PRODUTOS'].apply(lambda x: x.upper().replace(' ', ''))
df_map.drop_duplicates(inplace=True, subset='PRODUTOS')
del mapping

# cruzando dados de entrada com tabela de mapeamento
df_entradas_prev_map = pd.merge(
    df_entradas[['Data entrada', 'Item Descricao', 'Item Codigo', 'Item Quantidade', 'Item Valor', 'Natureza']], df_map, how='left',
    left_on=['Item Descricao'], right_on=['PRODUTOS'])
df_na_descr = df_entradas_prev_map[df_entradas_prev_map['SKU'].isna()].drop_duplicates(subset=['Item Descricao'])
del df_entradas_prev_map


# atribuindo sku e quantidade para produtos não mapeados
def append_map():
    unds_i = []
    sku_i = []
    descrip_i = []
    for i in df_na_descr.index.values.tolist():

        print(f'Deseja acrescentar o produto {df_na_descr["Item Descricao"][i]} a base de dados de mapeamento? [Y/N]')
        resposta = str(input())

        while not resposta.upper() in ['Y', 'N']:
            print(f'Deseja acrescentar o produto {df_na_descr["Item Descricao"][i]} a base de dados de mapeamento? [Y/N]')
            resposta = str(input())

        if resposta.upper() == 'Y':
            descrip_i.append(df_na_descr["Item Descricao"][i])
            print(f'Qual a QUANTIDADE de produtos unitários no item {df_na_descr["Item Descricao"][i]}?\nNa NF constam {df_na_descr["Item Quantidade"][i]} itens ao custo de {df_na_descr["Item Valor"][i]}')
            unds_i.append(int(input()))
            print(f'Qual o SKU do item {df_na_descr["Item Descricao"][i]}?')
            sku_i.append(str(input()))
        else:
            continue

    # acrescentado mapeamentos a database
    prev_input_map = pd.DataFrame({'UNDS': unds_i, 'SKU': sku_i, 'PRODUTOS': descrip_i})

    return prev_input_map


prev_input_map = append_map()
del df_na_descr

print('Acrescentar os novos produtos mapeados a database? [Y/N]')
resposta = str(input()).upper()
if resposta == 'Y':
    df_map = pd.concat([df_map, prev_input_map], axis=0)
    # df_map.to_excel('../mapeamento_produtos.xlsx', index=False)
    df_map.to_excel(r'C:\Users\meial\Meu Drive\Minidrinks\12. Dev\6. Financeiro\1. DRE\auxiliar\mapeamento_produtos.xlsx', index=False)

# refazendo o cruzamento de mapeamento após correção manual
df_entradas_mapped = pd.merge(df_entradas, df_map[['PRODUTOS', 'UNDS', 'SKU']],
                              how='left', left_on=['Item Descricao'], right_on=['PRODUTOS'])
df_entradas_mapped.sort_values(by=['Data entrada'], axis=0, ascending=True, inplace=True)
df_entradas_mapped.drop(columns=['Contato', 'CPF / CNPJ', 'UF', 'PRODUTOS'], inplace=True)
df_entradas_mapped.sort_values(by='Numero Nota', inplace=True)
df_entradas_mapped.reset_index(drop=True, inplace=True)
del df_entradas, df_map

# corrigindo valores e quantidades
df_entradas_mapped['Quantidade Corrigida'] = df_entradas_mapped['Item Quantidade'] * df_entradas_mapped['UNDS']
df_entradas_mapped['Item Valor Corrigido'] = (df_entradas_mapped['Item Valor'] / df_entradas_mapped['UNDS'])
df_entradas_mapped['Item Valor Final'] = \
    (df_entradas_mapped['Valor Imposto ST / ICMS'] / df_entradas_mapped['Quantidade Corrigida']) \
    + (df_entradas_mapped['Valor Imposto IPI'] / df_entradas_mapped['Quantidade Corrigida']) \
    - df_entradas_mapped['Valor Imposto Simples / ICMS'].where(df_entradas_mapped['Valor Imposto ST / ICMS'] == 0, 0) / \
    df_entradas_mapped['Quantidade Corrigida'] \
    + df_entradas_mapped['Item Valor Corrigido']

# =========== criando DataFrame final de movimentações gerais com os referentes custos ================
df_entradas_mapped = df_entradas_mapped.rename(
    columns={'Data entrada': 'DATA', 'Quantidade Corrigida': 'QTDE', 'Item Valor Final': 'CUSTO_NF',
             'Natureza': 'TIPO'})

# =========== ultimo preço de custo
last_cost = df_entradas_mapped.loc[df_entradas_mapped.TIPO != 'Devolução de venda de mercadoria adquirida de terceiros para']
last_cost.groupby(['SKU']).last()[['DATA', 'CUSTO_NF', 'Numero Nota']].reset_index().to_csv(r'../auxiliar/last_PCU.csv', index=False)
del last_cost

# =========== criando DataFrame final
df_entradas_final = df_entradas_mapped[['DATA', 'SKU', 'TIPO', 'QTDE', 'CUSTO_NF']]
df_entradas_final.reset_index(drop=True, inplace=True)
del df_entradas_mapped

# =========== movimentações
df_mov = pd.concat([df_saidas, df_entradas_final], axis=0)
df_mov.sort_values(by=['SKU', 'DATA'], axis=0, ascending=True, inplace=True, ignore_index=True)

df_final = df_mov.merge(df_custos[['SKU', 'ESTOQUE', 'CUSTO']], on='SKU', how='left')
df_final['ESTOQUE'].dropna(inplace=True)
del df_mov

# balanço de movimentação e verificação de estoque negativo no período
df_final['NOVO_ESTOQUE'] = df_final.groupby('SKU')['QTDE'].cumsum() + df_final['ESTOQUE']

min_estoque_negativo = df_final.loc[df_final['NOVO_ESTOQUE'] < 0, ['SKU', 'NOVO_ESTOQUE']].groupby('SKU').min()
min_estoque_negativo.reset_index(drop=False, inplace=True)

# correção de estoques inicial e final
novo_estoque_inicial = df_final.groupby('SKU')['ESTOQUE'].first() + min_estoque_negativo.groupby('SKU')['NOVO_ESTOQUE'].first() * -1 + 1
novo_estoque_inicial.dropna(inplace=True)
del min_estoque_negativo

df_final['QTDE_INICIAL'] = df_final['SKU'].map(novo_estoque_inicial)
del novo_estoque_inicial

df_final['QTDE_INICIAL'].fillna(df_final.ESTOQUE, inplace=True)
df_final['ESTOQUE_FINAL'] = df_final.groupby('SKU')['QTDE'].cumsum() + df_final['QTDE_INICIAL']
df_final.drop(['ESTOQUE', 'NOVO_ESTOQUE', 'QTDE_INICIAL'], axis=1, inplace=True)

# atualização do preço de custo a partir da entrada
df_final['CUSTO'] = np.where(
    (df_final.TIPO != 'S') & (df_final.TIPO != 'Devolução de venda de mercadoria adquirida de terceiros para'),
    (df_final.QTDE * df_final.CUSTO_NF + (df_final.ESTOQUE_FINAL - df_final.QTDE) * df_final.CUSTO)
    / df_final.ESTOQUE_FINAL, df_final.CUSTO)

ultimo_custo = []
for i in df_final.index:

    if df_final.SKU[i] != df_final.iloc[i-1].SKU:
        ultimo_custo.append(df_final.CUSTO[i])

    else:
        if df_final.TIPO[i] != 'Devolução de venda de mercadoria adquirida de terceiros para' and df_final.TIPO[i] != 'S':
            novo_custo = (ultimo_custo[-1] * (df_final.ESTOQUE_FINAL[i] - df_final.QTDE[i]) + df_final.CUSTO_NF[i] * df_final.QTDE[i])/df_final.ESTOQUE_FINAL[i]
            df_final.CUSTO[i] = novo_custo
            ultimo_custo.append(novo_custo)

        else:
            df_final.CUSTO[i] = ultimo_custo[-1]


del i, novo_custo, ultimo_custo
# finalização do dataframe
df_final['CMV'] = df_final['QTDE'] * df_final['CUSTO']

CMV = df_final.loc[df_final.TIPO == 'S']
CMV['DIA'] = CMV.DATA.dt.day
CMV['MES'] = CMV.DATA.dt.month
CMV['ANO'] = CMV.DATA.dt.year
CMV['CMV'] = df_final['CMV'].apply(lambda x: round(x, 2))
CMV['CUSTO'] = df_final['CUSTO'].apply(lambda x: round(x, 2))
CMV.COD_DRE = 9

CMV_mes = CMV.set_index(['DATA']).groupby(pd.Grouper(freq='M')).sum()['CMV']
CMV_SKU_mes = CMV.groupby(['SKU', 'MES']).sum()['CMV']
QTDE_SKU_mes = CMV.groupby(['SKU', 'MES']).sum()['QTDE']
VU_SKU = CMV_SKU_mes/QTDE_SKU_mes

CMV[['MES', 'ANO', 'SKU', 'CUSTO', 'QTDE', 'CMV']].to_csv(r'C:\Users\meial\Meu Drive\Minidrinks\12. Dev\6. Financeiro\1. DRE\BI-CMV_MiniDrinks.csv', index=False)
print('')
