#!/usr/bin/env python
# coding: utf-8

#acoes monitoradas
lista_extras = [
    'VALE3',
    'ITSA4',
    'ITUB3',
    'PETR4',
    'ABEV3',
    'GOLL4',
    'WEGE3',
    'HAPV3',
    'ENBR3',
    'CIEL3',
    'SBSP3',
    'TCNO4',
    'ELMD3',
]




from yahooquery import Ticker
import datetime as dt
import pandas as pd
import numpy as np
import time

#credencial json para acesso ao google
credencial = ''
idSheet = '1veUlKyg9RzgykIG1_x1vAW2O08A5GXuQoR5k-U75hyw'
def main():
	df_acoes = pd.read_html('https://pt.wikipedia.org/wiki/Lista_de_companhias_citadas_no_Ibovespa')[0]
	lista_acoes = df_acoes['Código'].to_list()

	lista_faltantes = list(set(lista_extras)- set(lista_acoes))
	lista_acoes = lista_acoes + lista_faltantes

	lista_acoes2  = []
	for i in lista_acoes:
		lista_acoes2.append(f'{i}.SA')


	df = pd.DataFrame()
	for i in lista_acoes:
		acao = Ticker(f'{i}.SA')
		df_temp = acao.history(period="720d",  interval = "1d")
		if type(df_temp) == dict:
			f = open("acoes_error.txt", "w")
			f.write(str(df_temp))
			continue

		df_temp = df_temp.reset_index()
		df_temp = df_temp[df_temp['close'].notnull()]

		for i in df_temp.index:
			if df_temp['close'][i]==0:
				df_temp['close'][i] = df_temp['open'][i]

		for i in df_temp.index:
			if df_temp['low'][i]==0:
				df_temp['low'][i] = df_temp['open'][i]    

		df_temp['MM_10'] = df_temp['close'].rolling(10).mean()
		df_temp['MM_20'] = df_temp['close'].rolling(20).mean()
		df_temp['MM_30'] = df_temp['close'].rolling(30).mean()

		df_temp['STD_10'] = df_temp['close'].rolling(10).std()
		df_temp['STD_20'] = df_temp['close'].rolling(20).std()
		df_temp['STD_30'] = df_temp['close'].rolling(30).std()

		df = df.append(df_temp,ignore_index=True)

	for i in df.index:
		df[f'MM_10_%'] = df['close']/df['MM_10']
		df[f'MM_20_%'] = df['close']/df['MM_20']
		df[f'MM_30_%'] = df['close']/df['MM_30']

		df[f'STD_10_%'] = df['close']/df['STD_10']
		df[f'STD_20_%'] = df['close']/df['STD_20']
		df[f'STD_30_%'] = df['close']/df['STD_30']


	df['resultado']= df['close']-df['open']

	df['resultado %']=(df['close']/df['open'])-1


	df['variacao %'] = (df['high']/df['low'])-1
	df['distancia da baixa %'] = (df['close']/df['low'])-1
	df['distancia da alta %'] = 1-(df['close']/df['high'])

	df['banda superior'] = df['MM_20']+(df['STD_20']*2)
	df['banda inferior'] = df['MM_20']-(df['STD_20']*2)


	df.loc[(df['banda inferior']>df['close']),'BB']=-1
	df.loc[(df['banda superior']<df['close']),'BB']=1
	df.loc[(df['BB'].isnull()),'BB']=0



	import gspread
	from oauth2client.service_account import ServiceAccountCredentials


	scope = ['https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive',]
	creds = ServiceAccountCredentials.from_json_keyfile_name(credencial, scope)
	client = gspread.authorize(creds)


	sheet= client.open_by_key(idSheet)
	#worksheet = sh.get_worksheet(0)
	worksheet = sheet.worksheet('Alertas')


	df_aviso = df[(df['BB']==-1)&(df['date']==df['date'].max())][['symbol','date','open','close','MM_20','banda inferior']]
	df_aviso['date'] =df_aviso['date'].apply(lambda x: str(x))


	worksheet = sheet.worksheet('Alertas')

	tamanho = df_aviso.shape[0]
	colunas = df_aviso.shape[1]


	worksheet.clear()


	cell_list = worksheet.range('A1:F1')
	celula = 0
	for c in df_aviso.columns:
		cell_list[celula].value = c
		celula += 1

	worksheet.update_cells(cell_list,'USER_ENTERED')


	cell_list = worksheet.range('A2:F{}'.format(tamanho+1))
	#cell_list = worksheet.range(1,2,colunas,tamanho+1)

	celula = 0
	for row in df_aviso.values:
		for c in row:
			cell_list[celula].value = c
			celula += 1

	worksheet.update_cells(cell_list,'USER_ENTERED')

today = dt.date.today()

if today.weekday() < 5:
	main()