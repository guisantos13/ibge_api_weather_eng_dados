##################################################################################################
############################FUNÇÕES##PARA##AUXILIAR##NO##PROJETO##################################
##################################################################################################
import json
import ssl
from urllib import response
from helpers.methods import *


def http_requests():
    """
    Esta função nos auxilia a realizar o get,sendo necessária pois o servidor não suporta
    a conexão OpenSSL3.

    Arg = Endereço url
    Return = get

    """
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ctx.options |= 0x4  # OP_LEGACY_SERVER_CONNECT
    session = requests.session()
    session.mount("https://", CustomHttpAdapter(ctx))
    return session


def requests_weather_api(cities, api_key, api_url, metodo, periodo):
    """
    Esta função é responsavél por realizar o request na API-WATHER para criação do dataframe.
    Retorna os dados dos próximos 5 dias.

    Arg1 = lista com os nomes das cidades,
    Arg2 = chave da api
    Arg3 = url da api

    Return = retorna uma lista com todos os dados solicitados.
    """
    weather_api_results = []  # variavél para armazenamento dos dados

    try:
        if len(cities) > 0:
            for city in cities:
                response = http_requests().get(
                    f"{api_url}{metodo}{city}%20sp/{periodo}?unitGroup=metric&key={api_key}&include=obs%2Cfcst%2Cstats%2Calerts%2Ccurrent%2Chistfcst&elements=datetime,conditions,precipprob,tempmax,tempmin,temp,preciptype,sunrise,sunset,severerisk,windspeedmax&contentType=json"
                )
                weather_api_results.append(response.json())
    except Exception as exc:
        print(f"Erro{exc}")

    return weather_api_results


def dados_of_df_weather(dados_api):
    """
    Está função recebe os dados de consulta da api weather e retorna uma lista de dicionários formatados para a criação do dataframe.

    Arg : dados de consulta da API
    Return : lista de dicionarios com os dados da cidade e os dados de previsão para cada dia da consulta
    """

    position = 0
    dados_df = []
    for dados in dados_api:
        for position in range(6):
            dados_df_dict = {
                "Latitude": dados.get("latitude"),
                "Longitude": dados.get("longitude"),
                "Cidade": dados.get("resolvedAddress"),
            }
            dias_de_previsão = dados.get("days")[position]
            dias_de_previsão.update(dados_df_dict)
            dados_df.append(dias_de_previsão)
            position += 1
        else:
            position = 0

    return dados_df
