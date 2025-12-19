# Desafio 7: Moving Average de Eventos por Usuário
#
# Escreva uma função que calcula a média móvel (moving average) de valores de eventos
# para cada usuário, considerando uma janela de tamanho k eventos.
#
# A função deve receber:
# - logs: lista de strings no formato "YYYY-MM-DD HH:MM:SS|usuario|evento|valor"
# - event_type: o tipo de evento que será considerado ("click", "view", etc.)
# - k: tamanho da janela móvel (em número de eventos)
#
# Deve retornar um dicionário no formato:
# {usuario1: [média1, média2, ...], usuario2: [média1, média2, ...], ...}
#
# Regras:
# - Ignorar logs inválidos
# - Ordenar eventos por timestamp antes de calcular a média
# - Calcular a média apenas do evento especificado (event_type)
# - O resultado para cada usuário é uma lista de médias móveis, começando do primeiro evento válido
#
# Exemplo:
# logs = [
#   "2024-01-10 10:00:00|u1|click|5",
#   "2024-01-10 10:01:00|u1|click|7",
#   "2024-01-10 10:02:00|u1|click|3",
#   "2024-01-10 10:03:00|u2|click|4"
# ]
# moving_average(logs, "click", 2)
# Deve retornar:
# {
#   "u1": [6.0, 5.0],   # médias móveis de tamanho 2: (5+7)/2=6, (7+3)/2=5
#   "u2": [4.0]         # apenas um evento, média é ele mesmo
# }

from datetime import datetime
from pprint import pprint
import re

def ordenar_por_usuario_e_data(usuario, data):
    return usuario, data


def moving_average(logs, event_type, k):

    logs_limpos = []
    for log in logs:
        try:
            # Distribui os dados dos logs às suas devidas variáveis
            data_, usuario_, evento, valor_ = log.split("|")

            # Valida data
            data = datetime.strptime(data_, "%Y-%m-%d %H:%M:%S")
            
            # Valida usuario
            usuario_padrao = bool(re.fullmatch(r'u\d+', usuario_))
            if usuario_padrao:
                usuario = usuario_
            else:
                continue

            # Valida evento
            if evento != event_type:
                continue
            
            valor = int(valor_)
            if valor < 0:
                continue

            # lista dos logs com seus dados limpos
            logs_limpos.append([data, usuario, evento, valor])
        except (ValueError, TypeError):
            continue
        
    # Dados ordenados por data para dar sentido à média móvel
    usuarios_ordenados = sorted(logs_limpos, key=lambda x: ordenar_por_usuario_e_data(x[1], x[0]))
    media_movel_usuarios = dict()
    usuario = ""
    valores = []

    for _, usuario_atual, _, valor_atual in usuarios_ordenados:
        # Assumindo que a lista está em ordem de usuário
        # e cada usuário tem seu histórico em ordem de dada
        # podemos varrer a lista assumindo essa ordem sem 
        # quebrar o sistema
        if usuario != usuario_atual:
            usuario = usuario_atual
            valores.clear()

        valores.append(valor_atual)
        janela_movel = valores[-k:]
        tamanho_janela_movel = len(janela_movel)
        
        media_movel = round(sum(janela_movel) / tamanho_janela_movel, 2)
       
        media_movel_usuarios.setdefault(usuario_atual, []).append(media_movel)

    return media_movel_usuarios




if __name__ == "__main__":
    logs = [
    # VÁLIDOS
    "2024-01-10 11:21:00|u2|click|10",
    "2024-01-10 11:22:00|u7|view|7",
    "2024-01-10 11:23:00|u3|click|9",
    "2024-01-10 11:24:00|u6|view|2",
    "2024-01-10 11:25:00|u9|click|5",
    "2024-01-10 11:26:00|u10|click|1",
    "2024-01-10 11:27:00|u1|view|10",
    "2024-01-10 11:28:00|u4|click|7",
    "2024-01-10 11:29:00|u5|click|3",
    "2024-01-10 11:30:00|u8|view|6",

    # INVÁLIDOS – valor não numérico
    "2024-01-10 11:31:00|u2|click|x",
    "2024-01-10 11:32:00|u3|click|NaN",

    # INVÁLIDOS – data inválida
    "2024-13-10 11:33:00|u7|click|5",
    "2024-01-32 11:34:00|u6|view|1",

    # VÁLIDOS
    "2024-01-10 11:35:00|u9|click|6",
    "2024-01-10 11:36:00|u10|view|5",

    # INVÁLIDOS – campo faltando
    "2024-01-10 11:37:00|u1|click",
    "2024-01-10 11:38:00|u4",

    # INVÁLIDOS – separador errado
    "2024-01-10 11:39:00,u5,view,2",

    # VÁLIDOS
    "2024-01-10 11:40:00|u8|click|7",

    # INVÁLIDOS – evento desconhecido
    "2024-01-10 11:41:00|u2|purchase|3",

    # INVÁLIDOS – valor negativo
    "2024-01-10 11:42:00|u3|click|-10",

    # VÁLIDOS
    "2024-01-10 11:43:00|u7|click|5",
    "2024-01-10 11:44:00|u6|click|4",

    # INVÁLIDOS – texto aleatório
    "isso nao é um log",
    "",
    "||||",

    # VÁLIDOS
    "2024-01-10 11:45:00|u9|click|10",
    "2024-01-10 11:46:00|u10|view|6",
    "2024-01-10 11:47:00|u1|view|7",

    # INVÁLIDOS – timestamp fora de ordem (mas sintaticamente válido)
    "2023-12-31 23:59:59|u4|click|9",

    # INVÁLIDOS – valor float
    "2024-01-10 11:48:00|u5|click|3.5",

    # VÁLIDOS
    "2024-01-10 11:49:00|u5|click|2",
    "2024-01-10 11:50:00|u8|view|4",

    # INVÁLIDOS – espaços extras
    " 2024-01-10 11:51:00 | u2 | click | 7 ",

    # VÁLIDOS
    "2024-01-10 11:52:00|u3|view|3",
    "2024-01-10 11:53:00|u7|click|5",
    "2024-01-10 11:54:00|u6|click|4",

    # INVÁLIDOS – data não ISO
    "10-01-2024 11:55:00|u9|click|10",

    # VÁLIDOS
    "2024-01-10 11:56:00|u10|view|6",
    "2024-01-10 11:57:00|u1|view|7",

    # INVÁLIDOS – usuário vazio
    "2024-01-10 11:58:00||click|9",

    # VÁLIDOS
    "2024-01-10 11:59:00|u5|click|2",
    "2024-01-10 12:00:00|u8|view|4",

    # INVÁLIDOS – campo extra
    "2024-01-10 12:01:00|u2|click|6|extra",

    # VÁLIDOS
    "2024-01-10 12:02:00|u3|click|8",
    "2024-01-10 12:03:00|u7|view|5",

    # INVÁLIDOS – timestamp impossível
    "2024-01-10 25:61:00|u6|click|7",

    # VÁLIDOS
    "2024-01-10 12:04:00|u6|click|7",
    "2024-01-10 12:05:00|u9|view|2",
    "2024-01-10 12:06:00|u10|click|9",

    # INVÁLIDOS – valor vazio
    "2024-01-10 12:07:00|u1|click|",

    # VÁLIDOS
    "2024-01-10 12:08:00|u4|view|6",
    "2024-01-10 12:09:00|u5|click|4",
    "2024-01-10 12:10:00|u8|click|5",

    # INVÁLIDOS – log parcialmente correto
    "2024-01-10|u2|click|8",

    # VÁLIDOS
    "2024-01-10 12:11:00|u2|view|8",
    "2024-01-10 12:12:00|u3|click|6",
    "2024-01-10 12:13:00|u7|click|10",

    # INVÁLIDOS – evento em maiúsculo
    "2024-01-10 12:14:00|u6|CLICK|3",

    # VÁLIDOS
    "2024-01-10 12:15:00|u9|click|7",
    "2024-01-10 12:16:00|u10|view|4",

    # INVÁLIDOS – caracteres especiais
    "2024-01-10 12:17:00|u@1|click|8",

    # VÁLIDOS
    "2024-01-10 12:18:00|u4|click|5",
    "2024-01-10 12:19:00|u5|view|6",
    "2024-01-10 12:20:00|u8|click|9"
    ]

    resultado = moving_average(logs, "click", 2)
    pprint(resultado)
