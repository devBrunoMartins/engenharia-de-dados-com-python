# ==============================================================
# DESAFIO 6 — Ranking de Usuários por Janela de Tempo
# ==============================================================
#
# CONTEXTO
# Um time de produto precisa identificar os usuários mais
# ativos em diferentes períodos de tempo para alimentar
# dashboards e análises semanais.
#
# Você recebe logs de eventos de usuários.
#
# Formato esperado do log:
# timestamp | user_id | event_type | value
#
# Exemplo:
# "2024-01-10 10:15:00|u1|click|5"
#
# --------------------------------------------------------------
# OBJETIVO
# Implemente a função:
#
# def top_users_by_window(
#     logs: list[str],
#     event_type: str,
#     window_minutes: int,
#     top_n: int
# ) -> list[dict[str, object]]:
#
# A função deve gerar rankings de usuários por janelas
# de tempo consecutivas.
#
# --------------------------------------------------------------
# DEFINIÇÃO DE JANELA
#
# - As janelas são FIXAS e consecutivas
# - A primeira janela começa no MENOR timestamp válido
# - Cada janela tem duração de window_minutes
#
# Exemplo (window_minutes = 60):
# - 10:00:00 → 10:59:59
# - 11:00:00 → 11:59:59
#
# --------------------------------------------------------------
# MÉTRICA
#
# Para cada janela, calcule:
# - total de value por usuário (somente event_type informado)
#
# --------------------------------------------------------------
# SAÍDA ESPERADA
#
# Uma lista ordenada por janela (timestamp inicial crescente):
#
# [
#   {
#     "window_start": "2024-01-10 10:00:00",
#     "top_users": [
#         ("u1", 12),
#         ("u2", 7)
#     ]
#   },
#   {
#     "window_start": "2024-01-10 11:00:00",
#     "top_users": [
#         ("u3", 9)
#     ]
#   }
# ]
#
# --------------------------------------------------------------
# REGRAS DE RANKING
#
# - Ordenar usuários por:
#     1) total DESC
#     2) user_id ASC (desempate)
# - Retornar apenas top_n usuários por janela
#
# --------------------------------------------------------------
# REQUISITOS FUNCIONAIS
#
# A função deve:
#
# 1. Processar apenas logs VÁLIDOS
#    - Exatamente 4 campos separados por "|"
#    - timestamp no formato "%Y-%m-%d %H:%M:%S"
#    - value deve ser inteiro
#
# 2. Filtrar apenas event_type informado
#
# 3. Ignorar silenciosamente logs inválidos
#
# 4. Usar apenas biblioteca padrão
#
# --------------------------------------------------------------
# EXEMPLO
#
# logs = [
#     "2024-01-10 10:00:00|u1|click|5",
#     "2024-01-10 10:10:00|u2|click|7",
#     "2024-01-10 10:20:00|u1|click|7",
#     "2024-01-10 11:05:00|u3|click|9",
#     "2024-01-10 11:10:00|u1|view|100",
#     "log_invalido"
# ]
#
# Chamada:
#
# top_users_by_window(
#     logs,
#     event_type="click",
#     window_minutes=60,
#     top_n=2
# )
#
# Saída esperada:
#
# [
#   {
#     "window_start": "2024-01-10 10:00:00",
#     "top_users": [("u1", 12), ("u2", 7)]
#   },
#   {
#     "window_start": "2024-01-10 11:00:00",
#     "top_users": [("u3", 9)]
#   }
# ]
#
# --------------------------------------------------------------
# ENTREGA
#
# - Envie apenas a função
# - Inclua doctest no docstring
# - Sem prints
# - Sem explicações
#
# --------------------------------------------------------------
# TEMPO RECOMENDADO
#
# ⏱️ 60 minutos
# ==============================================================

from datetime import datetime, timedelta
from pprint import pprint


def top_users_by_window(
    logs: list[str],
    event_type: str,
    window_minutes: int,
    top_n: int
) -> list[dict[str, object]]:
    

    lista_logs = []
    for log in logs:
        try:
            data_, usuario, evento, valor_ = log.split('|')
            
            if evento != event_type: continue

            data = datetime.strptime(data_, "%Y-%m-%d %H:%M:%S")
            valor = int(valor_)

            lista_logs.append([data, usuario, evento, valor])

        except ValueError:
            continue
    

    min_timestamp = min(data for data, _, _, _ in lista_logs)
    window_delta = timedelta(minutes=window_minutes)
    ranking_by_window = {}

    for data_evento, usuario, _, valor in lista_logs:
        
        janela_tempo = (
            ((data_evento - min_timestamp) // window_delta)
            * window_delta
        ) + min_timestamp

        index_time = datetime.strftime(janela_tempo, "%Y-%m-%d %H:%M:%S")

        ranking_by_window.setdefault(index_time, {})
        ranking_by_window[index_time][usuario] = (
            ranking_by_window[index_time].get(usuario, 0) + valor
        )

    top_usuarios = []
    for janela, metricas in sorted(ranking_by_window.items()):
        usuarios_mais_ativos = sorted(metricas.items(), key=lambda x: x[1], reverse=True)[:top_n]
        
        top_usuarios.append({
            "window_start": janela,
            "top_users": usuarios_mais_ativos
            })

    return top_usuarios


    









if __name__ == '__main__':

    logs = [
        "2024-01-10 09:12:05|u1|click|3",
        "2024-01-10 09:14:10|u2|click|7",
        "2024-01-10 09:15:00|u3|view|10",
        "2024-01-10 09:16:22|u1|click|5",
        "2024-01-10 09:18:45|u4|click|2",
        "2024-01-10 09:20:00|u2|view|15",
        "2024-01-10 09:21:30|u3|click|4",
        "2024-01-10 09:24:55|u1|click|6",
        "2024-01-10 09:26:00|u5|click|9",
        "2024-01-10 09:29:59|u2|click|1",

        "2024-01-10 09:35:00|u1|click|8",
        "2024-01-10 09:36:14|u3|click|5",
        "2024-01-10 09:38:40|u4|view|20",
        "2024-01-10 09:40:00|u5|click|7",
        "2024-01-10 09:42:18|u2|click|6",
        "2024-01-10 09:44:50|u1|view|12",
        "2024-01-10 09:47:00|u3|click|3",
        "2024-01-10 09:49:33|u4|click|4",
        "2024-01-10 09:50:00|u5|click|5",
        "2024-01-10 09:54:10|u2|click|9",

        "2024-01-10 10:00:00|u1|click|10",
        "2024-01-10 10:01:05|u2|view|30",
        "2024-01-10 10:03:18|u3|click|6",
        "2024-01-10 10:05:40|u4|click|8",
        "2024-01-10 10:07:00|u5|view|25",
        "2024-01-10 10:09:59|u1|click|2",
        "2024-01-10 10:11:30|u2|click|7",
        "2024-01-10 10:14:00|u3|view|18",
        "2024-01-10 10:16:44|u4|click|5",
        "2024-01-10 10:19:00|u5|click|6",

        "2024-01-10 10:20:00|u1|click|4",
        "2024-01-10 10:22:10|u2|click|3",
        "2024-01-10 10:24:55|u3|click|9",
        "2024-01-10 10:26:00|u4|view|40",
        "2024-01-10 10:29:59|u5|click|1",

        "2024-01-10 10:35:00|u1|click|11",
        "2024-01-10 10:36:20|u2|view|22",
        "2024-01-10 10:38:45|u3|click|7",
        "2024-01-10 10:40:00|u4|click|6",
        "2024-01-10 10:42:30|u5|click|8",
        "2024-01-10 10:44:00|u1|view|14",
        "2024-01-10 10:47:10|u2|click|5",
        "2024-01-10 10:49:59|u3|click|4",

        "2024-01-10 10:55:00|u4|click|9",
        "2024-01-10 10:56:35|u5|view|33",
        "2024-01-10 10:58:10|u1|click|6",

        # logs fora de ordem
        "2024-01-10 09:05:00|u2|click|4",
        "2024-01-10 09:01:30|u1|click|2",

        # logs inválidos
        "log_invalido",
        "2024-01-10 10:10:00|u3|click|x",
        "2024-01-10 10:15:00|u4|click|",
        "2024-01-10|u5|click|7",

        # bordas exatas de janela (35 min)
        "2024-01-10 09:00:00|u1|click|5",
        "2024-01-10 09:34:59|u2|click|6",
        "2024-01-10 09:35:00|u3|click|7",
        "2024-01-10 10:09:59|u4|click|8",
        "2024-01-10 10:10:00|u5|click|9",

        # mais ruído
        "2024-01-10 11:00:00|u1|view|50",
        "2024-01-10 11:05:10|u2|click|10",
        "2024-01-10 11:10:20|u3|click|12",
        "2024-01-10 11:15:30|u4|view|60",
        "2024-01-10 11:20:40|u5|click|11"
    ]


    pprint(top_users_by_window(
        logs,
        event_type="click",
        window_minutes=60,
        top_n=2
    ))

  