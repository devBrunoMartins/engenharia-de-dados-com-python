import pprint
from datetime import datetime
# ============================================================
# DESAFIO 5 — Métricas por Usuário e Tipo de Evento
# ============================================================
#
# CONTEXTO
# Você recebe logs de eventos de usuários vindos de diferentes
# sistemas. O time de analytics precisa de um resumo confiável
# por usuário e por tipo de evento.
#
# Formato esperado do log:
# timestamp | user_id | event_type | value
#
# Exemplo:
# "2024-01-10 10:15:00|u1|click|5"
#
# ------------------------------------------------------------
# OBJETIVO
# Implemente a função:
#
# def user_event_metrics(
#     logs: list[str]
# ) -> dict[str, dict[str, dict[str, float]]]:
#
# A função deve gerar métricas agregadas por usuário e tipo
# de evento.
#
# ------------------------------------------------------------
# MÉTRICAS ESPERADAS (por usuário e event_type)
#
# Para cada (user_id, event_type), calcule:
#
# - count : número de eventos
# - total : soma dos valores
# - avg   : média dos valores
# - max   : maior valor
# - min   : menor valor
#
# ------------------------------------------------------------
# ESTRUTURA DE SAÍDA
#
# {
#   "u1": {
#       "click": {
#           "count": 3,
#           "total": 15,
#           "avg": 5.0,
#           "max": 7,
#           "min": 3
#       },
#       "view": {
#           "count": 1,
#           "total": 10,
#           "avg": 10.0,
#           "max": 10,
#           "min": 10
#       }
#   },
#   "u2": {
#       "click": {
#           "count": 2,
#           "total": 7,
#           "avg": 3.5,
#           "max": 4,
#           "min": 3
#       }
#   }
# }
#
# ------------------------------------------------------------
# REQUISITOS FUNCIONAIS
#
# A função deve:
#
# 1. Processar apenas logs VÁLIDOS
#    - Exatamente 4 campos separados por "|"
#    - timestamp no formato "%Y-%m-%d %H:%M:%S"
#    - value deve ser inteiro
#
# 2. Agregar métricas por:
#    - user_id
#    - event_type
#
# 3. Ignorar silenciosamente logs inválidos
#
# 4. Calcular média como float
#
# ------------------------------------------------------------
# REGRAS IMPORTANTES
#
# - Usar apenas biblioteca padrão
# - Uma única passada pelos logs
# - Sem prints
# - Sem exceções propagadas
# - Código legível e consistente
#
# ------------------------------------------------------------
# EXEMPLO
#
# logs = [
#     "2024-01-10 10:00:00|u1|click|5",
#     "2024-01-10 10:01:00|u1|click|3",
#     "2024-01-10 10:02:00|u1|click|7",
#     "2024-01-10 10:03:00|u1|view|10",
#     "2024-01-10 10:04:00|u2|click|3",
#     "2024-01-10 10:05:00|u2|click|4",
#     "log_invalido"
# ]
#
# Chamada:
#
# user_event_metrics(logs)
#
# Saída esperada:
#
# {
#   "u1": {
#       "click": {
#           "count": 3,
#           "total": 15,
#           "avg": 5.0,
#           "max": 7,
#           "min": 3
#       },
#       "view": {
#           "count": 1,
#           "total": 10,
#           "avg": 10.0,
#           "max": 10,
#           "min": 10
#       }
#   },
#   "u2": {
#       "click": {
#           "count": 2,
#           "total": 7,
#           "avg": 3.5,
#           "max": 4,
#           "min": 3
#       }
#   }
# }
#
# ------------------------------------------------------------
# ENTREGA
#
# - Envie apenas a função
# - Inclua doctest no docstring
# - Sem prints
# - Sem explicações
#
# ------------------------------------------------------------
# TEMPO RECOMENDADO
#
# ⏱️ 45 minutos
# ============================================================

from datetime import datetime
from itertools import groupby
def user_event_metrics(
    logs: list[str]
) -> dict[str, dict[str, dict[str, float]]]:
    """
    >>> logs = [
    ... "2024-01-10 10:00:00|u1|click|5",
    ... "2024-01-10 10:01:00|u1|click|3",
    ... "2024-01-10 10:02:00|u1|click|7",
    ... "2024-01-10 10:03:00|u1|view|10",
    ... "2024-01-10 10:04:00|u2|click|3",
    ... "2024-01-10 10:05:00|u2|click|4",
    ... "log_invalido"
    ... ]

    >>> user_event_metrics(logs)
    {'u1': {'click': {'count': 3, 'total': 15, 'avg': 5.0, 'max': 7, 'min': 3}, 'view': {'count': 1, 'total': 10, 'avg': 10.0, 'max': 10, 'min': 10}}, 'u2': {'click': {'count': 2, 'total': 7, 'avg': 3.5, 'max': 4, 'min': 3}}}

    """
    
    
    metricas = dict()
    for log in logs:
        try:
            data_hora, usuario, evento, valor_str = log.split('|')
            valor = int(valor_str)
            dt = datetime.fromisoformat(data_hora)
            # print()
            # print(usuario, usuario in metricas)
            if usuario in metricas:
                # print(evento, metricas[usuario], evento in metricas[usuario])
                if evento in metricas[usuario]:
                    count_ = metricas[usuario][evento]["count"] +1
                    total = metricas[usuario][evento]["total"] + valor
                    avg = total / count_
                    max = valor if valor > metricas[usuario][evento]["max"] else metricas[usuario][evento]["max"]
                    min = valor if valor < metricas[usuario][evento]["min"] else metricas[usuario][evento]["min"]

                    dados = { 
                        "count": count_,
                        "total": total,
                        "avg": avg,
                        "max": max,
                        "min": min
                        } 

                    metricas[usuario][evento] = dados
                    
                else:
                    dados = { "count": 1,
                        "total": valor,
                        "avg": float(valor),
                        "max": valor,
                        "min": valor}
                    metricas[usuario][evento] = dados
            else:
                dados = { "count": 1,
                    "total": valor,
                    "avg": float(valor),
                    "max": valor,
                    "min": valor}
                dict_user_metricas = {evento: dados,}
                metricas[usuario] = dict_user_metricas
                
        except (ValueError, TypeError):
            continue
        
    return metricas



if __name__ == "__main__":
    import doctest
    doctest.testmod(verbose=True)


    # logs = [
    #     "2024-01-10 10:00:00|u1|click|5",
    #     "2024-01-10 10:01:00|u1|click|3",
    #     "2024-01-10 10:02:00|u1|click|7",
    #     "2024-01-10 10:03:00|u1|view|10",
    #     "2024-01-10 10:04:00|u2|click|3",
    #     "2024-01-10 10:05:00|u2|click|4",
    #     "log_invalido"
    # ]

    # print(user_event_metrics(logs))
