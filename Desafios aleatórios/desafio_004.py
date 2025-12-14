# ==========================================================
# DESAFIO 4 — Ranking Temporal de Eventos por Usuário
# ==========================================================
#
# CONTEXTO
# Você recebe uma lista de logs de eventos de usuários.
# Esses eventos precisam ser organizados e analisados
# respeitando ordem temporal, tipo de evento e regras
# de desempate bem definidas.
#
# Formato esperado do log:
# timestamp | user_id | event_type | value
#
# Exemplo:
# "2024-01-10 10:15:00|u1|click|5"
#
# ----------------------------------------------------------
# OBJETIVO
# Implemente a função:
#
# def rank_user_events(
#     logs: list[str],
#     event_type: str
# ) -> dict[str, list[tuple[str, int]]]:
#
# A função deve retornar, para cada usuário, um ranking
# ordenado dos seus eventos filtrados por tipo.
#
# ----------------------------------------------------------
# REQUISITOS FUNCIONAIS
#
# A função deve:
#
# 1. Processar apenas logs VÁLIDOS
#    - Exatamente 4 campos separados por "|"
#    - timestamp no formato "%Y-%m-%d %H:%M:%S"
#    - value deve ser inteiro
#
# 2. Filtrar eventos:
#    - event_type igual ao informado
#
# 3. Agrupar por usuário
#
# 4. Para cada usuário:
#    - Ordenar eventos por:
#        a) timestamp ASC (mais antigo primeiro)
#        b) value DESC (em caso de empate)
#
# 5. Retornar estrutura final:
#
# {
#   "u1": [
#       ("2024-01-10 10:00:00", 5),
#       ("2024-01-10 10:02:00", 3)
#   ],
#   "u2": [
#       ("2024-01-10 10:01:00", 7)
#   ]
# }
#
# ----------------------------------------------------------
# REGRAS IMPORTANTES
#
# - Logs inválidos devem ser ignorados silenciosamente
# - Não usar print
# - Não lançar exceções
# - Usar apenas biblioteca padrão
# - Parsing deve ser seguro
# - Ordenação deve ser determinística
#
# ----------------------------------------------------------
# EXEMPLO
#
# logs = [
#     "2024-01-10 10:00:00|u1|click|5",
#     "2024-01-10 10:02:00|u1|click|3",
#     "2024-01-10 10:01:00|u2|click|7",
#     "2024-01-10 10:01:00|u1|view|10",
#     "log_invalido",
#     "2024-01-10 10:00:00|u1|click|8"
# ]
#
# Chamada:
#
# rank_user_events(logs, event_type="click")
#
# Saída esperada:
#
# {
#   "u1": [
#       ("2024-01-10 10:00:00", 8),
#       ("2024-01-10 10:00:00", 5),
#       ("2024-01-10 10:02:00", 3)
#   ],
#   "u2": [
#       ("2024-01-10 10:01:00", 7)
#   ]
# }
#
# ----------------------------------------------------------
# ENTREGA
#
# - Envie apenas a função
# - Inclua doctest no docstring
# - Sem prints
# - Sem explicações
#
# ----------------------------------------------------------
# TEMPO RECOMENDADO
#
# ⏱️ 35 minutos
# ==========================================================

from datetime import datetime

def rank_user_events(
    logs: list[str],
    event_type: str
) -> dict[str, list[tuple[str, int]]]:


    """
        >>> logs = [
        ... "2024-01-10 10:00:00|u1|click|5",
        ... "2024-01-10 10:02:00|u1|click|3",
        ... "2024-01-10 10:01:00|u2|click|7",
        ... "2024-01-10 10:01:00|u1|view|10",
        ... "log_invalido",
        ... "2024-01-10 10:00:00|u1|click|8"
        ... ]

        >>> rank_user_events(logs, event_type="click")
        {'u1': [('2024-01-10 10:00:00', 8), ('2024-01-10 10:00:00', 5), ('2024-01-10 10:02:00', 3)], 'u2': [('2024-01-10 10:01:00', 7)]}
    """

    usuarios_agrupados = dict()
    for log in logs:
        try:
            data_time, usuario, tipo, str_valor = log.split('|')
            valor = int(str_valor)

            datetime.fromisoformat(data_time)
            if tipo != event_type: continue
            
            
            if usuario not in usuarios_agrupados:
                usuarios_agrupados[usuario] = [(data_time, valor)]
            else:
                usuarios_agrupados[usuario].append((data_time, valor))

        except (ValueError, TypeError) as err:
            continue

    usuarios_ordenados = dict()
    for user, eventos in usuarios_agrupados.items():
        usuarios_ordenados[user] = sorted(eventos, key=lambda x: (datetime.fromisoformat(x[0]), -x[1]))
        
    return usuarios_ordenados

    


if __name__ == "__main__":
    import doctest
    doctest.testmod(verbose=True)
    logs = [
        "2024-01-10 10:00:00|u1|click|5",
        "2024-01-10 10:02:00|u1|click|3",
        "2024-01-10 10:01:00|u2|click|7",
        "2024-01-10 10:01:00|u1|view|10",
        "log_invalido",
        "2024-01-10 10:00:00|u1|click|8"
    ]

    
    rank_user_events(logs, event_type="click")
