# ============================================
# DESAFIO 2 — Top N Usuários por Consumo
# ============================================
#
# CONTEXTO
# Você recebe um grande volume de logs de eventos de um sistema de analytics.
# Cada log é uma string com campos separados por "|".
#
# Formato:
# timestamp | user_id | event_type | value
#
# Exemplo:
# "2024-01-10 10:00:00|u1|click|5"
#
# --------------------------------------------
# OBJETIVO
# Implemente a função:
#
# def top_n_users(
#     logs: list[str],
#     event_type: str,
#     n: int
# ) -> list[tuple[str, int]]:
#
# A função deve:
# 1. Filtrar apenas eventos cujo event_type seja igual ao informado
# 2. Agregar (somar) o campo "value" por user_id
# 3. Retornar os n usuários com maior valor total
#
# --------------------------------------------
# FORMATO DE RETORNO
# Lista de tuplas:
# [
#     (user_id, total_value),
#     ...
# ]
#
# --------------------------------------------
# REGRAS DE ORDENAÇÃO
# 1. Ordenar pelo total_value em ordem decrescente
# 2. Em caso de empate, ordenar pelo user_id em ordem alfabética crescente
#
# --------------------------------------------
# REGRAS E RESTRIÇÕES
# - logs pode ser muito grande (pense em eficiência)
# - n sempre será >= 1
# - Se houver menos que n usuários, retorne todos
# - Não use bibliotecas externas
# - Código deve ser determinístico e legível
#
# --------------------------------------------
# EXEMPLO
#
# logs = [
#     "2024-01-10 10:00:00|u1|click|5",
#     "2024-01-10 10:01:00|u2|click|3",
#     "2024-01-10 10:02:00|u1|click|7",
#     "2024-01-10 10:03:00|u3|click|12",
#     "2024-01-10 10:04:00|u2|click|4",
#     "2024-01-10 10:05:00|u3|view|100"
# ]
#
# Chamada:
# top_n_users(logs, "click", 2)
#
# Saída esperada:
# [('u1', 12), ('u3', 12)]
#
# --------------------------------------------
# ENTREGA
# - Envie apenas o código da função
# - Inclua doctest no docstring
# - Sem prints
# - Sem explicações
# ============================================

import doctest


def top_n_users(
    logs: list[str],
    event_type: str,
    n: int
) -> list[tuple[str, int]]:
    

    """
    >>> logs = [
    ...     "2024-01-10 10:00:00|u1|click|5",
    ...     "2024-01-10 10:01:00|u2|click|3",
    ...     "2024-01-10 10:02:00|u1|click|7",
    ...     "2024-01-10 10:03:00|u3|click|12",
    ...     "2024-01-10 10:04:00|u2|click|4",
    ...     "2024-01-10 10:05:00|u3|view|100"
    ... ]
    >>> top_n_users(logs, "click", 2)
    [('u1', 12), ('u3', 12)]
    """

    lista_de_dict_usuario_valor = dict()
    for log in logs:
        log_fragmentado = log.split("|")
        if log_fragmentado[2] == event_type:
            lista_de_dict_usuario_valor[log_fragmentado[1]] = lista_de_dict_usuario_valor.get(log_fragmentado[1], 0) + int(log_fragmentado[3])
    
    lista_de_dict_total_por_usuario = sorted(lista_de_dict_usuario_valor.items(), key=lambda x: (-x[1], x[0]))
    
    return lista_de_dict_total_por_usuario[:n]





if __name__ == "__main__":
    import doctest
    doctest.testmod(verbose=True)




    logs = [
        "2024-01-10 10:00:00|u1|click|5",
        "2024-01-10 10:01:00|u2|click|3",
        "2024-01-10 10:02:00|u1|click|7",
        "2024-01-10 10:03:00|u3|click|12",
        "2024-01-10 10:04:00|u2|click|4",
        "2024-01-10 10:05:00|u3|view|100"
    ]
    print(top_n_users(logs, "click", 2))

