import doctest



# ============================================
# DESAFIO 1 — Normalização e Agregação de Logs
# ============================================
#
# CONTEXTO
# Você recebeu um conjunto de logs de eventos de um sistema de ingestão de dados.
# Cada log é uma string com campos separados pelo caractere "|".
#
# Formato de cada linha:
# timestamp | user_id | event_type | value
#
# Exemplo de linha:
# "2024-01-10 10:00:00|u1|click|5"
#
# --------------------------------------------
# OBJETIVO
# Implemente a função:
#
# def aggregate_clicks(logs: list[str]) -> dict[str, int]:
#
# A função deve:
# 1. Processar apenas eventos do tipo "click"
# 2. Somar o campo "value" por "user_id"
# 3. Retornar um dicionário no formato:
#    {
#        "u1": 12,
#        "u2": 4,
#        "u3": 2
#    }
#
# --------------------------------------------
# REGRAS E RESTRIÇÕES
# - logs nunca será None
# - Cada linha é válida e segue o formato especificado
# - value é sempre um inteiro positivo
# - Não use bibliotecas externas (apenas Python padrão)
# - O código deve ser claro, legível e eficiente
#
# --------------------------------------------
# EXEMPLO DE ENTRADA
#
# logs = [
#     "2024-01-10 10:00:00|u1|click|5",
#     "2024-01-10 10:01:00|u2|view|3",
#     "2024-01-10 10:02:00|u1|click|7",
#     "2024-01-10 10:03:00|u3|click|2",
#     "2024-01-10 10:04:00|u2|click|4"
# ]
#
# --------------------------------------------
# SAÍDA ESPERADA
#
# {'u1': 12, 'u3': 2, 'u2': 4}
#
# (A ordem das chaves não importa)
#
# --------------------------------------------
# ENTREGA
# - Envie apenas o código da função
# - Sem prints
# - Sem explicações
# ============================================

def aggregate_clicks(logs: list[str]) -> dict[str, int]:
    """
    Agrega valores de eventos do tipo "click" por usuário.

    >>> logs = [
    ...     "2024-01-10 10:00:00|u1|click|5",
    ...     "2024-01-10 10:01:00|u2|view|3",
    ...     "2024-01-10 10:02:00|u1|click|7",
    ...     "2024-01-10 10:03:00|u3|click|2",
    ...     "2024-01-10 10:04:00|u2|click|4"
    ... ]
    >>> aggregate_clicks(logs) == {'u1': 12, 'u2': 4, 'u3': 2}
    True

    >>> aggregate_clicks([])
    {}

    >>> aggregate_clicks([
    ...     "2024-01-10 10:00:00|u1|view|5",
    ...     "2024-01-10 10:01:00|u2|view|3"
    ... ])
    {}
    """
    lista_total_por_usuario = {}
    for log in logs:
        _, usuario, tipo, valor  = log.split("|")
        if tipo == "click":
            if lista_total_por_usuario.get(usuario):
                lista_total_por_usuario[usuario] += int(valor)
            else:
                lista_total_por_usuario[usuario] = int(valor)

    return lista_total_por_usuario









if __name__ == "__main__":
    doctest.testmod(verbose=True)
    logs = [
    "2024-01-10 10:00:00|u1|click|5",
    "2024-01-10 10:01:00|u2|view|3",
    "2024-01-10 10:02:00|u1|click|7",
    "2024-01-10 10:03:00|u3|click|2",
    "2024-01-10 10:04:00|u2|click|4"]

    print(aggregate_clicks(logs))