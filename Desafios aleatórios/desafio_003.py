# ============================================
# DESAFIO 3 — Agregação por Janela de Tempo
# ============================================
#
# CONTEXTO
# Você recebe logs de eventos vindos de múltiplas fontes.
# Alguns registros podem estar fora do padrão, mas NÃO devem
# quebrar o pipeline de processamento.
#
# Formato esperado de cada log:
# timestamp | user_id | event_type | value
#
# Exemplo:
# "2024-01-10 10:15:00|u1|click|5"
#
# --------------------------------------------
# OBJETIVO
# Implemente a função:
#
# def aggregate_by_time_window(
#     logs: list[str],
#     event_type: str,
#     start: str,
#     end: str
# ) -> dict[str, int]:
#
# A função deve agregar valores por usuário considerando
# uma janela de tempo específica.
#
# --------------------------------------------
# REQUISITOS FUNCIONAIS
#
# A função deve:
#
# 1. Processar apenas logs VÁLIDOS
#    - Exatamente 4 campos separados por "|"
#    - value deve ser um inteiro válido
#    - timestamp no formato "%Y-%m-%d %H:%M:%S"
#
# 2. Filtrar eventos:
#    - event_type igual ao informado
#    - timestamp entre start e end (inclusive)
#
# 3. Agregar:
#    - Somar value por user_id
#
# 4. Ignorar silenciosamente logs inválidos:
#    - Não lançar exceções
#    - Não imprimir erros
#
# --------------------------------------------
# REGRAS E RESTRIÇÕES
# - start e end usam o mesmo formato do timestamp
# - start <= end
# - Use apenas biblioteca padrão
# - Uma única passada pelos logs
# - Código deve ser legível e robusto
#
# --------------------------------------------
# EXEMPLO
#
# logs = [
#     "2024-01-10 10:00:00|u1|click|5",
#     "2024-01-10 10:30:00|u2|click|3",
#     "2024-01-10 11:00:00|u1|click|7",
#     "2024-01-10 09:59:00|u3|click|10",
#     "2024-01-10 10:15:00|u1|view|4",
#     "log_invalido",
#     "2024-01-10 10:45:00|u2|click|x"
# ]
#
# Chamada:
#
# aggregate_by_time_window(
#     logs,
#     event_type="click",
#     start="2024-01-10 10:00:00",
#     end="2024-01-10 10:59:59"
# )
#
# Saída esperada:
#
# {
#     "u1": 5,
#     "u2": 3
# }
#
# --------------------------------------------
# ENTREGA
# - Envie apenas o código da função
# - Inclua doctest no docstring
# - Sem prints
# - Sem explicações
# ============================================

from datetime import datetime

def aggregate_by_time_window(
    logs: list[str],
    event_type: str,
    start: str,
    end: str
) -> dict[str, int]:
    
    """
    >>> logs = [
    ... "2024-01-10 10:00:00|u1|click|5",
    ... "2024-01-10 10:30:00|u2|click|3",
    ... "2024-01-10 11:00:00|u1|click|7",
    ... "2024-01-10 09:59:00|u3|click|10",
    ... "2024-01-10 10:15:00|u1|view|4",
    ... "log_invalido",
    ... "2024-01-10 10:45:00|u2|click|x"
    ... ]

    >>> aggregate_by_time_window(
    ... logs,
    ... event_type="click",
    ... start="2024-01-10 10:00:00",
    ... end="2024-01-10 10:59:59"
    ... )
    {'u1': 5, 'u2': 3}
    """
    clicks_usuarios = dict()

    for log in logs:
        try:
            # Validação dos dados
            data_hora, usuario, evento, str_valor = log.split('|')
            time_stamp = datetime.fromisoformat(data_hora)
            data_inicio = datetime.fromisoformat(start)
            data_fim = datetime.fromisoformat(end)

            if evento != event_type or data_inicio > time_stamp or time_stamp > data_fim:
                raise

            valor = int(str_valor)

            # Gera o dicionário que será retornado: soma(valor) agrupado por usuário
            clicks_usuarios[usuario] = clicks_usuarios.get(usuario, 0) + valor

        except:
            pass
        
    return clicks_usuarios







if __name__ == '__main__':
    import doctest

    doctest.testmod(verbose=True)

    logs = [
        "2024-01-10 10:00:00|u1|click|5",
        "2024-01-10 10:30:00|u2|click|3",
        "2024-01-10 11:00:00|u1|click|7",
        "2024-01-10 09:59:00|u3|click|10",
        "2024-01-10 10:15:00|u1|view|4",
        "log_invalido",
        "2024-01-10 10:45:00|u2|click|x"
    ]



    aggregate_by_time_window(
        logs,
        event_type="click",
        start="2024-01-10 10:00:00",
        end="2024-01-10 10:59:59"
    )