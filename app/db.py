"""Banco de dados (Postgres): histórico de mensagens e resumos de serviço.

Mantém o esquema original (tabelas `mensagens` e `resumos`), então dados já gravados continuam valendo.
A conexão é aberta sob demanda: se o banco estiver fora do ar, o atendimento segue funcionando
(webhook e IA não dependem dele) e o registro volta sozinho quando o banco voltar.
"""
import asyncio
import json
import logging
import os

import asyncpg
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger("chaveiro.db")

# Nome próprio de propósito: o Chatwoot (Rails) também lê a variável DATABASE_URL e, se ela existir no ambiente
# dele, passa a usar esse banco no lugar do dele. Por isso a do projeto se chama CHAVEIRO_DATABASE_URL.
DATABASE_URL = (
    os.getenv("CHAVEIRO_DATABASE_URL")
    or os.getenv("DATABASE_URL")
    or "postgresql://postgres:postgres@postgres:5432/chaveiro"
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS mensagens (
    id SERIAL PRIMARY KEY,
    conversation_id BIGINT,
    remetente TEXT,              -- cliente | ia | humano
    conteudo TEXT,
    message_id BIGINT,           -- ID da mensagem no Chatwoot
    criado_em TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS resumos (
    id SERIAL PRIMARY KEY,
    conversation_id BIGINT,
    tipo TEXT,                   -- ia (durante o atendimento) | fechamento (ao resolver)
    dados JSONB,                 -- servico, veiculo, ano, regiao, valor_orcado, valor_final, profissional
    criado_em TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_mensagens_conversa ON mensagens (conversation_id);
CREATE INDEX IF NOT EXISTS idx_mensagens_message_id ON mensagens (message_id);
CREATE INDEX IF NOT EXISTS idx_resumos_conversa ON resumos (conversation_id);

-- Base de serviços fechados: o resumo de fechamento mais recente de cada conversa que tem valor final.
CREATE OR REPLACE VIEW servicos_fechados AS
SELECT DISTINCT ON (conversation_id)
       conversation_id,
       criado_em,
       dados->>'servico'      AS servico,
       dados->>'veiculo'      AS veiculo,
       dados->>'ano'          AS ano,
       dados->>'regiao'       AS regiao,
       dados->>'valor_orcado' AS valor_orcado,
       dados->>'valor_final'  AS valor_final,
       dados->>'profissional' AS profissional
FROM resumos
WHERE tipo = 'fechamento' AND dados->>'valor_final' IS NOT NULL
ORDER BY conversation_id, criado_em DESC;
"""

_pool: asyncpg.Pool | None = None
_trava_pool = asyncio.Lock()


async def _obter_pool() -> asyncpg.Pool:
    """Devolve o pool de conexões, criando-o (e as tabelas) na primeira vez."""
    global _pool
    if _pool is not None:
        return _pool
    async with _trava_pool:
        if _pool is None:
            pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5, timeout=10)
            async with pool.acquire() as conn:
                await conn.execute(_SCHEMA)
            _pool = pool
    return _pool


async def iniciar(tentativas: int = 10, espera_s: float = 2.0) -> None:
    """Conecta ao banco ao subir o servidor, esperando o Postgres ficar pronto se preciso."""
    for n in range(1, tentativas + 1):
        try:
            await _obter_pool()
            log.info("Banco PostgreSQL conectado e tabelas verificadas.")
            return
        except Exception as e:  # noqa: BLE001
            log.warning("Banco indisponível (tentativa %s/%s): %s", n, tentativas, e)
            await asyncio.sleep(espera_s)
    log.error(
        "Não foi possível conectar ao Postgres. O atendimento segue, mas sem registrar histórico "
        "até o banco voltar."
    )


async def encerrar() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


async def registrar_mensagem(conversation_id: int, remetente: str, conteudo: str, message_id: int | None = None) -> None:
    """Grava a mensagem. Se o Chatwoot reenviar o mesmo evento, não duplica."""
    try:
        pool = await _obter_pool()
        async with pool.acquire() as conn:
            if message_id is not None:
                existente = await conn.fetchval("SELECT remetente FROM mensagens WHERE message_id = $1 LIMIT 1", message_id)
                if existente is not None:
                    if remetente == "ia" and existente != "ia":
                        await conn.execute("UPDATE mensagens SET remetente = 'ia' WHERE message_id = $1", message_id)
                    return
            await conn.execute(
                "INSERT INTO mensagens (conversation_id, remetente, conteudo, message_id) VALUES ($1, $2, $3, $4)",
                conversation_id, remetente, conteudo, message_id,
            )
    except Exception as e:  # noqa: BLE001 - falha de registro nunca deve derrubar o atendimento
        log.error("Falha ao gravar mensagem no Postgres: %s: %s", type(e).__name__, e)


async def registrar_resumo(conversation_id: int, tipo: str, dados: dict) -> None:
    if not any(dados.values()):
        return
    try:
        pool = await _obter_pool()
        await pool.execute(
            "INSERT INTO resumos (conversation_id, tipo, dados) VALUES ($1, $2, $3)",
            conversation_id, tipo, json.dumps(dados, ensure_ascii=False),
        )
    except Exception as e:  # noqa: BLE001
        log.error("Falha ao gravar resumo no Postgres: %s: %s", type(e).__name__, e)