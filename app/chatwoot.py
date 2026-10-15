"""Integração com a API do Chatwoot: enviar mensagens, atualizar atributos e ler o histórico."""
import logging
import os

import httpx
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger("chaveiro.chatwoot")

CHATWOOT_URL = os.getenv("CHATWOOT_URL", "http://localhost:3000").rstrip("/")
API_TOKEN = os.getenv("CHATWOOT_API_TOKEN", "")
ACCOUNT_ID_PADRAO = int(os.getenv("CHATWOOT_ACCOUNT_ID", "2"))


def _headers() -> dict:
    return {"api_access_token": API_TOKEN, "Content-Type": "application/json"}


def _novo_cliente() -> httpx.AsyncClient:
    return httpx.AsyncClient(timeout=15.0)


def _base(account_id: int, conversation_id: int) -> str:
    return f"{CHATWOOT_URL}/api/v1/accounts/{account_id}/conversations/{conversation_id}"


async def enviar_resposta_chatwoot(
    account_id: int, conversation_id: int, mensagem: str, privada: bool = False
) -> int | None:
    """Envia uma mensagem na conversa e devolve o ID dela (None se falhar).

    Com privada=True vira nota interna (o cliente não vê). O ID serve para o sistema reconhecer,
    mais tarde, as mensagens que ele mesmo enviou.
    """
    payload = {"content": mensagem, "message_type": "outgoing", "private": privada}
    async with _novo_cliente() as client:
        resp = await client.post(f"{_base(account_id, conversation_id)}/messages", json=payload, headers=_headers())
    if not resp.is_success:
        log.error(
            "Falha ao enviar mensagem na conversa #%s: HTTP %s - %s",
            conversation_id, resp.status_code, resp.text[:300],
        )
        return None
    dados = resp.json() if resp.content else {}
    return dados.get("id") or -1  # -1: enviada, mas o Chatwoot não devolveu o ID


async def atualizar_atributos_chatwoot(account_id: int, conversation_id: int, atributos: dict) -> bool:
    """Atualiza os atributos personalizados da conversa (lateral 'Informação da conversa').

    A API exige o envelope {"custom_attributes": {...}}. Sem ele o Chatwoot responde 500.
    """
    filtrados = {k: v for k, v in atributos.items() if v not in (None, "")}
    if not filtrados:
        return True
    url = f"{_base(account_id, conversation_id)}/custom_attributes"
    async with _novo_cliente() as client:
        resp = await client.post(url, json={"custom_attributes": filtrados}, headers=_headers())
    if not resp.is_success:
        log.error(
            "Falha ao atualizar atributos da conversa #%s: HTTP %s - %s",
            conversation_id, resp.status_code, resp.text[:300],
        )
        return False
    return True


async def buscar_historico(account_id: int, conversation_id: int, limite: int = 20) -> list[dict]:
    """Lê as últimas mensagens da conversa, em ordem cronológica.

    Retorna itens no formato {"papel": "cliente" | "atendente", "texto": "..."}.
    Notas privadas e mensagens de sistema (ex.: 'John atribuiu a conversa') são ignoradas.
    """
    async with _novo_cliente() as client:
        resp = await client.get(f"{_base(account_id, conversation_id)}/messages", headers=_headers())
    if not resp.is_success:
        log.error(
            "Falha ao buscar histórico da conversa #%s: HTTP %s - %s",
            conversation_id, resp.status_code, resp.text[:300],
        )
        return []

    mensagens = resp.json().get("payload", [])
    mensagens.sort(key=lambda m: m.get("created_at") or 0)

    historico = []
    for m in mensagens:
        if m.get("private"):
            continue
        texto = (m.get("content") or "").strip()
        if not texto:
            continue
        tipo = m.get("message_type")
        if tipo in (0, "incoming"):
            papel = "cliente"
        elif tipo in (1, "outgoing"):
            papel = "atendente"
        else:
            continue
        historico.append({"papel": papel, "texto": texto})
    return historico[-limite:]