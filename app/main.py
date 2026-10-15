"""Servidor de webhook: recebe os eventos do Chatwoot e aciona a IA."""
import asyncio
import logging
import os
import secrets
from collections import OrderedDict
from contextlib import asynccontextmanager
from datetime import datetime, time, timedelta, timezone

from dotenv import load_dotenv
from fastapi import BackgroundTasks, FastAPI, HTTPException, Request

from app import ai_processor, chatwoot, db

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    handlers=[
        logging.FileHandler(os.getenv("LOG_ARQUIVO", "atendimento.log"), encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("chaveiro.main")

# ---------------------------------------------------------------------------
# Configuração (tudo vem do .env)
# ---------------------------------------------------------------------------
WEBHOOK_TOKEN = os.getenv("WEBHOOK_TOKEN", "")
IA_HORARIO_INICIO = os.getenv("IA_HORARIO_INICIO", "")  # ex.: "22:00" (vazio = sem restrição de horário)
IA_HORARIO_FIM = os.getenv("IA_HORARIO_FIM", "")        # ex.: "07:00"
ETIQUETA_IA = os.getenv("ETIQUETA_IA", "ia").lower()
ETIQUETA_HUMANO = os.getenv("ETIQUETA_HUMANO", "humano").lower()
MENSAGEM_FALHA = os.getenv(
    "MENSAGEM_FALHA", "Recebi sua mensagem! Já estou verificando e retorno em instantes."
)
# Quando você (ou qualquer humano) escreve na conversa, a IA fica quieta por este tempo.
# Cada nova mensagem sua renova a contagem. 0 desliga a pausa automática.
PAUSA_HUMANO_MIN = float(os.getenv("PAUSA_HUMANO_MIN", "30"))
# Espera antes de decidir se uma mensagem de saída foi da IA ou de um humano.
ATRASO_DETECCAO_S = float(os.getenv("ATRASO_DETECCAO_S", "3"))
FUSO = timezone(timedelta(hours=-3))  # Brasília (sem horário de verão desde 2019)

# Chave geral: desliga/liga a IA em todas as conversas, sem reiniciar o servidor.
estado = {"ia_ativa": os.getenv("IA_ATIVA", "true").lower() == "true"}

_processadas: "OrderedDict[int, bool]" = OrderedDict()  # IDs de mensagens já tratadas (evita duplicidade)
_travas: dict[int, asyncio.Lock] = {}                   # uma conversa é processada por vez
_ids_ia: "OrderedDict[int, bool]" = OrderedDict()       # IDs das mensagens que a própria IA enviou
_pausas: dict[int, datetime] = {}                       # conversa -> até quando a IA fica quieta


@asynccontextmanager
async def lifespan(_app: FastAPI):
    await db.iniciar()
    if not WEBHOOK_TOKEN:
        log.warning("WEBHOOK_TOKEN não definido: o webhook aceita chamadas de qualquer pessoa com a URL.")
    yield
    await db.encerrar()


app = FastAPI(lifespan=lifespan)


# ---------------------------------------------------------------------------
# Regras: quando a IA responde
# ---------------------------------------------------------------------------
def _parse_hora(texto: str) -> time:
    horas, minutos = texto.strip().split(":")
    return time(int(horas), int(minutos))


def _dentro_do_horario(agora: datetime | None = None) -> bool:
    if not (IA_HORARIO_INICIO and IA_HORARIO_FIM):
        return True
    atual = (agora or datetime.now(FUSO)).time()
    inicio, fim = _parse_hora(IA_HORARIO_INICIO), _parse_hora(IA_HORARIO_FIM)
    if inicio <= fim:
        return inicio <= atual < fim
    return atual >= inicio or atual < fim  # janela que atravessa a meia-noite (ex.: 22:00 às 07:00)


def _pausar(conversation_id: int) -> datetime:
    ate = datetime.now(FUSO) + timedelta(minutes=PAUSA_HUMANO_MIN)
    _pausas[conversation_id] = ate
    return ate


def _pausada_ate(conversation_id: int) -> datetime | None:
    ate = _pausas.get(conversation_id)
    if ate and datetime.now(FUSO) < ate:
        return ate
    _pausas.pop(conversation_id, None)
    return None


def ia_deve_responder(etiquetas: list[str], conversation_id: int | None = None) -> tuple[bool, str]:
    """Prioridade: chave geral > etiqueta 'humano' > pausa por humano > etiqueta 'ia' > horário programado."""
    etiquetas_norm = {str(e).lower() for e in (etiquetas or [])}
    if not estado["ia_ativa"]:
        return False, "chave geral desligada"
    if ETIQUETA_HUMANO in etiquetas_norm:
        return False, f"etiqueta '{ETIQUETA_HUMANO}' na conversa"
    ate = _pausada_ate(conversation_id) if conversation_id is not None else None
    if ate:
        return False, f"humano atendeu há pouco (pausa até {ate.strftime('%H:%M')})"
    if ETIQUETA_IA in etiquetas_norm:
        return True, f"etiqueta '{ETIQUETA_IA}' na conversa"
    if not _dentro_do_horario():
        return False, "fora do horário programado da IA"
    return True, "dentro do horário (ou sem horário definido)"


# ---------------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------------
def _autorizado(request: Request) -> bool:
    if not WEBHOOK_TOKEN:
        return True
    recebido = request.query_params.get("token") or request.headers.get("x-webhook-token") or ""
    return secrets.compare_digest(recebido, WEBHOOK_TOKEN)


def _exigir_token(request: Request) -> None:
    if not _autorizado(request):
        raise HTTPException(status_code=401, detail="token inválido")


def _ja_processada(message_id) -> bool:
    if message_id is None:
        return False
    if message_id in _processadas:
        return True
    _processadas[message_id] = True
    if len(_processadas) > 2000:
        _processadas.popitem(last=False)
    return False


def _trava(conversation_id: int) -> asyncio.Lock:
    if conversation_id not in _travas:
        _travas[conversation_id] = asyncio.Lock()
    return _travas[conversation_id]


def _tipo_mensagem(valor) -> str:
    if valor in (0, "incoming"):
        return "incoming"
    if valor in (1, "outgoing"):
        return "outgoing"
    return str(valor)


def _ids(payload: dict) -> tuple[int | None, int]:
    conversa = payload.get("conversation") or {}
    conversation_id = conversa.get("id") or payload.get("conversation_id")
    account_id = (payload.get("account") or {}).get("id") or conversa.get("account_id") or chatwoot.ACCOUNT_ID_PADRAO
    return conversation_id, account_id


# ---------------------------------------------------------------------------
# Tarefas em segundo plano (o webhook responde ao Chatwoot na hora e processa depois)
# ---------------------------------------------------------------------------
async def _enviar_ia(account_id: int, conversation_id: int, texto: str, privada: bool = False) -> int | None:
    """Envia pela IA e anota o ID, para depois distinguir o que foi da IA do que foi de um humano."""
    mid = await chatwoot.enviar_resposta_chatwoot(account_id, conversation_id, texto, privada=privada)
    if mid and not privada:
        _ids_ia[mid] = True
        if len(_ids_ia) > 2000:
            _ids_ia.popitem(last=False)
        await db.registrar_mensagem(conversation_id, "ia", texto, mid)
    return mid


async def registrar_saida(conversation_id: int, content: str, message_id: int | None) -> None:
    """Mensagem de saída: se não foi a IA que enviou, um humano falou e a IA fica quieta por um tempo."""
    try:
        await asyncio.sleep(ATRASO_DETECCAO_S)  # dá tempo de a IA anotar o ID da própria mensagem
        if message_id is not None and message_id in _ids_ia:
            return
        await db.registrar_mensagem(conversation_id, "humano", content, message_id)
        if PAUSA_HUMANO_MIN > 0:
            ate = _pausar(conversation_id)
            log.info("Conversa #%s: humano atendeu; IA pausada até %s.", conversation_id, ate.strftime("%H:%M"))
    except Exception:  # noqa: BLE001
        log.exception("Erro ao registrar mensagem de saída da conversa #%s", conversation_id)


async def _avisar_falha(account_id: int, conversation_id: int, erro: Exception) -> None:
    await _enviar_ia(account_id, conversation_id, f"⚠️ IA indisponível no momento: {erro}", privada=True)
    if MENSAGEM_FALHA:
        await _enviar_ia(account_id, conversation_id, MENSAGEM_FALHA)


async def atender_mensagem(account_id: int, conversation_id: int, content: str, etiquetas: list[str]) -> None:
    try:
        permitido, motivo = ia_deve_responder(etiquetas, conversation_id)
        if not permitido:
            log.info("Conversa #%s: IA não responde (%s).", conversation_id, motivo)
            return

        async with _trava(conversation_id):
            historico = await chatwoot.buscar_historico(account_id, conversation_id)
            atual = {"papel": "cliente", "texto": content}
            if not historico or historico[-1] != atual:
                historico.append(atual)

            try:
                resultado = await ai_processor.processar_mensagem(historico)
            except ai_processor.LLMIndisponivel as erro:
                log.error("Conversa #%s: IA indisponível (%s).", conversation_id, erro)
                await _avisar_falha(account_id, conversation_id, erro)
                return

            enviada = await _enviar_ia(account_id, conversation_id, resultado["resposta_cliente"])
            log.info(
                "Conversa #%s: resposta %s (provedor %s).",
                conversation_id, "enviada" if enviada else "NÃO enviada", resultado["provedor"],
            )

            atributos = resultado["atributos"]
            if await chatwoot.atualizar_atributos_chatwoot(account_id, conversation_id, atributos):
                log.info("Conversa #%s: atributos atualizados %s", conversation_id, atributos)
            await db.registrar_resumo(conversation_id, "ia", atributos)
    except Exception:  # noqa: BLE001
        log.exception("Erro inesperado ao atender a conversa #%s", conversation_id)


async def fechar_atendimento(account_id: int, conversation_id: int) -> None:
    """Ao resolver a conversa, a IA lê tudo e preenche os atributos finais (inclusive valores)."""
    try:
        historico = await chatwoot.buscar_historico(account_id, conversation_id, limite=60)
        if not any(item["papel"] == "cliente" for item in historico):
            log.info("Conversa #%s resolvida sem mensagens do cliente; nada a resumir.", conversation_id)
            return
        resumo = await ai_processor.extrair_resumo(historico)
        await chatwoot.atualizar_atributos_chatwoot(account_id, conversation_id, resumo)
        await db.registrar_resumo(conversation_id, "fechamento", resumo)
        log.info("Conversa #%s: resumo de fechamento %s", conversation_id, resumo)
    except Exception:  # noqa: BLE001
        log.exception("Erro ao resumir a conversa #%s", conversation_id)


# ---------------------------------------------------------------------------
# Rotas
# ---------------------------------------------------------------------------
def _tratar_mensagem(payload: dict, background: BackgroundTasks) -> dict:
    conversation_id, account_id = _ids(payload)
    if not conversation_id:
        return {"status": "ignored", "reason": "conversa não identificada"}

    content = (payload.get("content") or "").strip()
    message_id = payload.get("id")
    tipo = _tipo_mensagem(payload.get("message_type"))

    if payload.get("private"):
        return {"status": "ignored", "reason": "nota privada"}

    if tipo == "outgoing":
        # Mensagem de saída nunca dispara resposta (evita loop). Se não foi a IA, um humano falou: pausa a IA.
        if content:
            background.add_task(registrar_saida, conversation_id, content, message_id)
        return {"status": "ignored", "reason": "mensagem de saída"}

    if tipo != "incoming" or not content:
        return {"status": "ignored", "reason": "sem texto de cliente"}

    if _ja_processada(message_id):
        return {"status": "ignored", "reason": "mensagem duplicada"}

    etiquetas = (payload.get("conversation") or {}).get("labels") or []
    log.info("Conversa #%s: mensagem recebida: %r", conversation_id, content)
    background.add_task(db.registrar_mensagem, conversation_id, "cliente", content, message_id)
    background.add_task(atender_mensagem, account_id, conversation_id, content, etiquetas)
    return {"status": "accepted"}


def _tratar_status(payload: dict, background: BackgroundTasks) -> dict:
    if payload.get("status") != "resolved":
        return {"status": "ignored", "reason": "conversa não resolvida"}
    conversation_id = payload.get("id") or (payload.get("conversation") or {}).get("id")
    _, account_id = _ids(payload)
    if not conversation_id:
        return {"status": "ignored", "reason": "conversa não identificada"}
    background.add_task(fechar_atendimento, account_id, conversation_id)
    return {"status": "accepted", "reason": "resumo de fechamento agendado"}


@app.post("/webhook")
async def webhook(request: Request, background: BackgroundTasks):
    _exigir_token(request)
    try:
        payload = await request.json()
    except Exception:  # noqa: BLE001
        raise HTTPException(status_code=400, detail="JSON inválido")
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="payload inesperado")

    evento = payload.get("event")
    if evento in (None, "message_created"):
        return _tratar_mensagem(payload, background)
    if evento == "conversation_status_changed":
        return _tratar_status(payload, background)
    return {"status": "ignored", "reason": f"evento '{evento}' não tratado"}


@app.get("/")
async def raiz():
    return {"status": "ok", "ia_ativa": estado["ia_ativa"]}


@app.get("/ia/status")
async def ia_status(request: Request):
    _exigir_token(request)
    return {
        "ia_ativa": estado["ia_ativa"],
        "horario_programado": f"{IA_HORARIO_INICIO} às {IA_HORARIO_FIM}" if IA_HORARIO_INICIO and IA_HORARIO_FIM else None,
        "dentro_do_horario": _dentro_do_horario(),
    }


@app.post("/ia/retomar")
async def ia_retomar(request: Request, conversa: int):
    """Encerra a pausa de uma conversa antes do tempo: POST /ia/retomar?conversa=ID&token=..."""
    _exigir_token(request)
    _pausas.pop(conversa, None)
    log.info("Conversa #%s: pausa encerrada manualmente.", conversa)
    return {"conversa": conversa, "pausada": False}


@app.post("/ia/ligar")
async def ia_ligar(request: Request):
    _exigir_token(request)
    estado["ia_ativa"] = True
    log.info("Chave geral: IA LIGADA.")
    return {"ia_ativa": True}


@app.post("/ia/desligar")
async def ia_desligar(request: Request):
    _exigir_token(request)
    estado["ia_ativa"] = False
    log.info("Chave geral: IA DESLIGADA.")
    return {"ia_ativa": False}