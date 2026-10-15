# Uso:
#   .\teste_webhook.ps1                      -> simula uma mensagem de cliente
#   .\teste_webhook.ps1 -Mensagem "Aceito"   -> simula outra mensagem
#   .\teste_webhook.ps1 -Fechar              -> simula "Resolver conversa" (IA preenche os valores finais)
#   .\teste_webhook.ps1 -Token "sua-senha" -ConversaId 5
param(
    [string]$Mensagem = "Olá, preciso de um orçamento para fazer uma chave cópia de um Onix 2020 aqui na região do Grajaú.",
    [int]$ConversaId = 3,
    [int]$ContaId = 2,
    [string]$Token = "",
    [switch]$Fechar
)

$url = "http://127.0.0.1:8000/webhook"
if ($Token) { $url += "?token=$Token" }

if ($Fechar) {
    $bodyObj = @{
        event   = "conversation_status_changed"
        id      = $ConversaId
        status  = "resolved"
        account = @{ id = $ContaId }
    }
} else {
    $bodyObj = @{
        id           = Get-Random -Minimum 100000 -Maximum 999999999
        event        = "message_created"
        content      = $Mensagem
        message_type = "incoming"
        account      = @{ id = $ContaId }
        conversation = @{ id = $ConversaId; labels = @() }
        sender       = @{ name = "Katia Silva Fidelis"; phone_number = "+5511999999999"; type = "contact" }
    }
}

$json  = $bodyObj | ConvertTo-Json -Depth 5
$bytes = [System.Text.Encoding]::UTF8.GetBytes($json)
Invoke-RestMethod -Uri $url -Method Post -Body $bytes -ContentType "application/json; charset=utf-8"
