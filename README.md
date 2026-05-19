# 🚀 Watermill Study - Event Driven Architecture in Go

Projeto de estudo de **arquitetura event-driven** em Go utilizando a biblioteca [Watermill](https://github.com/ThreeDotsLabs/watermill), com um broker em memória baseado em **GoChannel**.

O objetivo é entender na prática:

- Pub/Sub em Go
- Processamento assíncrono de eventos
- Middleware pipeline
- Retry e tratamento de falhas
- Dead Letter Queue (DLQ)
- Propagação de correlation ID
- Arquitetura em camadas

---

# 🧠 Arquitetura do projeto

Estrutura:
```
internal/
├── domain/ # entidades e contratos
├── usecase/ # regras de negócio
├── messaging/ # handlers + router Watermill
├── infra/
│ └── bus/ # implementação GoChannel
```
---

# ⚙️ Stack

- Go 1.25+
- Watermill
- GoChannel (broker em memória)

---

# 📦 Fluxo do sistema

## 🔄 Pipeline de execução

Publisher → GoChannel → Router → Middleware → Handler → Usecase

---

# 🧾 Evento principal

```go
type OrderCreated struct {
	OrderID string
	UserID  string
	Amount  float64
}
```

## 🚀 Como executar
```bash
go run . -count=4
```
Parâmetro:

count: número de mensagens publicadas

🧠 Middleware pipeline

O router aplica os seguintes middlewares:

1. Recover de panic

Captura panics no usecase/handler e evita crash do sistema.

2. EnsureCorrelationID (custom)

Garante que toda mensagem tenha um:

correlation_id

Se não existir, ele é gerado automaticamente.

3. CorrelationID (Watermill)

Propaga o correlation_id através do pipeline.

4. Retry middleware

MaxRetries: 2
InitialInterval: 200ms

5. Poison Queue (DLQ)

Mensagens com erro são enviadas para:

order-created-dlq

Exceções de negócio não são enviadas para DLQ:

ErrSaldoInsuficiente
ErrEstoqueIndisponivel

🧩 Handler

func OrderHandler(msg *message.Message) error

Responsável por:

- Log de entrada da mensagem
- Leitura do correlation_id
- Deserialização do payload
- Chamada do usecase

Exemplo de log:

🔥 CHEGOU NO HANDLER

handler started | msg_id=... correlation_id=...

🧠 Usecase

func ProcessOrder(order domain.Order) error

Regras:

ord-3 → panic simulado
Amount > 1000 → saldo insuficiente
Amount > 500 → estoque indisponível
Amount == 0 → erro inválido

Caso válido:

processando pedido order_id=ord-1 amount=250.000000
🏗️ Bus (GoChannel)

Implementação em memória:

gochannel.NewGoChannel(...)

Interface:

type Bus interface {
	message.Publisher
	message.Subscriber
}

🧪 Correlation ID

O sistema garante rastreabilidade usando:

- EnsureCorrelationID middleware
- CorrelationID middleware
- Metadata da mensagem

Exemplo:

correlation_id=550e8400-e29b-41d4-a716-446655440000

📤 Publisher

As mensagens são publicadas assim:

msg := message.NewMessage(watermill.NewUUID(), payload)
bus.Publish("order-created", msg)

⚠️ Limitações do projeto

Este projeto usa GoChannel, portanto:

- Não há persistência
- Não há distribuição real
- Mensagens vivem apenas em memória
- Serve apenas para estudo

🚀 Evoluções futuras

Sugestões de evolução:

- Substituir GoChannel por Kafka
- Adicionar OpenTelemetry (tracing distribuído)
- Logs estruturados (JSON)
- Separar producer e consumer em processos diferentes
- Criar observabilidade completa

📚 Referências
https://github.com/ThreeDotsLabs/watermill
https://watermill.io/

👨‍💻 Autor
Projeto de estudo de mensageria e arquitetura event-driven com Go + Watermill.
