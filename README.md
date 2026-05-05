# pedido-process

Camada de orquestração Saga para processamento de pedidos. Coordena **pedido-sys**, **estoque-sys** e **fatura-sys** em sequência, com compensação automática em ordem reversa em caso de falha e recuperação de desastres via estado persistido por etapa.

---

## Visão geral

```
Cliente → POST /process/process-order
              │
              ▼
        ┌─────────────────────────────────────────┐
        │           pedido-process (Saga)          │
        │                                          │
        │  1. POST /orders      → pedido-sys       │
        │  2. POST /inventory/  → estoque-sys      │
        │     reserve                              │
        │  3. POST /billing/    → fatura-sys       │
        │     charge                               │
        │                                          │
        │  Em falha: compensação reversa           │
        │    DELETE /billing/charge/{id}  (log)    │
        │    DELETE /inventory/reserve/{id}        │
        │    DELETE /orders/{id}                   │
        │                                          │
        │  Kafka: evento COMPLETED / COMPENSATING  │
        └─────────────────────────────────────────┘
```

---

## Pré-requisitos

- Mule Runtime 4.11.2
- Java 11+
- Maven 3.8+
- Acesso ao Anypoint Exchange (groupId `c7ee15f6-f8c2-43f4-9069-fdf07fa9ed36`) para baixar os connectors REST Connect e a spec RAML

---

## Executar localmente

```bash
cd pedido-process
mvn clean package
mvn mule:run
```

A aplicação sobe em `http://localhost:8081/process`.

---

## Endpoints

### `POST /process/process-order`

Executa a orquestração Saga completa.

**Request:**
```json
{
  "customer": {
    "customerId": "1002",
    "name": "Maria Souza",
    "email": "maria@email.com",
    "document": "98765432100"
  },
  "order": {
    "item": "Laptop",
    "qty": 1,
    "unitPrice": 2599.99,
    "totalAmount": 2599.99
  },
  "billing": {
    "paymentMethod": "CREDIT_CARD",
    "cardToken": "tok_1234567890"
  },
  "shipping": {
    "address": "Rua das Flores, 123",
    "city": "São Paulo",
    "state": "SP",
    "zipCode": "01234-567"
  }
}
```

**Resposta 200 — Saga concluída:**
```json
{
  "sagaId": "saga-550e8400-e29b-41d4-a716-446655440000",
  "status": "COMPLETED",
  "message": "Order processed successfully",
  "executionTime": "PT1.234S",
  "results": {
    "orderResult":    { "orderId": "ORD-001", "status": "CREATED" },
    "inventoryResult":{ "reservationId": "RES-001", "status": "RESERVED" },
    "billingResult":  { "chargeId": "CHG-001", "status": "CHARGED" }
  }
}
```

**Resposta 500 — Saga compensada:**
```json
{
  "sagaId": "saga-550e8400-e29b-41d4-a716-446655440000",
  "status": "COMPENSATED",
  "message": "Order processing failed. Compensation completed.",
  "executionTime": "PT0.987S",
  "error": {
    "errorType": "FATURA-API:INTERNAL_SERVER_ERROR",
    "description": "Payment declined",
    "timestamp": "2026-05-04T18:00:00Z"
  },
  "compensationActions": [
    "BILLING_COMPENSATED",
    "INVENTORY_RELEASED",
    "ORDER_CANCELLED"
  ]
}
```

---

### `GET /process/saga-status/{sagaId}`

Consulta o estado persistido de uma saga. Útil para diagnóstico e recuperação de desastres.

```bash
curl http://localhost:8081/process/saga-status/saga-550e8400-e29b-41d4-a716-446655440000
```

**Resposta 200:**
```json
{
  "sagaId": "saga-550e8400-e29b-41d4-a716-446655440000",
  "status": "ESTOQUE_COMPLETED",
  "lastCompletedStep": "ESTOQUE",
  "orderId": "ORD-001",
  "reservationId": "RES-001",
  "startTime": "2026-05-04T17:59:59Z",
  "originalRequest": { "..." }
}
```

**Resposta 404:** saga expirada (TTL 24 h) ou nunca existiu.

---

## Estados da Saga

```
STARTED → PEDIDO_COMPLETED → ESTOQUE_COMPLETED → FATURA_COMPLETED → COMPLETED
                ↓                    ↓                   ↓
           COMPENSATED          COMPENSATED         COMPENSATED
```

Cada transição é gravada no **Object Store persistente** (TTL 24 h). Se o nó reiniciar durante a execução, o último estado salvo fica disponível via `GET /saga-status/{sagaId}`, permitindo identificar a etapa de falha e decidir sobre reprocessamento.

---

## Eventos Kafka

| Evento       | Tópico       | Quando                              |
|--------------|--------------|-------------------------------------|
| `COMPLETED`  | `pedido.new` | Todas as 3 etapas concluídas        |
| `COMPENSATING`| `pedido.new`| Falha + compensação executada       |

Payload publicado contém: `sagaId`, `timestamp`, `originalRequest`, `apiResponses`, `sagaStatus`, `executionTime`, `compensationActions`.

---

## Configuração (`config.yaml`)

| Propriedade | Valor padrão | Descrição |
|---|---|---|
| `http.listener.host` | `0.0.0.0` | Host do listener HTTP |
| `http.listener.port` | `8081` | Porta local |
| `kafka.bootstrapServers` | `cloud-services.demos.mulesoft.com:30858` | Bootstrap do Kafka |
| `kafka.topic` | `pedido.new` | Tópico de eventos |
| `apis.pedido.host` | `pedido-sys-sh06qr.qfq181.bra-s1.cloudhub.io` | Host da pedido-sys |
| `apis.estoque.host` | `estoque-sys-sh06qr.qfq181.bra-s1.cloudhub.io` | Host da estoque-sys |
| `apis.fatura.host` | `fatura-sys-sh06qr.qfq181.bra-s1.cloudhub.io` | Host da fatura-sys |

---

## Arquitetura interna

| Arquivo | Responsabilidade |
|---|---|
| `global.xml` | Configs globais: HTTP listener, APIKit, REST Connect connectors, Kafka producer, Object Store |
| `pedido-process.xml` | Flows: main APIKit, orquestração POST, consulta GET, compensação, publicação Kafka |
| `config.yaml` | Propriedades externalizadas (hosts, portas, tópico) |

### Dependências principais (pom.xml)

| Artefato | Versão | Função |
|---|---|---|
| `mule-http-connector` | 1.9.0 | HTTP listener |
| `mule-apikit-module` | 1.11.15 | Roteamento baseado na spec RAML |
| `mule-kafka-connector` | 4.13.0 | Publicação de eventos |
| `mule-objectstore-connector` | 1.3.0 | Persistência do estado da saga |
| `mule-plugin-pedido-api-spec` | 1.0.0 | REST Connect connector — pedido-sys |
| `mule-plugin-estoque-api-spec` | 1.0.0 | REST Connect connector — estoque-sys |
| `mule-plugin-fatura-api-spec` | 1.0.0 | REST Connect connector — fatura-sys |
| `pedido-process-spec` | 1.0.3 | Spec RAML do próprio serviço (Exchange) |

---

## Testes rápidos

### Caminho feliz
```bash
curl -s -X POST http://localhost:8081/process/process-order \
  -H "Content-Type: application/json" \
  -d '{
    "customer": {"customerId":"1002","name":"Maria Souza","email":"maria@email.com","document":"98765432100"},
    "order": {"item":"Laptop","qty":1,"unitPrice":2599.99,"totalAmount":2599.99},
    "billing": {"paymentMethod":"CREDIT_CARD","cardToken":"tok_1234567890"},
    "shipping": {"address":"Rua das Flores, 123","city":"São Paulo","state":"SP","zipCode":"01234-567"}
  }' | python3 -m json.tool
```
Esperado: HTTP 200, `"status": "COMPLETED"`.

### Forçar compensação
```bash
# customerId "1001" → fatura-sys retorna erro, saga executa rollback
curl -s -X POST http://localhost:8081/process/process-order \
  -H "Content-Type: application/json" \
  -d '{
    "customer": {"customerId":"1001","name":"João Silva","email":"joao@email.com","document":"12345678900"},
    "order": {"item":"Geladeira","qty":1,"unitPrice":3500.00,"totalAmount":3500.00},
    "billing": {"paymentMethod":"CREDIT_CARD","cardToken":"tok_fail"},
    "shipping": {"address":"Av. Brasil, 500","city":"Rio de Janeiro","state":"RJ","zipCode":"20040-020"}
  }' | python3 -m json.tool
```
Esperado: HTTP 500, `"status": "COMPENSATED"`, `compensationActions` com as 3 ações.

### Recuperação de desastres
```bash
# 1. Capturar sagaId da resposta acima
SAGA_ID="saga-xxxx-yyyy-zzzz"

# 2. Consultar estado persistido
curl -s http://localhost:8081/process/saga-status/$SAGA_ID | python3 -m json.tool
```
