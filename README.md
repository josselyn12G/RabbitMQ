# BikeStore Async

Sistema asíncrono de procesamiento de pedidos para una tienda de bicicletas, basado en **Spring Boot 3.3.5** y **RabbitMQ**. La aplicación implementa un patrón de arquitectura orientada a eventos con procesamiento distribuido para pagos y notificaciones de correo.

## 📋 Características

- **API REST** para crear pedidos
- **Arquitectura asíncrona** basada en colas de mensajes (RabbitMQ)
- **Procesamiento distribuido** con múltiples workers
- **Reintentos automáticos** para pagos fallidos
- **Dead Letter Queue (DLQ)** para mensajes no procesables
- **Logging centralizado** con trazabilidad de pedidos

## 🏗️ Arquitectura

La aplicación utiliza un patrón **Productor-Consumidor** con la siguiente flujo:

```
Cliente HTTP
    ↓
OrderProducer (REST Controller)
    ↓
RabbitMQ Exchange (bikestore.exchange)
    ├─→ order.created (routing key)
    │   └─→ PaymentConsumer (Payment Processing)
    │       ├─ Aprobado → payment.paid
    │       └─ Rechazado → Reintentos (max 3)
    │                    → Dead Letter Queue
    │
    └─→ payment.paid (routing key)
        └─→ EmailConsumer (Email Notification)
```

## 🔧 Componentes

### 1. **RabbitConfig** (`RabbitConfig.java`)
Configuración centralizada de RabbitMQ:
- Exchange principal: `bikestore.exchange` (tipo Direct)
- Colas:
  - `order.queue`: Recibe pedidos creados
  - `email.queue`: Recibe confirmaciones de pago
  - `order.dlq`: Dead Letter Queue para fallos definitivos

### 2. **OrderProducer** (`OrderProducer.java`)
Controlador REST que expone el endpoint para crear pedidos:
- **Endpoint**: `POST /api/orders`
- **Payload**: 
  ```json
  {
    "clienteNombre": "Juan Pérez",
    "clienteEmail": "juan@example.com",
    "producto": "Mountain Bike XL",
    "total": 1500.00
  }
  ```
- **Respuesta**: ID del pedido generado y estado inicial (PENDING)
- Publica el pedido en RabbitMQ con routing key `order.created`

### 3. **PaymentConsumer** (`PaymentConsumer.java`)
Worker que procesa pagos de forma asíncrona:
- Consume mensajes de `order.queue`
- Simula el procesamiento de pago (50% de éxito)
- **Lógica de reintentos**:
  - Máximo 3 intentos
  - Si falla después del tercer intento → DLQ
- Si el pago es aprobado:
  - Actualiza estado a `PAID`
  - Publica en RabbitMQ con routing key `payment.paid`

### 4. **EmailConsumer** (`EmailConsumer.java`)
Worker que envía notificaciones por correo:
- Consume mensajes de `email.queue`
- Verifica que el estado sea `PAID`
- Simula el envío de correo de confirmación
- Registra en logs el envío exitoso

## 🚀 Requisitos

- **Java 17** o superior
- **Spring Boot 3.3.5**
- **RabbitMQ** corriendo en `localhost:5672`
  - Usuario: `guest`
  - Contraseña: `guest`
- **Maven 3.6+** para compilación

## 📦 Dependencias

```xml
<dependencies>
  <!-- Spring Boot Web -->
  <dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-web</artifactId>
  </dependency>

  <!-- RabbitMQ Client -->
  <dependency>
    <groupId>com.rabbitmq</groupId>
    <artifactId>amqp-client</artifactId>
    <version>5.22.0</version>
  </dependency>

  <!-- JSON Processing -->
  <dependency>
    <groupId>org.json</groupId>
    <artifactId>json</artifactId>
    <version>20240303</version>
  </dependency>
</dependencies>
```

## 🛠️ Instalación y Ejecución

### 1. Verificar que RabbitMQ esté corriendo

```bash
# Iniciar RabbitMQ (si no está activo)
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4-management
```

Accede al panel de administración: http://localhost:15672 (usuario: `guest`, contraseña: `guest`)

### 2. Compilar el proyecto

```bash
cd bikestore
mvn clean install
```

### 3. Ejecutar la aplicación

```bash
mvn spring-boot:run
```

O ejecutar el JAR compilado:

```bash
java -jar target/bikestore-1.0-SNAPSHOT.jar
```

La aplicación se inicia en `http://localhost:8080`

## 📝 Uso

### Crear un pedido

```bash
curl -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "clienteNombre": "Juan Pérez",
    "clienteEmail": "juan@example.com",
    "producto": "Road Bike Pro",
    "total": 2500.00
  }'
```

**Respuesta exitosa:**

```json
{
  "mensaje": "Pedido enviado correctamente a RabbitMQ",
  "pedidoId": "PED-1716188317903",
  "estado": "PENDING"
}
```

### Monitorear el flujo

Observa los logs en la consola:

```
[2024-05-19T19:58:37.123] [pedidoId=PED-1716188317903] [thread=main] OrderProducer publicó el pedido en RabbitMQ
[2024-05-19T19:58:37.456] [pedidoId=PED-1716188317903] [thread=PaymentWorker-Thread] PaymentWorker recibió pedido. Intento: 1
[2024-05-19T19:58:37.789] [pedidoId=PED-1716188317903] [thread=PaymentWorker-Thread] Pago aprobado. Mensaje enviado a EmailWorker
[2024-05-19T19:58:37.912] [pedidoId=PED-1716188317903] [thread=EmailWorker-Thread] EmailWorker envió correo de confirmación a juan@example.com por el producto: Road Bike Pro
```

## 🔄 Flujo Completo

1. **Cliente HTTP** envía una solicitud POST a `/api/orders`
2. **OrderProducer** genera un ID único y publica en RabbitMQ
3. **PaymentConsumer** consume el pedido y procesa el pago:
   - Si es aprobado → publica con routing key `payment.paid`
   - Si falla → reintenta (máx 3 intentos)
   - Si agota reintentos → envía a DLQ
4. **EmailConsumer** consume el evento de pago aprobado y envía confirmación por correo
5. Todos los eventos quedan registrados en los logs

## 📊 Configuración de Colas y Bindings

| Cola | Exchange | Routing Key | Descripción |
|------|----------|-------------|-------------|
| `order.queue` | `bikestore.exchange` | `order.created` | Recibe pedidos nuevos |
| `email.queue` | `bikestore.exchange` | `payment.paid` | Recibe pagos aprobados |
| `order.dlq` | (default) | `order.dlq` | Mensajes fallidos definitivamente |

## ⚙️ Configuración

Todas las configuraciones de RabbitMQ se encuentran en `RabbitConfig.java`:

```java
// Host de RabbitMQ
factory.setHost("localhost");

// Credenciales
factory.setUsername("guest");
factory.setPassword("guest");
```

Para usar otro servidor, modifica estos valores en `RabbitConfig.getConnection()`.

## 📈 Monitoreo

### Consola de RabbitMQ

Accede a http://localhost:15672 para ver en tiempo real:
- Mensajes en las colas
- Consumers conectados
- Estadísticas de procesamiento
- Mensajes en la DLQ

### Logs de la Aplicación

La aplicación registra cada evento con timestamp y traceabilidad:

```
[timestamp] [pedidoId=PED-XXX] [thread=nombre-thread] evento
```

## 🔐 Seguridad y Confiabilidad

- **Persistencia**: Las colas están configuradas como persistentes
- **Reintentos**: Máximo 3 intentos para pagos fallidos
- **Dead Letter Queue**: Mensajes no procesables quedan en DLQ para análisis
- **Confirmación de Mensajes**: Cada worker confirma la recepción (`basicAck`)

## 📝 Estructura de Directorios

```
bikestore/
├── pom.xml                          # Configuración Maven
├── src/
│   └── main/
│       └── java/
│           └── com/
│               └── bikestore/
│                   ├── App.java                      # Aplicación principal
│                   ├── RabbitConfig.java             # Configuración de RabbitMQ
│                   ├── OrderProducer.java            # Endpoint REST para pedidos
│                   ├── PaymentConsumer.java          # Worker de pagos
│                   └── EmailConsumer.java            # Worker de correos
└── target/                          # Artefactos compilados
```

## 🐛 Troubleshooting

### Error: Connection refused (RabbitMQ)

**Solución**: Asegúrate que RabbitMQ esté corriendo en `localhost:5672`

```bash
docker ps | grep rabbitmq
```

### Mensajes en la DLQ

Indica que los pagos fallaron después de 3 intentos. Revisa los logs para más detalles.

### No aparecen logs de workers

Verifica que la aplicación esté ejecutándose y que RabbitMQ esté activo. Los workers se inician en hilos separados al iniciar Spring Boot.

## 📄 Licencia

Este proyecto es de propósito educativo y es parte de la arquitectura de BikeStore.

## 🤝 Contribuciones

Para agregar funcionalidades o reportar issues, contacta con el equipo de desarrollo.

---

**Versión**: 1.0-SNAPSHOT  
**Java**: 17  
**Spring Boot**: 3.3.5  
**RabbitMQ**: 3.8+
