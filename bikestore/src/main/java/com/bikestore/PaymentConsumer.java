package com.bikestore;
import com.rabbitmq.client.*;
import jakarta.annotation.PostConstruct;
import org.json.JSONObject;
import org.springframework.stereotype.Component;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
// Indica que esta clase será administrada automáticamente por Spring Boot como un componente del sistema.
@Component
public class PaymentConsumer {
    // Número máximo de intentos permitidos antes de enviar el mensaje a la Dead-Letter Queue.
    private static final int MAX_RETRIES = 3;
    // Objeto Random para simular si el pago falla o se aprueba de forma aleatoria.
    private static final Random random = new Random();
    // Este método se ejecuta automáticamente cuando Spring Boot termina de crear el componente.
    @PostConstruct
    public void startWorker() {
        // Se crea un hilo independiente para que PaymentConsumer escuche mensajes sin bloquear la aplicación web.
        Thread workerThread = new Thread(() -> {
            try {
                // Crea la conexión con RabbitMQ usando la configuración centralizada en RabbitConfig.
                Connection connection = RabbitConfig.getConnection();
                // Crea un canal de comunicación con RabbitMQ para consumir y publicar mensajes.
                Channel channel = connection.createChannel();
                // Crea o verifica el exchange, las colas y la Dead-Letter Queue necesarias.
                RabbitConfig.setup(channel);
                // Define la lógica que se ejecutará cada vez que llegue un mensaje a la cola de pedidos.
                DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                    // Convierte el cuerpo del mensaje recibido desde bytes a texto JSON.
                    String body = new String(delivery.getBody(), StandardCharsets.UTF_8);
                    // Convierte el texto JSON en un objeto JSONObject para poder leer sus datos.
                    JSONObject order = new JSONObject(body);
                    // Obtiene el identificador único del pedido.
                    String pedidoId = order.getString("pedidoId");
                    // Obtiene el número de intentos realizados desde los headers del mensaje.
                    int retryCount = getRetryCount(delivery);
                    // Registra en consola que PaymentConsumer recibió el pedido e indica el intento actual.
                    RabbitConfig.log(pedidoId, "PaymentConsumer recibió pedido. Intento: " + (retryCount + 1));
                    // Simula el resultado del pago con 50 % de probabilidad de fallo y 50 % de aprobación.
                    boolean paymentFailed = random.nextBoolean();
                    // Si el pago falla, se ejecuta la lógica de reintento o envío a DLQ.
                    if (paymentFailed) {
                        // Registra en consola que el pago falló.
                        RabbitConfig.log(pedidoId, "Pago fallido");
                        // Verifica si ya se alcanzó el máximo de intentos permitidos.
                        if (retryCount + 1 >= MAX_RETRIES) {
                            // Registra que el pedido falló definitivamente y será enviado a la Dead-Letter Queue.
                            RabbitConfig.log(pedidoId, "Falló después de 3 intentos. Enviando a Dead-Letter Queue");
                            // Rechaza el mensaje sin reencolarlo; RabbitMQ lo enviará a la DLQ configurada.
                            channel.basicReject(delivery.getEnvelope().getDeliveryTag(), false);
                        } else {
                            // Crea un mapa de headers para guardar el nuevo número de intento.
                            Map<String, Object> headers = new HashMap<>();
                            // Aumenta el contador de reintentos en 1.
                            headers.put("x-retry-count", retryCount + 1);
                            // Crea las propiedades del nuevo mensaje incluyendo los headers actualizados.
                            AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                                    .headers(headers)
                                    .build();
                            // Publica nuevamente el mismo pedido en RabbitMQ para que sea procesado otra vez.
                            channel.basicPublish(
                                    RabbitConfig.EXCHANGE,
                                    "order.created",
                                    props,
                                    body.getBytes(StandardCharsets.UTF_8)
                            );
                            // Confirma que el mensaje original ya fue procesado, porque se publicó una nueva copia para reintento.
                            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                            // Registra en consola que el pedido fue reenviado para un nuevo intento.
                            RabbitConfig.log(pedidoId, "Pedido reenviado para reintento");
                        }
                    } else {
                        // Si el pago fue aprobado, se actualiza el estado del pedido a PAID.
                        order.put("paymentStatus", "PAID");
                        // Publica el pedido aprobado en RabbitMQ para que lo procese EmailWorker.
                        channel.basicPublish(
                                RabbitConfig.EXCHANGE,
                                "payment.paid",
                                null,
                                order.toString().getBytes(StandardCharsets.UTF_8)
                        );
                        // Confirma que el mensaje original de pago fue procesado correctamente.
                        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                        // Registra en consola que el pago fue aprobado y enviado al worker de correo.
                        RabbitConfig.log(pedidoId, "Pago aprobado. Mensaje enviado a EmailWorker");
                    }
                };
                // Indica a RabbitMQ que esta clase consumirá mensajes desde la cola principal de pedidos.
                channel.basicConsume(RabbitConfig.ORDER_QUEUE, false, deliverCallback, consumerTag -> {});
                // Mensaje informativo para saber que el consumer quedó escuchando pedidos.
                System.out.println("PaymentConsumer escuchando mensajes...");
            } catch (Exception e) {
                // Imprime cualquier error que ocurra durante la conexión o procesamiento de mensajes.
                e.printStackTrace();
            }
        });
        // Asigna un nombre al hilo para que aparezca claramente en los logs.
        workerThread.setName("PaymentConsumer-Thread");
        // Inicia el hilo para que PaymentConsumer empiece a escuchar mensajes.
        workerThread.start();
    }
    // Método auxiliar que obtiene el número de reintentos desde los headers del mensaje.
    private int getRetryCount(Delivery delivery) {
        // Si el mensaje no tiene headers, significa que es el primer intento.
        if (delivery.getProperties().getHeaders() == null) {
            return 0;
        }
        // Obtiene el valor del header llamado x-retry-count.
        Object retry = delivery.getProperties().getHeaders().get("x-retry-count");
        // Si el header no existe, también se considera como primer intento.
        if (retry == null) {
            return 0;
        }
        // Convierte el valor del header a número entero y lo retorna.
        return Integer.parseInt(retry.toString());
    }
}