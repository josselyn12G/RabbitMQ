package com.bikestore;
import com.rabbitmq.client.*;
import jakarta.annotation.PostConstruct;
import org.json.JSONObject;
import org.springframework.stereotype.Component;
import java.nio.charset.StandardCharsets;
// Indica que esta clase será administrada automáticamente por Spring Boot.
@Component
public class EmailConsumer {
    // Este método se ejecuta automáticamente cuando Spring Boot inicia la aplicación.
    @PostConstruct
    public void startWorker() {

        // Se crea un hilo independiente para que EmailWorker escuche mensajes sin bloquear la aplicación web.
        Thread workerThread = new Thread(() -> {
            try {
                // Crea la conexión con RabbitMQ usando la configuración centralizada en RabbitConfig.
                Connection connection = RabbitConfig.getConnection();
                // Crea un canal de comunicación con RabbitMQ.
                Channel channel = connection.createChannel();
                // Crea o verifica el exchange, las colas y los bindings necesarios.
                RabbitConfig.setup(channel);
                // Define la lógica que se ejecutará cada vez que llegue un mensaje a la cola de correos.
                DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                    Thread.currentThread().setName("EmailConsumer-Thread");
                    // Convierte el cuerpo del mensaje recibido desde bytes a texto JSON.
                    String body = new String(delivery.getBody(), StandardCharsets.UTF_8);
                    // Convierte el texto JSON en un objeto para poder leer sus datos.
                    JSONObject order = new JSONObject(body);
                    // Obtiene el identificador único del pedido.
                    String pedidoId = order.getString("pedidoId");
                    // Obtiene el estado del pago.
                    String paymentStatus = order.getString("paymentStatus");
                    // Obtiene el correo electrónico del cliente.
                    String email = order.getString("clienteEmail");
                    // Obtiene el producto comprado.
                    String producto = order.getString("producto");
                    // Verifica si el pago fue aprobado antes de simular el envío del correo.
                    if ("PAID".equals(paymentStatus)) {
                        // Registra en consola que se envió el correo de confirmación.
                        RabbitConfig.log(
                                pedidoId,
                                "EmailWorker envió correo de confirmación a " + email +
                                        " por el producto: " + producto
                        );
                    } else {
                        // Registra que el correo no se envió porque el pago no está aprobado.
                        RabbitConfig.log(
                                pedidoId,
                                "EmailWorker ignoró pedido porque paymentStatus no es PAID"
                        );
                    }
                    // Confirma a RabbitMQ que el mensaje fue procesado correctamente.
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                };
                // Indica que EmailWorker consumirá mensajes desde la cola email.queue.
                channel.basicConsume(RabbitConfig.EMAIL_QUEUE, false, deliverCallback, consumerTag -> {});
                // Mensaje informativo para confirmar que EmailWorker está activo.
                System.out.println("EmailWorker escuchando mensajes...");
            } catch (Exception e) {
                // Imprime cualquier error que ocurra durante la conexión o procesamiento del mensaje.
                e.printStackTrace();
            }
        });
        
        // Asigna un nombre al hilo para identificarlo en los logs.
        workerThread.setName("EmailConsumer-Thread");
        // Inicia el hilo para que EmailWorker empiece a escuchar mensajes.
        workerThread.start();
    }
}