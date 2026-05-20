package com.bikestore;

// Importa las clases necesarias de RabbitMQ para manejar canales y conexiones
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

// Importa estructuras de datos para manejar configuraciones de colas
import java.util.HashMap;
import java.util.Map;

public class RabbitConfig {

    // Nombre del exchange principal de la aplicación
    public static final String EXCHANGE = "bikestore.exchange";
    // Cola donde se envían los pedidos creados
    public static final String ORDER_QUEUE = "order.queue";
    // Cola donde se envían eventos relacionados con emails
    public static final String EMAIL_QUEUE = "email.queue";
    // Dead Letter Queue (DLQ) para mensajes fallidos
    public static final String DLQ_QUEUE = "order.dlq";

    // Método para crear y retornar una conexión con RabbitMQ
    public static Connection getConnection() throws Exception {
        // Crea la fábrica de conexiones
        ConnectionFactory factory = new ConnectionFactory();
        // Configura el host donde está RabbitMQ
        factory.setHost("localhost");
        // Usuario por defecto de RabbitMQ
        factory.setUsername("guest");
        // Contraseña por defecto de RabbitMQ
        factory.setPassword("guest");
        // Retorna una nueva conexión
        return factory.newConnection();
    }

    // Método que configura exchanges, colas y bindings
    public static void setup(Channel channel) throws Exception {
        // Declara un exchange tipo direct y persistente
        channel.exchangeDeclare(EXCHANGE, "direct", true);
        // Declara la cola DLQ persistente
        channel.queueDeclare(DLQ_QUEUE, true, false, false, null);
        // Mapa para almacenar argumentos de configuración de la cola
        Map<String, Object> orderArgs = new HashMap<>();
        // Define el exchange al que irán mensajes rechazados, utiliza el por default 
        orderArgs.put("x-dead-letter-exchange", "");
        // Define la routing key para enviar mensajes a la DLQ
        orderArgs.put("x-dead-letter-routing-key", DLQ_QUEUE);
        // Declara la cola de pedidos con configuración DLQ
        channel.queueDeclare(ORDER_QUEUE, true, false, false, orderArgs);
        // Vincula la cola de pedidos al exchange con la routing key
        channel.queueBind(ORDER_QUEUE, EXCHANGE, "order.created");
        // Declara la cola de emails
        channel.queueDeclare(EMAIL_QUEUE, true, false, false, null);
        // Vincula la cola de emails al exchange
        channel.queueBind(EMAIL_QUEUE, EXCHANGE, "payment.paid");
    }

    // Método auxiliar para mostrar logs del sistema
    public static void log(String pedidoId, String event) {
        // Imprime fecha, id del pedido, hilo actual y evento ocurrido
        System.out.println(
                "[" + java.time.LocalDateTime.now() + "] " +
                "[pedidoId=" + pedidoId + "] " +
                "[thread=" + Thread.currentThread().getName() + "] " +
                event
        );
    }
}