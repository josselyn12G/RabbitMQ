package com.bikestore;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.json.JSONObject;
import org.springframework.web.bind.annotation.*;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/orders")
public class OrderProducer {

    // Define un endpoint POST
    @PostMapping
    public Map<String, Object> createOrder(@RequestBody Map<String, Object> request) throws Exception {
        // Genera un ID único para el pedido usando el tiempo actual
        String pedidoId = "PED-" + System.currentTimeMillis();
        // Crea un objeto JSON para representar el pedido
        JSONObject order = new JSONObject();
        // Agrega el ID del pedido al JSON
        order.put("pedidoId", pedidoId);
        // Agrega el nombre del cliente recibido en el request
        order.put("clienteNombre", request.get("clienteNombre"));
        // Agrega el email del cliente recibido en el request
        order.put("clienteEmail", request.get("clienteEmail"));
        // Agrega el producto solicitado
        order.put("producto", request.get("producto"));
        // Agrega el total del pedido
        order.put("total", request.get("total"));

        // Define el estado inicial del pago
        order.put("paymentStatus", "PENDING");

        // Abre conexión y canal con RabbitMQ
        try (Connection connection = RabbitConfig.getConnection();
             // Crea un canal para enviar mensajes
             Channel channel = connection.createChannel()) {
            // Configura exchanges y colas
            RabbitConfig.setup(channel);
            // channel.basicPublish publica un mensaje en RabbitMQ
            channel.basicPublish(
                    // Exchange donde se enviará el mensaje
                    RabbitConfig.EXCHANGE,
                    // Routing key usada para dirigir el mensaje
                    "order.created",
                    // Propiedades adicionales del mensaje
                    null,
                    // Convierte el JSON a bytes UTF-8
                    order.toString().getBytes(StandardCharsets.UTF_8)
            );
            // Registra un log indicando que el pedido fue publicado
            RabbitConfig.log(pedidoId, "OrderProducer publicó el pedido en RabbitMQ");
        }

        // Crea el mapa de respuesta para el cliente
        Map<String, Object> response = new HashMap<>();
        // Mensaje de confirmación
        response.put("mensaje", "Pedido enviado correctamente a RabbitMQ");
        // Retorna el ID generado
        response.put("pedidoId", pedidoId);
        // Retorna el estado actual
        response.put("estado", "PENDING");
        // Devuelve la respuesta JSON
        return response;
    }
}