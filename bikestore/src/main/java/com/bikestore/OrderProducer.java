package com.bikestore;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.json.JSONObject;
import org.springframework.web.bind.annotation.*;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

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

        // Permite capturar errores que ocurran dentro del thread
        AtomicReference<Exception> error = new AtomicReference<>();

        // Crea un thread independiente para publicar el pedido en RabbitMQ
        Thread producerThread = new Thread(() -> {
            try (Connection connection = RabbitConfig.getConnection();
                 Channel channel = connection.createChannel()) {

                // Configura exchanges y colas
                RabbitConfig.setup(channel);

                // Publica el mensaje JSON en RabbitMQ
                channel.basicPublish(
                        RabbitConfig.EXCHANGE,
                        "order.created",
                        null,
                        order.toString().getBytes(StandardCharsets.UTF_8)
                );

                // Registra un log indicando que el pedido fue publicado
                RabbitConfig.log(pedidoId, "OrderProducer publicó el pedido en RabbitMQ");

            } catch (Exception e) {
                error.set(e);
            }
        });

        // Asigna un nombre al thread para identificarlo en consola
        producerThread.setName("OrderProducer-Thread");

        // Inicia el thread
        producerThread.start();

        // Espera a que termine la publicación antes de responder al cliente
        producerThread.join();

        // Si hubo error dentro del thread, se lanza para que Spring Boot lo muestre
        if (error.get() != null) {
            throw error.get();
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