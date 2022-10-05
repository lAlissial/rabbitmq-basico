import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.nio.charset.StandardCharsets;


public class Consumidor {
    private static final String NOME_FILA  = "task_queue";

    private static void doWork(String task) {
        for (char ch: task.toCharArray ()) {
            if (ch == '.') {
                try {
                    Thread.sleep (1000);
                } catch (InterruptedException _ignored) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {

        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        connectionFactory.setUsername("mqadmin");
        connectionFactory.setPassword("Admin123XX_");

        Connection conexao = connectionFactory.newConnection();
        Channel canal = conexao.createChannel();

        boolean duravel = true;
        canal.queueDeclare(NOME_FILA, duravel, false, false, null);

        int prefetchCount = 1;
        canal.basicQos(prefetchCount);

        DeliverCallback callback = (consumerTag, delivery) -> {
            String mensagem = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println ("[x] Recebido '" + mensagem + "'");
            try {
                doWork (mensagem);
            } finally {
                System.out.println ("[x] Feito");
                canal.basicAck(delivery.getEnvelope(). getDeliveryTag(), false);
            }
        };
        boolean autoAck = false;
        canal.basicConsume (NOME_FILA, autoAck, callback, consumerTag -> {});

    }
}


