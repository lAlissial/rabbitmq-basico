import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.nio.charset.StandardCharsets;


public class Consumidor {

    private static void doWork(String task) throws InterruptedException {
        for (char ch: task.toCharArray ()) {
            if (ch == '.') Thread.sleep (1000);
        }
    }

    public static void main(String[] args) throws Exception {


        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        connectionFactory.setUsername("mqadmin");
        connectionFactory.setPassword("Admin123XX_");

        Connection conexao = connectionFactory.newConnection();
        Channel canal = conexao.createChannel();

        String NOME_FILA = "hello";
        canal.queueDeclare(NOME_FILA, false, false, false, null);

        DeliverCallback callback = (consumerTag, delivery) -> {
            String mensagem = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println ("[x] Recebido '" + mensagem + "'");
            try {
                try {
                    doWork (mensagem);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } finally {
                System.out.println ("[x] Feito");
            }
        };
        boolean autoAck = true; // ack é feito aqui. Como está autoAck, enviará automaticamente
        canal.basicConsume (NOME_FILA, autoAck, callback, consumerTag -> {});

    }
}


