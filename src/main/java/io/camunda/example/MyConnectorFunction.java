package io.camunda.example;

import io.camunda.connector.api.annotation.OutboundConnector;
import io.camunda.connector.api.error.ConnectorException;
import io.camunda.connector.api.outbound.OutboundConnectorContext;
import io.camunda.connector.api.outbound.OutboundConnectorFunction;
import io.camunda.connector.generator.java.annotation.ElementTemplate;
import io.camunda.example.dto.MyConnectorRequest;
import io.camunda.example.dto.MyConnectorResult;
import io.nats.client.Connection;
import io.nats.client.Nats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@OutboundConnector(
    name = "NATS Connector",
    inputVariables = {"subject", "message", "host","port"},
    type = "io.camunda:template:1")
@ElementTemplate(
        id = "ad92ff8b-eee4-4f9b-929f-db844e6620c7",
        name = "NATS Connector",
        inputDataClass = MyConnectorRequest.class)
public class MyConnectorFunction implements OutboundConnectorFunction {

  private static final Logger LOGGER = LoggerFactory.getLogger(MyConnectorFunction.class);

  @Override
  public Object execute(OutboundConnectorContext context) {
    final var connectorRequest = context.bindVariables(MyConnectorRequest.class);
    return executeConnector(connectorRequest);
  }

  private MyConnectorResult executeConnector(final MyConnectorRequest connectorRequest) {
    // TODO: implement connector logic
    LOGGER.info("Executing my connector with request {}", connectorRequest);
    String subject = connectorRequest.subject();
    String message = connectorRequest.message();
    String host = connectorRequest.host();
    String port = connectorRequest.port();

    try (Connection natsConnection = Nats.connect("nats://"+host+":"+port)) {
      natsConnection.publish(subject, message.getBytes());
      System.out.printf("Message published to subject: %s and server %s", subject,
              natsConnection.getServerInfo());
      return new MyConnectorResult("Message published to subject: %s and server %s"+ subject+" "+natsConnection.getServerInfo());

    } catch (Exception e) {
      throw new ConnectorException("FAIL", "Connector failed... " + e.getMessage());
    }



  }
}
