package org.apache.activemq.artemis.core.protocol.mqtt;

import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttSubscribePayload;
import io.netty.handler.codec.mqtt.MqttSubscriptionOption;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerPlugin;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.SUBSCRIPTION_IDENTIFIER;

public class P2pAutoSubscribePlugin {

    private static final Logger log = LoggerFactory.getLogger(P2pAutoSubscribePlugin.class);

    private final boolean enabled;

    public P2pAutoSubscribePlugin(boolean enabled) {
        this.enabled = enabled;
    }

    public void afterSessionStart(MQTT2Session session) {
        if (enabled) {
            MQTT2Connection connection = session.getConnection();
            if (connection != null) {
                String clientID = connection.getClientID();
                if (clientID != null) {
                    String autoTopic = "+/p2p/" + connection.getClientID();
                    MqttSubscribeMessage message = buildMqttSubscribeMessage(autoTopic);
                    Integer subscriptionIdentifier = MQTTUtil.getProperty(Integer.class, message.idAndPropertiesVariableHeader().properties(), SUBSCRIPTION_IDENTIFIER, null);
                    try {
                        session.getSubscriptionManager()
                                .addSubscriptions(message.payload().topicSubscriptions(), subscriptionIdentifier);
                        log.info("addSubscriptions: {}", autoTopic);
                    } catch (Exception e) {
                        log.warn("Failed to add subscriptions for MQTT2SubscriptionManager", e);
                    }
                }
            }
        }
    }

    private MqttSubscribeMessage buildMqttSubscribeMessage(String autoTopic) {
        MqttSubscriptionOption option = MqttSubscriptionOption.onlyFromQos(MqttQoS.AT_LEAST_ONCE); // QOS 1
        MqttTopicSubscription topicSubscription = new MqttTopicSubscription(autoTopic, option);

        List<MqttTopicSubscription> topics = Collections.singletonList(topicSubscription);
        MqttSubscribePayload payload = new MqttSubscribePayload(topics);

        int packetId = 1;
        MqttMessageIdVariableHeader variableHeader = MqttMessageIdVariableHeader.from(packetId);

        MqttFixedHeader fixedHeader = new MqttFixedHeader(
                MqttMessageType.SUBSCRIBE,
                false, // isDup
                MqttQoS.AT_LEAST_ONCE,
                false,
                0
        );
        return new MqttSubscribeMessage(fixedHeader, variableHeader, payload);
    }

}
