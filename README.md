# apache-artemis-plugin

1. 如何配置自动订阅

编辑 `etc/broker.xml`

找到 `<acceptor name="mqtt">...</acceptor>` 注释

在下面增加

`<acceptor name="mqtt2">tcp://0.0.0.0:1883?tcpSendBufferSize=1048576;tcpReceiveBufferSize=1048576;protocols=MQTT2;useEpoll=true;p2p=true</acceptor>`

- `name=mqtt2` 这里必须这么写

- `protocols=MQTT2` 这个必须这么写，这里是拷贝的apache artemis mqtt protocol实现，在MQTTSession的start方法增加了自动订阅功能

- `p2p=true` 表示开启自动订阅
