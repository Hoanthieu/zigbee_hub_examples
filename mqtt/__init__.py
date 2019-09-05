# -*- coding: UTF-8 -*-

import logging
import ujson as json
import utime
import uasyncio as asyncio
from umqtt.simple import MQTTClient
from micropython import const

globalPan = None

def sendMessage(ieeeAddr, switch_id, msg):
    #ieeeAddr = nodeid
    #state is on or off 
    #id_swicth: index of switch in JAVIS 3 GANG
    global globalPan
    state = msg
    if state.upper() == 'ON':
        payload = [0x11, 0x00, 0x01]
    elif state.upper() == 'OFF':
        payload = [0x11, 0x00, 0x00]
    nodeId = ieeeAddr
    clusterId = 0x0006
    DstEndpoint = switch_id
    # DstEndpoint = 0x01 # 0x01、0x02、0x03
    SrcEndpoint = 0x01
    return globalPan.sendRawData(
        nodeId=nodeId,
        payload=payload,
        nwkAddr=None,  # use nodeId
        clusterId=clusterId,
        DstEndpoint=DstEndpoint,
        SrcEndpoint=SrcEndpoint
    )

class MqttClient:
    # User App of Hub SDK, subscribe and publish MQTT messages

    # http://www.hivemq.com/demos/websocket-client/
    MQTT_HOST            = "192.168.1.44"
    MQTT_PORT            = 1883

    UNIQUE_ID            = '000B3CFFFEF7EF5C'
    DEFAULT_KEEPALIVE    = const(60)
    KEEP_ALIVE_THRESHOLD = const(5)

    def __init__(self, loop, log, callback = None):
        self.loop = loop
        self.log = log
        self.subscribeCallback = callback

        self.sn = self.UNIQUE_ID
        self.client = None
        self.topicSubOperation = "{}/switch.1/set".format(self.sn) # same as: MC30AEA4CC1A40/operation
        self.mqttLive = False
        self.log.info('MQTT init')

    def _clientInit(self):
        self.client = MQTTClient(client_id  = self.sn,
                                 server     = self.MQTT_HOST,
                                 port       = self.MQTT_PORT,
                                 keepalive  = self.DEFAULT_KEEPALIVE)

    def _clientConnect(self):
        self.log.debug('MQTT connecting...')
        try:
            self.client.connect()
            self.log.info('MQTT live!')
            self.mqttLive = True
            return True
        except Exception as e:
            self.log.exception(e, 'could not establish MQTT connection')
            return False

    def _subscribeTopic(self):
        try:
            self.client.set_callback(self._msgReceivedCallback) # set a handler for incoming messages
            self.client.subscribe(topic = self.topicSubOperation, qos = 0)
            self.log.info('subscribe [%s]', self.topicSubOperation)
        except Exception as e:
            self.log.exception(e, 'subscribe fail')

    def _resetPingTimer(self):
        self.pingCountdown = self.DEFAULT_KEEPALIVE

    def _ping(self):
        ''' do a MQTT ping before keepalive period expires '''
        self.pingCountdown -= 1
        if self.pingCountdown < self.KEEP_ALIVE_THRESHOLD:
            self.log.debug('mqtt ping...')
            self.client.ping()
            self._resetPingTimer()

    def _connectAttempt(self):
        if self._clientConnect():
            self._subscribeTopic()
            self._resetPingTimer()
            return True
        else:
            return False

    def _msgReceivedCallback(self, topic, msg):
        if self.subscribeCallback is not None:
            self.subscribeCallback(topic, msg)

    def mqttIsLive(self):
        return self.mqttLive

    def publishMsg(self, msg):
        try:
            # topic = self.topicPubUpdate
            topic = self.topicSubOperation
            #code doan dieu khien thiet bi
            self.log.info("publish: topic[%s] msg[%s]", topic, msg)
            self.client.publish(topic=topic, msg=msg, qos=0)
        except Exception as e:
            self.log.exception(e, 'publish fail')

    async def taskMqttWorker(self):
        reconnectAttemptBackoff = 1 # don't try too hard, use backoff
        self._connectAttempt()
        while True:
            try:
                self.client.check_msg() # if there is a message, _msgReceivedCallback will be called
                self._ping()
                reconnectAttemptBackoff = 1
                await asyncio.sleep(1)
            except Exception as e:
                self.log.exception(e, 'MQTT check message problem')
                self.mqttLive = False
                if not self._connectAttempt():
                    reconnectAttemptBackoff *= 2 # don't retry too fast, this will abuse the server
                    if reconnectAttemptBackoff > 64:
                        reconnectAttemptBackoff = 64
                    self.log.debug('reconnect attempt backoff: %ds', reconnectAttemptBackoff)
                    await asyncio.sleep(reconnectAttemptBackoff)

    def start(self):
        self._clientInit()
        self.loop.create_task(self.taskMqttWorker())

class App:
    def __init__(self, mgr, loop, pan):
        self.log = logging.getLogger("MQTT")
        self.mc = MqttClient(loop, self.log, self.onMsgReceived)
        self.pan = pan
        global globalPan
        globalPan = pan
        self.mc.start()
        loop.create_task(self.taskPublishTest())

    async def taskPublishTest(self):
        while True:
            if not self.mc.mqttIsLive():
                await asyncio.sleep(1)
            else:
                msg = "on"
                self.mc.publishMsg(msg)
                await asyncio.sleep(60)
                
                msg = "off"
                self.mc.publishMsg(msg)
                await asyncio.sleep(60)
                self.log.info("Done")

    def onMsgReceived(self, topic, msg):
        s_topic = topic.decode()
        s_msg = msg.decode()
        ieeeAddr = s_topic.split("/")[0]
        switch_id = s_topic.split("/")[1][-1]
        sendMessage(int(ieeeAddr, 16), int(switch_id), s_msg)
        self.log.info("received: topic[%s] msg[%s]", s_topic, s_msg)