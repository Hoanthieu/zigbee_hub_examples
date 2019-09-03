# -*- coding: UTF-8 -*-

import uasyncio as asyncio
import ujson as json
from core.constants import *
import arequests as requests
import logging
from micropython import const
from core.db_pan_nodes import DBPanNodes
import picoweb


globalPan = None
displayNodes = DBPanNodes()


class App:
    # User App of Hub SDK, send a 'Hello world' to wireless display
    def __init__(self, mgr, loop, pan):
        self.nodeId = None
        self.log = logging.getLogger("APP")
        self.log.setLevel(logging.DEBUG)
        self.log.info('APP init')
        self.pan = pan
        global globalPan
        globalPan = pan
        self.web = WebServer()
        mgr.setSetupPressedCallback(self.onSetupPressed)
        self.pan.setAppNodeJoinCallback(self.onNodeJoin)
        self.pan.setAppDataReceiveCallback(self.onDataReceive)
        self.loop = loop

    def onSetupPressed(self):
        self.log.info('setup button pressed')
        if self.pan:
            self.pan.permitDeviceJoin(60)

    def onNodeJoin(self, rawData, n):
        self.log.info('node joined/rejoined')
        self.log.debug("SrcAddr %s", hex(n.SrcAddr))
        self.log.debug("NwkAddr %s", hex(n.NwkAddr))
        self.log.debug("IEEEAddr %s", hex(n.IEEEAddr))  # nodeId
        self.nodeId = n.IEEEAddr
        self.log.debug("Capabilities %s", hex(n.Capabilities))

    def onDataReceive(self, rawData, n):
        self.log.info('data received')
        self.log.debug("GroupId# %s", n.GroupId)
        self.log.debug("ClusterId# %s", hex(n.ClusterId))
        self.log.debug("SrcAddr# %s", hex(n.SrcAddr))
        self.log.debug("SrcEndpoint# %d", n.SrcEndpoint)
        self.log.debug("DstEndpoint# %d", n.DstEndpoint)
        self.log.debug("WasBroadcast# %d", n.WasBroadcast)
        self.log.debug("LinkQuality# %d", n.LinkQuality)
        self.log.debug("SecurityUse# %d", n.SecurityUse)
        self.log.debug("TimeStamp# %d", n.TimeStamp)
        self.log.debug("TransSeqNum# %d", n.TransSeqNum)
        self.log.debug("Len# %d", n.Len)
        print("Data#", end=' ')
        i = 0
        while i < n.Len:
            print('0x{0:0{1}X}'.format(n.Data[i], 2), end=' ')
            i += 1
        self.loop.create_task(self.taskDataReceived(n))  # run a asyncio task

    async def taskDataReceived(self, n):
        if n.SrcAddr == 0x39F6:  # Javis Switch
            nodes = displayNodes.getPanNodesInfo()
            index = n.SrcEndpoint
            if n.Data[6] == 0:
                for nodeid in nodes:
                    if nodes[nodeid]['nwkAddr'] == n.SrcAddr:
                        # nodes[nodeid]['state'] = 'off'
                        if 'state' not in nodes[nodeid]:
                            states = {
                                "switch1": "unknown",
                                "switch2": "unknown",
                                "switch3": "unknown",
                            }
                            states["switch" + str(index)] = "off"
                            nodes[nodeid]['state'] = states
                        else:
                            nodes[nodeid]['state']["switch" + str(index)] = "off"
                self.log.debug("Switch " + str(index) + " is off")
            elif n.Data[6] == 1:
                for nodeid in nodes:
                    if nodes[nodeid]['nwkAddr'] == n.SrcAddr:
                        # nodes[nodeid]['state'] = 'off'
                        if 'state' not in nodes[nodeid]:
                            states = {
                                "switch1": "unknown",
                                "switch2": "unknown",
                                "switch3": "unknown",
                            }
                            states["switch" + str(index)] = "on"
                            nodes[nodeid]['state'] = states
                        else:
                            nodes[nodeid]['state']["switch" + str(index)] = "on"
                self.log.debug("Switch " + str(index) + " is on")
            displayNodes.setPanNodesInfo(nodes)


def sendMessage(ieeeAddr, state, id_switch):
    global globalPan
    if state == 'ON':
        payload = [0x11, 0x00, 0x01]
    elif state == 'OFF':
        payload = [0x11, 0x00, 0x00]
    nodeId = ieeeAddr
    clusterId = 0x0006
    DstEndpoint = id_switch
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


def qs_parse(qs):
    parameters = {}
    ampersandSplit = qs.split("&")
    for element in ampersandSplit:
        equalSplit = element.split("=")
        parameters[equalSplit[0]] = equalSplit[1]
    return parameters


class WebServer():
    def __init__(self):
        self.log = logging.getLogger("Web Server")
        self.log.setLevel(logging.DEBUG)
        self.run()

    def run(self):
        webapp = picoweb.WebApp(__name__)

        @webapp.route("/list_nodes", methods=['GET', 'POST', 'DELETE'])
        def index(req, resp):
            method = req.method
            self.log.debug("Method %s called", method)
            if method == "GET":
                displayNodes = DBPanNodes()
                nodes = displayNodes.getPanNodesInfo()
                self.log.debug("Number of Nodes: %d", len(nodes))
                queryString = req.qs
                if queryString != "":
                    parameters = qs_parse(queryString)
                    self.log.debug(str(parameters))
                    yield from picoweb.start_response(resp)
                    if 'nodeid' in parameters:
                        if parameters['nodeid'].isdigit():
                            if int(parameters['nodeid']) in nodes:
                                yield from resp.awrite(str({int(parameters['nodeid']): nodes[int(parameters['nodeid'])]}))
                            else:
                                yield from resp.awrite("Cannot get Node with ID: " + parameters['nodeid'])
                        else:
                            yield from resp.awrite("NodeID not Valid")

                    else:
                        yield from resp.awrite("Not Found NodeID Param")
                else:
                    self.log.debug("Query String is None")
                    yield from picoweb.start_response(resp)
                    yield from resp.awrite(str(nodes))
            elif method == 'POST':
                yield from req.read_form_data()
                nodeid = req.form['nodeid']
                namenode = req.form['name']
                displayNodes = DBPanNodes()
                nodes = displayNodes.getPanNodesInfo()
                self.log.debug(nodes)
                for node in nodes:
                    if str(node) == nodeid:
                        nodes[node]["name"] = namenode
                displayNodes.setPanNodesInfo(nodes)
                yield from picoweb.start_response(resp)
                yield from resp.awrite("Add name of Node " + nodeid)
            elif method == 'DELETE':
                displayNodes = DBPanNodes()
                nodes = displayNodes.getPanNodesInfo()
                yield from req.read_form_data()
                nodeid = req.form['nodeid']
                try:
                    del nodes[nodeid]
                except:
                    pass
                displayNodes.setPanNodesInfo(nodes)
                self.log.debug("Deleted node: %s", nodeid)
                yield from picoweb.start_response(resp)
                yield from resp.awrite("Deleted Node " + str(nodeid))

        @webapp.route('/control', methods=['POST'])
        def control_device(req, resp):
            yield from req.read_form_data()
            nodeid = int(req.form['nodeid'])
            state = req.form['state']
            id_switch = int(req.form['idx'])
            sendMessage(int(nodeid), state, id_switch)
            yield from picoweb.start_response(resp)
            yield from resp.awrite("Controlled Device " + str(nodeid))
        self.log.info("Running on 0.0.0.0:5050...")
        webapp.run(debug=True, host="0.0.0.0", port=5050)
