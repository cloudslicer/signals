"""
Signals service provides a uniform publisher/subscriber interface for the entire
application that blends mqtt with local message groups.
"""
import threading
import queue
import multiprocessing
import paho.mqtt.client as mqtt
import logging
import traceback
import uuid
import os
import select
import time
import topics
import signal
import sys

# To be tweeked based on mqtt broker
MQTT_PORT = 1883
MQTT_HOST = '127.0.0.1'

# if your message broker requires a password 
#MQTT_AUTH = {"username":"", "password":""}

MqttErrCodes = {
    0: "Connection successful",
    1: "Connection refused - incorrect protocol version",
    2: "Connection refused - invalid client identifier",
    3: "Connection refused - server unavailable",
    4: "Connection refused - bad username or password",
    5: "Connection refused - not authorised"
}


DebugLogging = False

MAIN_MSGQ_READY = "ready"
MAIN_MSGQ_PIPE_DATA = "pipe-data"
MAIN_MSGQ_PIPE_ERROR = "pipe-error"
MAIN_MSGQ_DEFER = "defer"
MAIN_MSGQ_MQTT_INBOUND = "mqtt-inbound"


MAX_MAIN_LOOP_QUEUE_DEPTH = 1024

# if > 0 this INTEGER number is the max seconds
# that a message handler can take before an alarm
# is issued, for debig purposes only. It is set
# to -1 to disable. 
LIMIT_MAIN_FUNC_EXEC_TIME = -1
# prevent a 0 value
assert LIMIT_MAIN_FUNC_EXEC_TIME != 0


def create_mqtt_client(name, connected, on_message=None):
    # callback for when connected.
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            connected.set()
        else:
            errmsg = MqttErrCodes.get(rc, "Unknown error")
            logging.error(name + " " + errmsg)

    mqttc = mqtt.Client(client_id=uuid.uuid1().hex)
    mqttc.on_connect = on_connect
    if on_message:
        mqttc.on_message = on_message
    host = MQTT_HOST
    port = MQTT_PORT
    mqttc.username_pw_set(MQTT_AUTH['username'],
                          password=MQTT_AUTH['password'])

    mqttc.connect(host, port, 60)
    mqttc.loop_start()

    # blocking call, we can't service our message
    # queue until we are connected
    connected.wait()
    # --------------------------------------------

    return mqttc


class MqttStreamer(threading.Thread):
    "Outbound mqtt connection"

    def __init__(self, name, main_msgq):
        threading.Thread.__init__(self)
        self.daemon = True
        self.main_msgq = main_msgq
        self.msgq = queue.Queue()
        self.name = name
        self.connected = threading.Event()

    def stop(self):
        self.msgq.put(None)

    def send(self, topic, data):
        self.msgq.put((topic, data))

    def run(self):
        mqttc = None
        try:
            # create and connect to the mqtt message broker
            mqttc = create_mqtt_client(self.name, self.connected)

            logging.info(self.name + " ready")
            self.main_msgq.put((MAIN_MSGQ_READY, [self.name]))

            # push outbound messages to mqtt daemon
            while True:
                msg = self.msgq.get()
                if not msg:
                    break
                if DebugLogging:
                    logging.debug(str(msg))
                (topic, data) = msg
                try:
                    mqttc.publish(topic, payload=data)
                except:
                    (a,b,c) = sys.exc_info()
                    logging.error("mqtt publish failure: %s" % str(b)) 
                    t = str(type(data))
                    logging.error("for %s: payload type %s" % (topic,t))

        except BaseException:
            logging.error(traceback.format_exc())

        if mqttc:
            # graceful shutdown
            mqttc.disconnect()


class MqttListener(threading.Thread):
    "inbound mqtt listener"

    def __init__(self, name, main_msgq):
        threading.Thread.__init__(self)
        self.daemon = True
        self.commands = queue.Queue()
        self.name = name
        self.main_msgq = main_msgq
        self.connected = threading.Event()

    def on_message(self, client, userdata, msg):
        if not msg.topic.startswith('$SYS'):
            logging.info("INBOUND mqtt " + " received data from " + msg.topic)
        self.main_msgq.put((MAIN_MSGQ_MQTT_INBOUND, (msg,)))

    def run(self):
        mqttc = None

        try:
            # create and connect to the mqtt message broker
            mqttc = create_mqtt_client(self.name,
                                       self.connected, self.on_message)
            logging.info(self.name + " ready")
            self.main_msgq.put((MAIN_MSGQ_READY, [self.name]))

            while True:
                msg = self.commands.get()
                if not msg:
                    break
                if DebugLogging:
                    logging.debug(self.name + " command " + str(msg))
                (cmd, args) = msg
                if cmd == "subscribe":
                    mqttc.subscribe(args[0])
                elif cmd == "unsubscribe":
                    mqttc.unsubscribe(args[0])
                elif cmd == "publish":
                    mqttc.publish(args[0], payload=args[1])

        except BaseException:
            logging.error(traceback.format_exc())

        if mqttc:
            # graceful shutdown
            mqttc.disconnect()

    def subscribe(self, topic):
        self.commands.put(("subscribe", [topic]))

    def unsubscribe(self, topic):
        self.commands.put(("unsubscribe", [topic]))

    def publish(self, topic, message):
        self.commands.put(("subscribe", [topic, message]))

    def stop(self):
        self.commands.put(None)


class PipeWatcher(threading.Thread):
    def __init__(self, name, main_msgq):
        threading.Thread.__init__(self)
        self.daemon = True
        self.main_msgq = main_msgq
        self.commands = queue.Queue()
        self.cmd_r, self.cmd_w = os.pipe()
        self.plist = select.poll()
        self.plist.register(self.cmd_r, select.POLLIN)
        self.running = True

    def stop(self):
        self.commands.put(None)
        os.write(self.cmd_w, b'x')

    def register(self, fd):
        self.commands.put(("register", fd))
        os.write(self.cmd_w, b'x')

    def unregister(self, fd):
        self.commands.put(("unregister", fd))
        os.write(self.cmd_w, b'x')

    def proc(self, fd, evt):
        if fd == self.cmd_r:
            os.read(self.cmd_r, 1)
            x = self.commands.get()

            if not x:
                logging.info("PipeWatcher exiting")
                self.running = False
                return
            (cmd, fd) = x
            if cmd == "register":
                self.plist.register(fd, select.POLLIN)
            elif cmd == "unregister":
                self.plist.unregister(fd)
        else:
            error = None
            if evt & select.POLLIN:
                try:
                    data = os.read(fd, 32767)
                except BaseException:
                    error = (fd, "error", traceback.format_exc())
            else:
                error = (fd, "disconnect", evt)

            if error:
                msg = (MAIN_MSGQ_PIPE_ERROR, error)
                self.plist.unregister(fd)

            else:
                msg = (MAIN_MSGQ_PIPE_DATA, (fd, data))
            # send message on for processing
            self.main_msgq.put(msg)

    def run(self):
        try:
            while self.running:
                for (fd, evt) in self.plist.poll():
                    self.proc(fd, evt)
        except BaseException:
            logging.error(traceback.format_exc())


class SignalsInstance:
    def __init__(self):
        self.main_msgq = queue.Queue()
        self.last_mqtt_send_idx = 0
        self.mqtt_streamers = []
        self.not_ready = []
        for i in range(0, multiprocessing.cpu_count()):
            n = "mqtt-streamer-%d" % i
            s = MqttStreamer(n, self.main_msgq)
            self.mqtt_streamers.append(s)
            self.not_ready.append(n)

        self.mqtt_listener = MqttListener("mqtt-listener", self.main_msgq)
        self.not_ready.append("mqtt-listener")

        self.pipe_watcher = PipeWatcher("pipe-watcher", self.main_msgq)

        self.topics = {}
        self.mqtt_topics = {}
        self.pipe_topics = {}

        

    def subscribe(self, topic, callback):
        assert callable(callback)

        msg = (MAIN_MSGQ_DEFER, (self._subscribe, (topic, callback)))
        self.main_msgq.put(msg)

    def unsubscribe(self, topic, callback):
        assert callable(callback)

        msg = (MAIN_MSGQ_DEFER, (self._unsubscribe, (topic, callback)))
        self.main_msgq.put(msg)

    def publish(self, topic, data):
        assert not data or isinstance(data, tuple)
 
        msg = (MAIN_MSGQ_DEFER, (self._publish, (topic, data)))
        self.main_msgq.put(msg)

    def addPipeWatch(self, fd, onData, onError):
        """place a watch on a pipe
               fd                is a readable file descriptor
               onData( chunk )   callback that processes data from pipe
               onError( "disconnect", poll event mask ) or onError( "error", "traceback" )

           there can be multiple watchers on a single pipe, the watch is created on
           the first watcher and removed whne the last watcher is removed.
        """
        msg = (MAIN_MSGQ_DEFER, (self._addPipeWatch, (fd, onData, onError)))
        self.main_msgq.put(msg)

    def removePipeWatch(self, fd, onData, onError):
        "remove watch from pipe"
        msg = (MAIN_MSGQ_DEFER, (self._removePipeWatch, (fd, onData, onError)))
        self.main_msgq.put(msg)

    # --- these are to be called in main.py no where else ----
    def mainloop(self):
        "called inside of main to process all events in the main thread"
        dispatch = {
            MAIN_MSGQ_PIPE_ERROR: self._on_pipe_error,
            MAIN_MSGQ_PIPE_DATA: self._on_pipe_data,
            MAIN_MSGQ_DEFER: self._on_defer,
            MAIN_MSGQ_MQTT_INBOUND: self._on_mqtt_inbound
        }

        # arbitrary counter 
        counter = 1
        interval = 15

        if LIMIT_MAIN_FUNC_EXEC_TIME > 0:
            def raise_timeout(*args):
                raise RuntimeError("timeout")
            signal.signal(signal.SIGALRM, raise_timeout)
        
         
        while True:
            # periodic function called at a maximum interval of once every
            # 15 seconds, in practice under a higher load many times a second 
            if (counter % interval) == 0:
                now = time.time()

                if LIMIT_MAIN_FUNC_EXEC_TIME > 0:
                    signal.alarm(LIMIT_MAIN_FUNC_EXEC_TIME)

                # call periodic tasks system wide.
                #print("=== PERIODOC")
                self.call_handler(self.topics, topics.Periodic, (now,))

                if LIMIT_MAIN_FUNC_EXEC_TIME > 0:
                    signal.alarm(0)

                counter += 1
                        
            try:
                # dequeue events (tag,args tuple)
                (cmd, args) = self.main_msgq.get(timeout=1)
            except queue.Empty:
                counter += 1
            finally:
                # get the method to execute
                h = dispatch.get(
                    cmd, lambda _x: logging.error(
                        "Unhandled command %s" %
                        cmd))

                if LIMIT_MAIN_FUNC_EXEC_TIME > 0:
                    signal.alarm(LIMIT_MAIN_FUNC_EXEC_TIME)

                try:
                    # execute App.<function> here
                    #print("---",h,args)
                    h(args)
                    #print("---after")
                except:
                    logging.error(traceback.format_exc())
                    logging.error("bad message: ",h,args)

                if LIMIT_MAIN_FUNC_EXEC_TIME > 0:
                    signal.alarm(0)
                

    def setup(self):
        "setup mqtt/pipe watch, block until ready"

        # kick off connections to mqtt comms
        self.mqtt_listener.start()
        for s in self.mqtt_streamers:
            s.start()

        # we can't proceed until all service threads are ready
        while len(self.not_ready) > 0:
            # get the ready messages
            (cmd, args) = self.main_msgq.get()
            # we can only handle one message type now.
            assert cmd == MAIN_MSGQ_READY
            n = args[0]
            i = self.not_ready.index(n)
            del self.not_ready[i]

        # service to handle pipe I/O
        self.pipe_watcher.start()

    def dispose(self):
        for s in self.mqtt_streamers:
            s.stop()
        self.mqtt_listener.stop()
        self.pipe_watcher.stop()


    """
      Basic structure of topic handlers to implement a pubsub framework

        topic -> { key: set([callbacks,that,handle,messages])

        client -> publish(key,args) -> for each cb in topic[key] -> cb(args) 
    """

    # defered functions for the access methods
    def _addPipeWatch(self, fd, onData, onError):
        if fd not in self.pipe_topics:
            self.pipe_topics[fd] = set()
            # first watcher
            self.pipe_watcher.register(fd)

        self.pipe_topics[fd].add((onData, onError,))

    def _removePipeWatch(self, fd, onData, onError):
        if fd in self.pipe_topics:
            k = (onData,onError)
            if fd in self.pipe_topics and k in self.pipe_topics[fd]:
                self.pipe_topics[fd].remove( k )        

                if len(self.pipe_topics[fd]) == 0:
                    # last watcher removed
                    self.pipe_watcher.unregister(fd)
                    del self.pipe_topics[fd]

    def _subscribe(self, topic, callback):
        topic_to_add = None
        t = None

        if topic.startswith("mqtt://"):
            topic_to_add = topic[len("mqtt://"):]
            self.mqtt_listener.subscribe(topic_to_add)
            t = self.mqtt_topics

        else:
            topic_to_add = topic
            t = self.topics

        if topic_to_add not in t:
            t[topic_to_add] = set()
        t[topic_to_add].add(callback)

    def _unsubscribe(self, topic, callback):
        topic_to_rem = None
        t = None

        if topic.startswith("mqtt://"):
            topic_to_rem = topic[len("mqtt://"):]
            t = self.mqtt_topics

        else:
            topic_to_rem = topic
            t = self.topics

        if topic_to_rem in t:
            if callback in t[topic_to_rem]:
                t[topic_to_rem].remove( callback )
                # if no more callbacks remove list
                if len(t[topic_to_rem]) == 0:
                    del t[topic_to_rem]


    def _publish(self, topic, data):
        if topic.startswith("mqtt://"):
            topic = topic[len("mqtt://"):]
            self.last_mqtt_send_idx = 0
            s = self.mqtt_streamers[self.last_mqtt_send_idx]
            if data:
                s.send(topic, data[0])
            else:
                s.send(topic, None)
            self.last_mqtt_send_idx = (
                self.last_mqtt_send_idx + 1) % len(self.mqtt_streamers)
        else:
            # send to local topic handlers.
            self.call_handler(self.topics, topic, data)

    # --- primitives ----------------

    def call_handler(self, cbTable, topic, args):
        if topic in cbTable:
            badCb = []
            for cb in cbTable[topic]:
                try:
                    if args:
                        cb(*args)
                    else:
                        cb()
                except BaseException:
                    logging.error(traceback.format_exc())
                    logging.error("cb = %s, args = %s" % (str(cb),str(args)))
                    badCb.append(cb)

            for cb in badCb:        
                cbTable[topic].remove(cb)  
                        

    def _on_pipe_error(self, args):
        fd, failure_type, data = args
        if fd in self.pipe_topics:
            for (onData, onError) in self.pipe_topics[fd]:
                try:
                    onError(failure_type, data)
                except BaseException:
                    logging.error(traceback.format_exc())
                    # remove watch, pipeWatcher loop already
                    # unregisterd fd from poll
                    self._removePipeWatch(fd, onData, onError)

    def _on_pipe_data(self, args):
        # process pipe data, this would usually be video data
        (fd, data) = args
        if fd in self.pipe_topics:
            for onData, onError in self.pipe_topics[fd]:
                try:
                    # route to data handlers
                    onData(data)
                except BaseException:
                    logging.error(traceback.format_exc())
                    # to prevent malicious behaviour unregister pipe
                    # and remove this handler
                    self._removePipeWatch(fd, onData, onError)

    def _on_defer(self, args):
        (func, fa) = args
        func(*fa)

    def _on_mqtt_inbound(self, args):
        m = args[0]
        self.call_handler(self.mqtt_topics, m.topic, (m,))

