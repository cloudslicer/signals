# signals
Python publisher subscriber library that integrates with paho for handling IO, messaging and more

This is a module created for another project which I thought could be a useful general purpose
library. It combines an I/O processing loop, with MQTT and a small publisher subscriber module
to allow for a uniform, thread safe way to assign callbacks to pipes (or sockets) as well as mqtt 
andlocal topics.  


def subscribe(topic, callback):
--

 associate a topic with a callback so any call to subscribe.publish
 will be routed to the callback that the user has passed in. There 
 can be an unlimited number subscribers for a topic.

   parameters:
   
   topic: can be any string, if it is starts with 'mqtt://' it
     is is forwarded to paho and the remaining string after 'mqtt://'
     is used an the mqtt topic

   callback: a function to handle the topic.
     if topic starst with mqtt the function will be passed a paho message
     object, otherwise it will be passed whever argument the user
     passes in subscriber.publish 

   example:
   --------
      #Scenario #1, remote machine sends out a temperature reading from a 
      #sensor ...

      # listener machine(s)  
      def handler1(mqtt_message):
          # handle mqtt_message from remote device
          # this is a paho message object 
          #See https://pypi.python.org/pypi/paho-mqtt/1.1
          #    mqtt_message.payload -> data
          #    mqtt_message.topic -> topic
      signals.subscribe("mqtt://remotesensor/temp",handler1) 

      # publisher machine(s)
      signals.publish("mqtt://remotesensor/temp", (json.dumps({'temp':-10.8}),) )
      

      #Scenario #2: two threads
      
      # listener thread(s)
      def handler2( p, q ):
          # arbitrary data p, q
      signals.subscribe("p_q_topic", handler2) 
  
      # sender thread(s) 
      # arguments for handler2      p,   q
      #                             |    |
      signals.publish("p_q_topic", (10,"aaa",) ) 



def unsubscribe(topic, callback):
--
 remove a callback from a topic 

   parameters:
   

   topic: can be any string, if it is starts with 'mqtt://' it
     is is forwarded to paho and the remaining string after 'mqtt://'
     is used an the mqtt topic

   callback: a function to handle the topic.
     if topic starst with mqtt the function will be passed a paho message
     object, otherwise it will be passed whever argument the user
     passes in subscriber.publish 

   example:
   --------

     # subscribe to a topic 
     signals.subscribe(topic, callback)

     # when no longer needed unsubscribe
     signals.unsubscribe(topic, callback) 


def publish( topic, args ):
--
 Multicast 'args' to N number of listeners to 'topic'

   parameters:
   

   topic: can be any string, if it is starts with 'mqtt://' it
     is is forwarded to paho and the remaining string after 'mqtt://'
     is used an the mqtt topic

   args: a tuple of arguments,if topic starts with mqtt:// then arguments
     is a single item such as ("hello",)

   example:
   --------
   see subscribe example
 

def addPipeWatch(self, fd, onData, onError):
--
  Add fd to an internal poll loop on data it gets routed to 
  onData if an error has occured onError is called. This
  
   parameters:
   

   fd: file descriptor
   onData: function(chunk)
              chunk - arbitrary data read from pipe

   onError: function(failure_type, reason)
               failure_type: 'disconnect'|'error'
               reason: description of error
   

def removePipeWatch(self, fd, onData, onError):
--
   Remove fd from internal poll list 





Usage:
--

```
import signals

#optional configuration based on you mqtt message broker

#Default port 
#signals.MQTT_PORT = 1883
#Default host
#signals.MQTT_HOST = '127.0.0.1'

#if your message broker requires a password 
#signals.MQTT_AUTH = {"username":"...", "password":"..."}
#else no protection (default)
#signals.MQTT_AUTH = None


#basic structure of your app
#>  subscribe to events 
#>  call mainloop to service events

signals.subscribe(topic,callback)


#blocking call to handle all events
signals.mainloop() 
```
  
