# coding:utf-8
# import json
#
# from kafka import KafkaConsumer
#
# consumer = KafkaConsumer('test_rhj', bootstrap_servers=['49.4.90.247:6667'], value_deserializer=lambda m: json.loads(m.decode('utf-8')))
#
# for msg in consumer:
#     recv = "%s:%d:%d: key=%s value=%s" % (msg.topic, msg.partition, msg.offset, msg.key, msg.value)
#     print(recv)



from pykafka.client import KafkaClient
import logging
from pykafka.common import OffsetType

import json
import urllib
import threading
import sys
from kazoo.client import KazooClient
from time import sleep
from _socket import gethostname

logging.basicConfig(level=logging.INFO)

consumer_logger = logging.getLogger('consumer')



# 2, 连接kafka集群
client = KafkaClient('49.4.90.247:6667')

nmq = client.topics['nmq']

consumer = nmq.get_('balance-consumer',
                                     zookeeper_connect='localhost:3000,localhost:3001,localhost:3002/kafka',
                                     auto_offset_reset=OffsetType.LATEST, auto_commit_enable=True,
                                     num_consumer_fetchers=3)


# 3, 启动HTTP服务
def httpd_main(consumer):
    class ResetOffsetRequestHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):
        def __init__(self, request, client_addr, server):
            SimpleHTTPServer.SimpleHTTPRequestHandler.__init__(self, request, client_addr, server)
            self.consumer = consumer

        def do_GET(self):
            if self.path.startswith('/reset-offset'):
                try:
                    consumer.reset_offsets()
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write("succeed")
                except:
                    self.send_response(500)
                    self.end_headers()
                    self.wfile.write("fail")

    SocketServer.TCPServer.allow_reuse_address = True
    httpd = SocketServer.TCPServer(("", int(http_port)), ResetOffsetRequestHandler)
    httpd.serve_forever()


httpd_thread = threading.Thread(target=httpd_main, args=(consumer,))
httpd_thread.setDaemon(True)
httpd_thread.start()

# 4, 开始consume消费消息
while True:
    print(1)
    msg = consumer.consume()
    print(2)
    request = json.loads(msg.value)
    print(request)

