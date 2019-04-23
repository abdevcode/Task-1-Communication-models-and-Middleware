#!/usr/bin/env python
# -*- coding: latin-1 -*-
import pika
from cos_backend import COSBackend

cos = COSBackend()

url_rmq = 'amqp://lsswhkpi:DROG-7JFX0xR9tbXLRKSVGCFNNIzMJ-J@caterpillar.rmq.cloudamqp.com/lsswhkpi'


def map_count_words(bucket_name, nom_fitxer, inici, offset):
	global url_rmq

	data = cos.get_object(bucket_name, nom_fitxer, False, extra_get_args={'Range': 'bytes=%d-%d' % (inici, offset)})

	data = get_word(data)
	result_count = ""
	l_list = data.split()

	for word in l_list:
		output = "%s,%d;" % (str(word, "latin_1"), 1)
		result_count += output

	file_name_result = "%d_%s" % (inici, nom_fitxer)

	cos.put_object(bucket_name, file_name_result, result_count.encode("latin_1"))

	# Afegim a la cua el fitxer processat
	params = pika.URLParameters(url_rmq)
	params.socket_timeout = 5
	connection = pika.BlockingConnection(params)
	channel = connection.channel()
	channel.queue_declare(queue='mapqueue')
	channel.basic_publish(exchange='', routing_key='mapqueue', body=file_name_result)
	connection.close()



def get_word(word):
	word = word.replace(b".", b"")
	word = word.replace(b":", b"")
	word = word.replace(b",", b"")
	word = word.replace(b"/", b"")
	word = word.replace(b";", b"")
	word = word.replace(b"*", b"")
	word = word.replace(b"?", b"")
	word = word.replace(b"!", b"")
	word = word.replace(b"(", b"")
	word = word.replace(b")", b"")
	word = word.replace(b"{", b"")
	word = word.replace(b"}", b"")
	word = word.replace(b"[", b"")
	word = word.replace(b"]", b"")
	word = word.replace(b"<", b"")
	word = word.replace(b">", b"")
	word = word.replace(b"+", b"")
	word = word.replace(b"=", b"")
	word = word.replace(b"_", b"")
	word = word.replace(b"-", b"")
	word = word.replace(b"&", b"")
	word = word.replace(b"^", b"")
	word = word.replace(b"%", b"")
	word = word.replace(b"$", b"")
	word = word.replace(b"#", b"")
	word = word.replace(b"@", b"")
	word = word.replace(b"\n", b" ")
	word = word.replace(b"\r", b" ")
	word = word.replace(b"\xef", b"")
	word = word.replace(b"\xbb", b"")
	word = word.replace(b"\xbf", b"")

	return word
