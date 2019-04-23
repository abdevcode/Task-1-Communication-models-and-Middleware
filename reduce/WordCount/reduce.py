#!/usr/bin/env python
# -*- coding: latin-1 -*-
import pika
from cos_backend import COSBackend

cos = COSBackend()

# GLOBARS VARS
num_files = 0
dic = {}
bucket_name = ''
url_rmq = ''

"""
Funcio callback del rabbit-mq
"""
def callback(ch, method, properties, body):
    global num_files

    num_files -= 1
    if num_files <= 0:
        ch.stop_consuming();

    count_words(body)


"""
Funcio per comptar les paraules dels fitxers generats pels mappers
"""
def count_words(file_name_map):
    global bucket_name, dic

    data = cos.get_object(bucket_name, str(file_name_map, "latin_1"))
    data = str(data, "latin_1")
    aux = data.split(';')

    for line in aux:
        if len(line) > 0 and line.index(',') >= 0:
            word, count = line.split(',')
            try:
                if word in dic:
                    dic[word] += int(count)
                else:
                    dic[word] = int(count)
            except ValueError:
                continue

    # Eliminem el fitxer temp
    cos.delete_object(bucket_name, str(file_name_map, "latin_1"))


"""
Funcio principal del reduce
"""
def reduce_count_words(bckt_name, nom_fitxer, offset, numfit):
    global num_files, dic, bucket_name, url_rmq

    bucket_name = bckt_name
    num_files = int(numfit)
    dic = {}

    params = pika.URLParameters(url_rmq)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue='mapqueue')
    channel.basic_consume(callback, queue='mapqueue', no_ack=True)
    channel.start_consuming()
    connection.close()

    result = ""

    for key, value in dic.items():
        output = "%s,%s;" % (key, value)
        result += output

    cos.put_object(bucket_name, "res_%s" % (nom_fitxer), result.encode('latin_1'))

    params = pika.URLParameters(url_rmq)
    params.socket_timeout = 5
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue='reducequeue')
    channel.basic_publish(exchange='', routing_key='reducequeue', body="res_%s" % (nom_fitxer))
    connection.close()
