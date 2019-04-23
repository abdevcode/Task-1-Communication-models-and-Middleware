#!/usr/bin/env python
import pika
import ibm_cloud_config as icc
import sys
from ibm_cf_connector import CloudFunctions
from cos_backend import COSBackend
import time

cos = COSBackend(icc.config_cos)
cfc = CloudFunctions(icc.config_cf)

nom_fitxer = sys.argv[1]
num_part = int(sys.argv[2])

def callback(ch, method, properties, body):
    print("[x] Recibido " + str(body))
    print("[x] Bajando el fichero del COSBackend")
    data = cos.get_object(icc.config_ibm['bucket_name'], str(body, "latin_1"))
    print("[x] Contenido del fichero: ")
    data = str(data, "latin_1")
    print(data)
    ch.stop_consuming();

def main():
    start = time.time()

    file_head = cos.head_object(icc.config_ibm['bucket_name'], nom_fitxer)
    content_length = int(file_head['content-length'])
    print('[X] Tamany fitxer: %d' % (content_length))
    offset = int(content_length / num_part)

    offset_ini = 0
    offset_fi = offset

    params_map = {}
    print('[x] Map:')
    for i in range(0, num_part-1):
        params_map["bucket_name"] = icc.config_ibm['bucket_name']
        params_map["nom_fitxer"] = nom_fitxer
        params_map["inici"] = offset_ini
        params_map["offset"] = offset_fi

        print (params_map)

        cfc.invoke('map', params_map)
        #cos.delete_object(icc.config_ibm['bucket_name'], "%d_%s" % (offset_ini, nom_fitxer))

        offset_ini += offset
        offset_fi += offset

    params_map["bucket_name"] = icc.config_ibm['bucket_name']
    params_map["nom_fitxer"] = nom_fitxer
    params_map["inici"] = offset_ini
    params_map["offset"] = content_length
    print(params_map)
    cfc.invoke('map', params_map)
    #cos.delete_object(icc.config_ibm['bucket_name'], "%s_%s.txt" % (nom_fitxer, offset_ini))

    # REDUCE
    params_reduce = {}

    params_reduce["bucket_name"] = icc.config_ibm['bucket_name']
    params_reduce["nom_fitxer"] = nom_fitxer
    params_reduce["numfit"] = num_part
    params_reduce["offset"] = content_length
    print('[x] Reduce:')
    print(params_reduce)

    cfc.invoke('WordCount', params_reduce)
    #cfc.invoke('CountingWords', params_reduce)

    """
    Despues de ejecutar los maps esperamos la respuesta del reducer
    """
    print('[x] Esperando la cola del reduce...')
    url = icc.config_ibm['amqp_url']

    params = pika.URLParameters(url)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue='reducequeue')
    channel.basic_consume('reducequeue', callback, auto_ack=True)
    channel.start_consuming()
    connection.close()

    end = time.time()
    print('[x] Fin del programa :)')
    print('[X] Temps exec: %s' % (end - start))



main()
