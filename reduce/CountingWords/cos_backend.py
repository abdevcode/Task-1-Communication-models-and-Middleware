#!/bin/env python

import ibm_botocore
import ibm_boto3

class COSBackend:
	def __init__(self):
		"""
		service_endpoint = config['endpoint'].replace('http:', 'https:')
		secret_key = config['secret_key']
		acces_key = config['access_key']
		"""
		service_endpoint = ''
		secret_key = ''
		acces_key = ''

		client_config = ibm_botocore.client.Config(max_pool_connections=200)
		self.cos_client= ibm_boto3.client('s3',
											aws_access_key_id=acces_key,
											aws_secret_access_key=secret_key,
											config=client_config,
											endpoint_url=service_endpoint)


	def put_object(self, bucket_name, key, data):
		"""
		Put an object in Cos. Override the object if the key already exists.
		:param key: key of the object.
		:param data: data of the object
		:type data: str/bytes
		:return: None
		"""
		try:
			res = self.cos_client.put_object(Bucket=bucket_name,Key=key,Body=data)
			status = 'OK' if res['ResponseMetadata']['HTTPStatusCode']==200 else 'Error'
		except ibm_botocore.exceptions.ClientError as e:
			raise e

	def get_object(self, bucket_name, key, stream=False, extra_get_args={}):
		"""
		Get object from cOs with a key. Throws StorageNoSuchkeyError if the given key
		:param key: key of the object
		:return: Data of the object
		:rtype: str/bytes
		"""
		try:
			res = self.cos_client.get_object(Bucket=bucket_name, Key=key, **extra_get_args)
			if stream:
				data=res['Body']
			else:
				data=res['Body'].read()
			return data
		except ibm_botocore.exceptions.ClientError as e:
			raise e


	def head_object(self, bucket_name, key):
		"""
		Head object from COS with a key. Throws StorageNoSuchkeyError if the given key does not exist.
		:param key: key of the object
		:return: Data of the object
		:rtype: str/bytes
		"""
		try:
			metadata = self.cos_client.head_object(Bucket=bucket_name, Key=key)
			return metadata['ResponseMetadata']['HTTPHeaders']
		except ibm_botocore.exceptions.ClientError as e:
			raise e

	def delete_object(self, bucket_name, key):
		"""
		Delete an object from storage.
		:param bucket: bucket name
		:param key: data key
		"""
		return self.cos_client.delete_object(Bucket=bucket_name,Key=key)
