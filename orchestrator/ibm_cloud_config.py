import yaml
with open('ibm_cloud_config', 'r') as config_file:
    res = yaml.safe_load(config_file)


config_cf = res['ibm_cf']
config_cos = res['ibm_cos']
config_ibm = res['ibm_conf']
