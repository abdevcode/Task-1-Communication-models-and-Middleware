import cos_backend as cs
import reduce as r

def main(args):
    bucket_name = args.get('bucket_name')
    nom_fitxer = args.get('nom_fitxer')
    numfit = args.get('numfit')
    offset = args.get('offset')

    r.reduce_total_words(bucket_name, nom_fitxer, offset, numfit)
    
    return {"end": "ok"}
