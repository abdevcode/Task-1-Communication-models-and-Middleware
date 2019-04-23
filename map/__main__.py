import map as m

def main(args):
    bucket_name = args.get('bucket_name')
    nom_fitxer = args.get('nom_fitxer')
    inici = args.get('inici')
    offset = args.get('offset')

    m.map_count_words(bucket_name, nom_fitxer, inici, offset)
    
    return {"end": "ok"}
