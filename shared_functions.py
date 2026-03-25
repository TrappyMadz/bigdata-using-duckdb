import os
from config import con

def configure_s3():
    """Configure les accès S3 pour DuckDB"""
    endpoint = os.getenv('CELLAR_ENDPOINT', '').replace('https://', '')
    access_key = os.getenv('CELLAR_ACCESS_KEY')
    secret_key = os.getenv('CELLAR_SECRET_KEY')
    
    con.execute(f"SET s3_endpoint='{endpoint}';")
    con.execute(f"SET s3_access_key_id='{access_key}';")
    con.execute(f"SET s3_secret_access_key='{secret_key}';")
    con.execute("SET s3_url_style='path';")
    con.execute("SET s3_region='default';")

def build_unnest_logic(all_cols, indices, by_nuance):
    """Construit la liste des structures pour le UNNEST SQL"""
    structs = []
    for i in indices:
        if by_nuance:
            val_col = f"\"Nuance liste {i}\"" if f"Nuance liste {i}" in all_cols else "NULL"
            name_logic = f"COALESCE({val_col}::VARCHAR, 'SANS NUANCE')"
        else:
            nom_col = f"\"Nom candidat {i}\"" if f"Nom candidat {i}" in all_cols else "NULL"
            liste_col = f"\"Libellé de liste {i}\"" if f"Libellé de liste {i}" in all_cols else "NULL"
            name_logic = f"COALESCE({nom_col}::VARCHAR, {liste_col}::VARCHAR, 'Sans nom')"
        structs.append(f"{{'n': {name_logic}, 'v': \"Voix {i}\"}}")
    return ", ".join(structs)

def get_where_clause(filter_col, filter_value):
    """Génère la clause WHERE et les paramètres pour DuckDB"""
    if filter_col and filter_value:
        search_list = [filter_value] if isinstance(filter_value, str) else filter_value
        return f"WHERE \"{filter_col}\"::VARCHAR IN (SELECT unnest(?))", [search_list]
    return "", []