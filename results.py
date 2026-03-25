from typing import Union, List
from shared_functions import get_where_clause, build_unnest_logic
from config import files, con
from models import LieuResultats, ResultatCandidat, StatsTour

async def get_aggregated_results(
    filter_col: str = None, 
    filter_value: Union[str, List[str]] = None, 
    type_lieu: str = "national", 
    display_col: str = None, 
    label: str = "France Entière",
    by_nuance: bool = False
):
    tours_data = []
    where_clause, params = get_where_clause(filter_col, filter_value)
    
    # On garde le label par défaut, mais on se prépare à le mettre à jour
    final_label = label

    for tour_num, s3_path in files.items():
        try:
            # A. Analyse des colonnes
            cols_query = f"DESCRIBE SELECT * FROM read_parquet('{s3_path}') LIMIT 0"
            all_cols = [c[0] for c in con.execute(cols_query).fetchall()]
            indices = [c.replace("Voix ", "") for c in all_cols if c.startswith("Voix ")]
            
            unnest_list = build_unnest_logic(all_cols, indices, by_nuance)
            
            # Logique pour récupérer le nom officiel (ex: transformer le code 64445 en "Pau")
            name_select = f"ANY_VALUE(\"{display_col}\")" if display_col and display_col in all_cols else "NULL"

            # B. Requête SQL
            query = f"""
            WITH base_data AS (
                SELECT *, ("Code département" || '-' || "Code commune" || '-' || "Code BV") as id_bv
                FROM read_parquet('{s3_path}')
                {where_clause}
            ),
            stats_global AS (
                SELECT 
                    SUM(Inscrits) as t_ins, SUM(Votants) as t_vot, SUM(Exprimés) as t_exp,
                    {name_select} as official_name
                FROM (
                    SELECT DISTINCT id_bv, Inscrits, Votants, Exprimés, 
                    "{display_col if display_col in all_cols else 'Code commune'}" 
                    FROM base_data
                )
            ),
            voix_total AS (
                SELECT (unnest([{unnest_list}])).n as nom_group, (unnest([{unnest_list}])).v as voix
                FROM base_data
            )
            SELECT 
                nom_group, SUM(voix), s.t_ins, s.t_vot, s.t_exp, s.official_name
            FROM voix_total v, stats_global s
            WHERE v.nom_group IS NOT NULL AND v.voix IS NOT NULL
            GROUP BY 1, 3, 4, 5, 6 ORDER BY 2 DESC;
            """
            
            res = con.execute(query, params).fetchall()
            if not res: continue

            # MISE À JOUR DU LABEL : Si la DB renvoie un nom officiel, on l'utilise
            if res[0][5]:
                final_label = res[0][5]

            candidats = [
                ResultatCandidat(
                    nom=row[0], voix=int(row[1]),
                    pourcentage_exprimes=round((row[1] / row[4]) * 100, 2) if row[4] > 0 else 0,
                    pourcentage_inscrits=round((row[1] / row[2]) * 100, 2) if row[2] > 0 else 0
                ) for row in res
            ]

            tours_data.append(StatsTour(
                num_tour=tour_num, inscrits=int(res[0][2]), 
                votants=int(res[0][3]), exprimes=res[0][4],
                candidats=candidats
            ))

        except Exception as e:
            print(f"Erreur Tour {tour_num}: {e}")

    return LieuResultats(nom_lieu=final_label, type_lieu=type_lieu, tours=tours_data)