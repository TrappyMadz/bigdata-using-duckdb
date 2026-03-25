from typing import Union, List
from config import files, con
from shared_functions import get_where_clause
from models import RapportSiegesTerritoire, ProportionSieges, DemographicStats, StatSet
import os
from dotenv import load_dotenv

load_dotenv()

async def get_seats_distribution_stats(filter_col: str = None, filter_value: Union[str, List[str]] = None, label: str = "France", type_lieu: str = "national"):
    path_t1, path_t2 = files.get(1), files.get(2)
    where_clause, params = get_where_clause(filter_col, filter_value)
    
    # Correction Parser Error (AND)
    not_in_prefix = "WHERE" if not where_clause else "AND"

    # Correction Binder Error (Schema mismatch)
    cols_t1 = [c[0] for c in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{path_t1}') LIMIT 0").fetchall()]
    cols_t2 = [c[0] for c in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{path_t2}') LIMIT 0").fetchall()]
    indices = [c.replace("Voix ", "") for c in cols_t1 if c.startswith("Voix ")]

    def gen_structs(current_cols):
        s = []
        for i in indices:
            n = f"\"Nuance liste {i}\"" if f"Nuance liste {i}" in current_cols else "NULL"
            v = f"\"Voix {i}\"" if f"Voix {i}" in current_cols else "NULL"
            siege = f"\"Sièges au CM {i}\"" if f"Sièges au CM {i}" in current_cols else "NULL"
            s.append(f"{{'n': {n}::VARCHAR, 'v': {v}, 's': {siege}}}")
        return ", ".join(s)

    sql = f"""
    WITH t2_unnested AS (
        SELECT "Code commune", unnest([{gen_structs(cols_t2)}]) as data 
        FROM read_parquet('{path_t2}') {where_clause}
    ),
    t1_unnested AS (
        SELECT "Code commune", unnest([{gen_structs(cols_t1)}]) as data 
        FROM read_parquet('{path_t1}') 
        {where_clause} 
        {not_in_prefix} "Code commune" NOT IN (SELECT "Code commune" FROM t2_unnested)
    ),
    consolidated AS (
        SELECT * FROM t2_unnested UNION ALL SELECT * FROM t1_unnested
    ),
    commune_results AS (
        SELECT 
            "Code commune", 
            data.n as nuance, 
            SUM(data.v) as total_voix, 
            MAX(data.s) as sieges_obtenus
        FROM consolidated
        WHERE data.n IS NOT NULL
        GROUP BY 1, 2
    ),
    ranked_results AS (
        SELECT *, 
               RANK() OVER(PARTITION BY "Code commune" ORDER BY total_voix DESC) as rang
        FROM commune_results
    ),
    final_stats AS (
        SELECT 
            nuance,
            SUM(CASE WHEN rang = 1 THEN sieges_obtenus ELSE 0 END) as maj,
            SUM(CASE WHEN rang > 1 THEN sieges_obtenus ELSE 0 END) as opp,
            SUM(sieges_obtenus) as total
        FROM ranked_results
        GROUP BY 1
    )
    SELECT *, ROUND(total * 100.0 / SUM(total) OVER(), 2) as ratio
    FROM final_stats
    WHERE total > 0
    ORDER BY total DESC;
    """
    
    # Correction Invalid Input (params utilisés 2 fois dans la requête)
    res = con.execute(sql, params * 2 if params else []).fetchall()
    total_global = sum(r[3] for r in res) if res else 0

    return RapportSiegesTerritoire(
        nom_lieu=label,
        type_lieu=type_lieu,
        total_sieges_territoire=total_global,
        repartition=[
            ProportionSieges(
                nuance=r[0],
                sieges_majorite=int(r[1]),
                sieges_opposition=int(r[2]),
                total_sieges=int(r[3]),
                ratio_force=r[4]
            ) for r in res
        ]
    )

async def get_demographic_stats(filter_col: str = None, filter_value: Union[str, List[str]] = None, label: str = "France", type_lieu: str = "national"):
    bucket = os.getenv('CELLAR_BUCKET_NAME')
    s3_path = files.get(1)

    search_list = [filter_value] if isinstance(filter_value, str) else filter_value
    where_clause = f"WHERE \"{filter_col}\"::VARCHAR IN (SELECT unnest(?))" if filter_col else ""
    params = [search_list] if filter_col else []

    sql = f"""
    WITH commune_totals AS (
        -- On somme les bureaux de vote pour avoir le total par commune
        SELECT 
            "Code commune", 
            SUM(Inscrits::BIGINT) as t_ins, 
            SUM(Votants::BIGINT) as t_vot
        FROM read_parquet('{s3_path}')
        {where_clause}
        GROUP BY "Code commune"
    )
    SELECT 
        -- Stats pour les Inscrits
        AVG(t_ins), MEDIAN(t_ins), QUANTILE_CONT(t_ins, 0.25), 
        QUANTILE_CONT(t_ins, 0.75), QUANTILE_CONT(t_ins, 0.90), QUANTILE_CONT(t_ins, 0.99),
        -- Stats pour les Votants
        AVG(t_vot), MEDIAN(t_vot), QUANTILE_CONT(t_vot, 0.25), 
        QUANTILE_CONT(t_vot, 0.75), QUANTILE_CONT(t_vot, 0.90), QUANTILE_CONT(t_vot, 0.99)
    FROM commune_totals
    WHERE t_ins > 0;
    """
    
    r = con.execute(sql, params).fetchone()
    
    return DemographicStats(
        nom_lieu=label,
        type_lieu=type_lieu,
        inscrits=StatSet(
            moyenne=round(r[0], 2), mediane=r[1], p25=r[2], p75=r[3], p90=r[4], p99=r[5]
        ),
        votants=StatSet(
            moyenne=round(r[6], 2), mediane=r[7], p25=r[8], p75=r[9], p90=r[10], p99=r[11]
        )
    )