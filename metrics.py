from typing import Union, List
from shared_functions import get_where_clause
from config import files, con
from models import PoliticStats, NuanceSeats, NuanceMajorite, VictoiresTerritoire, NuanceVictoires

async def get_political_metrics(filter_col: str = None, filter_value: Union[str, List[str]] = None, label: str = "France", type_lieu: str = "national"):
    where_clause, params = get_where_clause(filter_col, filter_value)
    path_t1, path_t2 = files.get(1), files.get(2)
    not_in_prefix = "WHERE" if not where_clause else "AND"

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

    query = f"""
    WITH t2_unnested AS (
        SELECT "Code commune", unnest([{gen_structs(cols_t2)}]) as d 
        FROM read_parquet('{path_t2}') {where_clause}
    ),
    t1_unnested AS (
        SELECT "Code commune", unnest([{gen_structs(cols_t1)}]) as d 
        FROM read_parquet('{path_t1}') 
        {where_clause} 
        {not_in_prefix} "Code commune" NOT IN (SELECT "Code commune" FROM t2_unnested)
    ),
    consolidated AS (
        SELECT * FROM t2_unnested UNION ALL SELECT * FROM t1_unnested
    ),
    commune_nuance_totals AS (
        SELECT "Code commune", d.n as nuance, SUM(d.v) as total_voix, MAX(d.s) as sieges
        FROM consolidated WHERE d.n IS NOT NULL
        GROUP BY 1, 2
    ),
    winners AS (
        SELECT *, RANK() OVER(PARTITION BY "Code commune" ORDER BY total_voix DESC) as rang
        FROM commune_nuance_totals
    )
    SELECT 
        nuance, SUM(sieges), COUNT(CASE WHEN rang = 1 THEN 1 END),
        SUM(CASE WHEN rang = 1 THEN sieges ELSE 0 END),
        SUM(CASE WHEN rang > 1 THEN sieges ELSE 0 END)
    FROM winners GROUP BY 1 ORDER BY 2 DESC;
    """
    
    res = con.execute(query, params * 2 if params else []).fetchall()
    
    return PoliticStats(
        nom_lieu=label, type_lieu=type_lieu, tour=0,
        stats_sieges=[NuanceSeats(nuance=r[0], sieges_total=r[1], sieges_majorite=r[3], sieges_opposition=r[4]) for r in res],
        stats_victoires=[NuanceMajorite(nuance=r[0], nb_communes_gagnees=r[2]) for r in res]
    )

async def get_winning_communes_stats(filter_col: str = None, filter_value: Union[str, List[str]] = None, label: str = "France", type_lieu: str = "national"):
    path_t1, path_t2 = files.get(1), files.get(2)
    where_clause, params = get_where_clause(filter_col, filter_value)
    not_in_prefix = "WHERE" if not where_clause else "AND"

    cols_t1 = [c[0] for c in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{path_t1}') LIMIT 0").fetchall()]
    cols_t2 = [c[0] for c in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{path_t2}') LIMIT 0").fetchall()]
    indices = [c.replace("Voix ", "") for c in cols_t1 if c.startswith("Voix ")]
    
    def gen_structs_win(current_cols):
        s = []
        for i in indices:
            n = f"\"Nuance liste {i}\"" if f"Nuance liste {i}" in current_cols else "NULL"
            v = f"\"Voix {i}\"" if f"Voix {i}" in current_cols else "NULL"
            s.append(f"{{'n': {n}::VARCHAR, 'v': {v}}}")
        return ", ".join(s)

    sql = f"""
    WITH t2_unnested AS (
        SELECT "Code commune", unnest([{gen_structs_win(cols_t2)}]) as data 
        FROM read_parquet('{path_t2}') {where_clause}
    ),
    t1_unnested AS (
        SELECT "Code commune", unnest([{gen_structs_win(cols_t1)}]) as data 
        FROM read_parquet('{path_t1}') 
        {where_clause} 
        {not_in_prefix} "Code commune" NOT IN (SELECT "Code commune" FROM t2_unnested)
    ),
    consolidated AS (
        SELECT * FROM t2_unnested UNION ALL SELECT * FROM t1_unnested
    ),
    votes_par_commune AS (
        SELECT "Code commune", data.n as nuance, SUM(data.v) as voix
        FROM consolidated WHERE data.n IS NOT NULL
        GROUP BY 1, 2
    ),
    classement AS (
        SELECT nuance, 
               ROW_NUMBER() OVER(PARTITION BY "Code commune" ORDER BY voix DESC) as rang
        FROM votes_par_commune
    ),
    final_counts AS (
        SELECT nuance, COUNT(*) as nb_villes
        FROM classement
        WHERE rang = 1
        GROUP BY 1
    ),
    total_villes AS (SELECT SUM(nb_villes) as grand_total FROM final_counts)
    
    SELECT 
        nuance, 
        nb_villes, 
        round((nb_villes * 100.0 / (SELECT grand_total FROM total_villes)), 2)
    FROM final_counts
    ORDER BY nb_villes DESC;
    """
    
    res = con.execute(sql, params * 2 if params else []).fetchall()
    
    return VictoiresTerritoire(
        nom_lieu=label,
        type_lieu=type_lieu,
        total_communes_traitees=sum(r[1] for r in res) if res else 0,
        resultats=[NuanceVictoires(nuance=r[0], nb_communes=r[1], pourcentage=r[2]) for r in res]
    )