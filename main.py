from fastapi import FastAPI, HTTPException
from config import con, files
from shared_functions import configure_s3
from models import LieuResultats, FranceParticipation, PoliticStats, VictoiresTerritoire, RapportSiegesTerritoire, StatistiquesSieges, DemographicStats, DEPARTEMENTS_PAR_REGION, ParticipationStats
from typing import Union, List
from results import get_aggregated_results
from metrics import get_political_metrics, get_winning_communes_stats
from stats import get_seats_distribution_stats, get_demographic_stats
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()
con.execute("INSTALL httpfs; LOAD httpfs;")
configure_s3()

@app.get("/")
async def get_health():
    return {"message":"Hello there !"}

@app.get("/communes/{nom}", response_model=LieuResultats)
async def get_by_commune(nom: str):
    # On passe le nom saisi comme label de secours, mais display_col va chercher le vrai nom
    return await get_aggregated_results("Libellé commune", nom, "commune", display_col="Libellé commune", label=nom)

@app.get("/communes/code/{code}", response_model=LieuResultats)
async def get_by_insee_code(code: str):
    # Ici, le label par défaut est le code (ex: 64445), mais display_col va le remplacer par "Pau"
    return await get_aggregated_results("Code commune", code, "commune", display_col="Libellé commune", label=code)

@app.get("/departements/{nom}", response_model=LieuResultats)
async def get_by_departement(nom: str):
    return await get_aggregated_results("Libellé département", nom, "département", display_col="Libellé département")

@app.get("/regions/{nom}", response_model=LieuResultats)
async def get_by_region(nom: str):
    departements = DEPARTEMENTS_PAR_REGION.get(nom)
    if not departements:
        raise HTTPException(status_code=404, detail=f"Région '{nom}' inconnue")
    # Pas de display_col ici, on utilise le label "nom" (ex: "Nouvelle-Aquitaine")
    return await get_aggregated_results("Libellé département", departements, "région", label=nom)

# Nuances par département
@app.get("/departements/{nom}/nuances", response_model=LieuResultats)
async def get_nuances_by_departement(nom: str):
    # On agrège toutes les voix par nuance pour le département donné
    return await get_aggregated_results(
        "Libellé département", nom, "département", 
        display_col="Libellé département", 
        by_nuance=True
    )

# Nuances par région
@app.get("/regions/{nom}/nuances", response_model=LieuResultats)
async def get_nuances_by_region(nom: str):
    departements = DEPARTEMENTS_PAR_REGION.get(nom)
    if not departements:
        raise HTTPException(status_code=404, detail=f"Région '{nom}' inconnue")
    
    # On agrège toutes les voix de tous les départements de la région par nuance
    return await get_aggregated_results(
        "Libellé département", departements, "région", 
        label=nom, 
        by_nuance=True
    )

@app.get("/france/nuances", response_model=LieuResultats)
async def get_national_nuances():
    return await get_aggregated_results(
        type_lieu="national", 
        label="France Entière", 
        by_nuance=True
    )

@app.get("/france/participation", response_model=FranceParticipation)
async def get_france_participation():

    results = []

    for tour_num, s3_path in files.items():
        try:
            # Requête ultra-rapide : on somme directement les colonnes de base
            query = f"""
                SELECT 
                    SUM(Inscrits::BIGINT) as total_inscrits,
                    SUM(Votants::BIGINT) as total_votants,
                    SUM(Abstentions::BIGINT) as total_abstentions
                FROM read_parquet('{s3_path}')
            """
            
            res = con.execute(query).fetchone()
            
            if res and res[0] is not None:
                ins, vot, abs_ = res
                results.append(ParticipationStats(
                    tour=tour_num,
                    inscrits=ins,
                    votants=vot,
                    abstentions=abs_,
                    taux_participation=round((vot / ins) * 100, 2) if ins > 0 else 0,
                    taux_abstention=round((abs_ / ins) * 100, 2) if ins > 0 else 0
                ))
        except Exception as e:
            print(f"Erreur participation Tour {tour_num}: {e}")

    if not results:
        raise HTTPException(status_code=404, detail="Données de participation indisponibles")

    return FranceParticipation(details=results)

async def get_seats_distribution(filter_col: str = None, filter_value: Union[str, List[str]] = None, label: str = "France", type_lieu: str = "national"):
    bucket = os.getenv('CELLAR_BUCKET_NAME')
    # On utilise le fichier du 20/03 qui contient TOUTES les communes
    s3_path = f"s3://{bucket}/municipales-2026-resultats-bv-par-communes-2026-03-20.parquet"

    search_list = [filter_value] if isinstance(filter_value, str) else filter_value
    where_clause = f"WHERE \"{filter_col}\"::VARCHAR IN (SELECT unnest(?))" if filter_col else ""
    params = [search_list] if filter_col else []

    # 1. Identification des colonnes de sièges
    cols_query = f"DESCRIBE SELECT * FROM read_parquet('{s3_path}') LIMIT 0"
    all_cols = [c[0] for c in con.execute(cols_query).fetchall()]
    indices = [c.replace("Voix ", "") for c in all_cols if c.startswith("Voix ")]
    
    # On crée la somme horizontale : (COALESCE("Sièges au CM 1", 0) + ...)
    sum_parts = [f"COALESCE(\"Sièges au CM {i}\", 0)" for i in indices]
    horizontal_sum = " + ".join(sum_parts)

    sql = f"""
    WITH unique_communes AS (
        -- On prend une seule ligne par commune pour avoir le total de sièges réel
        SELECT DISTINCT "Code commune", ({horizontal_sum}) as total_sieges
        FROM read_parquet('{s3_path}')
        {where_clause}
    )
    SELECT 
        AVG(total_sieges) as moyenne,
        MEDIAN(total_sieges) as mediane,
        QUANTILE_CONT(total_sieges, 0.25) as p25,
        QUANTILE_CONT(total_sieges, 0.75) as p75,
        QUANTILE_CONT(total_sieges, 0.90) as p90,
        QUANTILE_CONT(total_sieges, 0.99) as p99
    FROM unique_communes
    WHERE total_sieges > 0;
    """
    
    r = con.execute(sql, params).fetchone()
    
    return StatistiquesSieges(
        nom_lieu=label,
        type_lieu=type_lieu,
        moyenne=round(r[0], 2),
        mediane=r[1],
        p25=r[2],
        p75=r[3],
        p90=r[4],
        p99=r[5]
    )


# --- NIVEAU NATIONAL ---
@app.get("/france/politique", response_model=PoliticStats)
async def get_france_politics():
    return await get_political_metrics(label="France Entière", type_lieu="national")

# --- NIVEAU REGIONAL ---
@app.get("/regions/{nom}/politique", response_model=PoliticStats)
async def get_region_politics(nom: str):
    depts = DEPARTEMENTS_PAR_REGION.get(nom)
    if not depts: raise HTTPException(status_code=404, detail="Région inconnue")
    return await get_political_metrics("Libellé département", depts, label=nom, type_lieu="région")

# --- NIVEAU DEPARTEMENTAL ---
@app.get("/departements/{nom}/politique", response_model=PoliticStats)
async def get_dept_politics(nom: str):
    return await get_political_metrics("Libellé département", nom, label=nom, type_lieu="département")

@app.get("/france/victoires", response_model=VictoiresTerritoire)
async def get_france_victoires():
    return await get_winning_communes_stats(label="France Entière", type_lieu="national")

@app.get("/regions/{nom}/victoires", response_model=VictoiresTerritoire)
async def get_region_victoires(nom: str):
    depts = DEPARTEMENTS_PAR_REGION.get(nom)
    if not depts: raise HTTPException(status_code=404, detail="Région inconnue")
    return await get_winning_communes_stats("Libellé département", depts, label=nom, type_lieu="région")

@app.get("/departements/{nom}/victoires", response_model=VictoiresTerritoire)
async def get_dept_victoires(nom: str):
    return await get_winning_communes_stats("Libellé département", nom, label=nom, type_lieu="département")

@app.get("/france/sieges", response_model=RapportSiegesTerritoire)
async def get_france_seats():
    return await get_seats_distribution_stats(label="France Entière", type_lieu="national")

@app.get("/regions/{nom}/sieges", response_model=RapportSiegesTerritoire)
async def get_region_seats(nom: str):
    depts = DEPARTEMENTS_PAR_REGION.get(nom)
    if not depts: raise HTTPException(status_code=404, detail="Région inconnue")
    return await get_seats_distribution_stats("Libellé département", depts, label=nom, type_lieu="région")

@app.get("/departements/{nom}/sieges", response_model=RapportSiegesTerritoire)
async def get_dept_seats(nom: str):
    return await get_seats_distribution_stats("Libellé département", nom, label=nom, type_lieu="département")

@app.get("/france/stats-sieges", response_model=StatistiquesSieges)
async def get_france_seats_stats():
    return await get_seats_distribution()

@app.get("/regions/{nom}/stats-sieges", response_model=StatistiquesSieges)
async def get_region_seats_stats(nom: str):
    depts = DEPARTEMENTS_PAR_REGION.get(nom)
    if not depts: raise HTTPException(status_code=404, detail="Région inconnue")
    return await get_seats_distribution("Libellé département", depts, label=nom, type_lieu="région")

@app.get("/departements/{nom}/stats-sieges", response_model=StatistiquesSieges)
async def get_dept_seats_stats(nom: str):
    return await get_seats_distribution("Libellé département", nom, label=nom, type_lieu="département")

@app.get("/france/stats-demog", response_model=DemographicStats)
async def get_france_demog_stats():
    return await get_demographic_stats()

@app.get("/regions/{nom}/stats-demog", response_model=DemographicStats)
async def get_region_demog_stats(nom: str):
    depts = DEPARTEMENTS_PAR_REGION.get(nom)
    if not depts: raise HTTPException(status_code=404, detail="Région inconnue")
    return await get_demographic_stats("Libellé département", depts, label=nom, type_lieu="région")

@app.get("/departements/{nom}/stats-demog", response_model=DemographicStats)
async def get_dept_demog_stats(nom: str):
    return await get_demographic_stats("Libellé département", nom, label=nom, type_lieu="département")