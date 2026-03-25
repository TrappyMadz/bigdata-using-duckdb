from pydantic import BaseModel, Field
from typing import List

class ResultatCandidat(BaseModel):
    """Détails des voix pour un candidat spécifique"""
    nom: str = Field(..., example="Jean Dupont")
    voix: int = Field(..., example=1250)
    pourcentage_exprimes: float = Field(..., example=15.4)
    pourcentage_inscrits: float = Field(..., example=12.1)

class StatsTour(BaseModel):
    """Statistiques globales et liste des candidats pour un tour donné"""
    num_tour: int = Field(..., example=1)
    inscrits: int = Field(..., example=10000)
    votants: int = Field(..., example=7500)
    exprimes: int = Field(..., example=7200)
    candidats: List[ResultatCandidat]

class LieuResultats(BaseModel):
    nom_lieu: str
    type_lieu: str  # ex: "commune", "département"
    tours: List[StatsTour]

class ParticipationStats(BaseModel):
    tour: int
    inscrits: int
    votants: int
    abstentions: int
    taux_participation: float
    taux_abstention: float

class FranceParticipation(BaseModel):
    pays: str = "France Entière"
    details: List[ParticipationStats]

class NuanceSeats(BaseModel):
    nuance: str
    sieges_total: int
    sieges_majorite: int
    sieges_opposition: int

class NuanceMajorite(BaseModel):
    nuance: str
    nb_communes_gagnees: int

class PoliticStats(BaseModel):
    nom_lieu: str
    type_lieu: str
    tour: int
    stats_sieges: List[NuanceSeats]
    stats_victoires: List[NuanceMajorite]

class NuanceVictoires(BaseModel):
    nuance: str
    nb_communes: int
    pourcentage: float # % de communes gagnées sur le total du territoire

class VictoiresTerritoire(BaseModel):
    nom_lieu: str
    type_lieu: str
    total_communes_traitees: int
    resultats: List[NuanceVictoires]

class ProportionSieges(BaseModel):
    nuance: str
    sieges_majorite: int
    sieges_opposition: int
    total_sieges: int
    ratio_force: float # % de sièges détenus par la nuance sur le territoire

class RapportSiegesTerritoire(BaseModel):
    nom_lieu: str
    type_lieu: str
    total_sieges_territoire: int
    repartition: List[ProportionSieges]

class StatistiquesSieges(BaseModel):
    nom_lieu: str
    type_lieu: str
    moyenne: float
    mediane: float
    p25: float  # 25% (1er quartile)
    p75: float  # 75% (3ème quartile)
    p90: float  # 90ème percentile
    p99: float  # 99ème percentile (les très grandes villes)

class StatSet(BaseModel):
    """Ensemble de statistiques descriptives pour une métrique sans dépendre d'un lieu"""
    moyenne: float
    mediane: float
    p25: float
    p75: float
    p90: float
    p99: float

class DemographicStats(BaseModel):
    nom_lieu: str
    type_lieu: str
    inscrits: StatSet
    votants: StatSet

DEPARTEMENTS_PAR_REGION = {
    "Auvergne-Rhône-Alpes": [
        "Ain", "Allier", "Ardèche", "Cantal", "Drôme", "Isère", 
        "Loire", "Haute-Loire", "Puy-de-Dôme", "Rhône", "Savoie", "Haute-Savoie"
    ],
    "Bourgogne-Franche-Comté": [
        "Côte-d'Or", "Doubs", "Jura", "Nièvre", "Haute-Saône", 
        "Saône-et-Loire", "Yonne", "Territoire de Belfort"
    ],
    "Bretagne": [
        "Côtes-d'Armor", "Finistère", "Ille-et-Vilaine", "Morbihan"
    ],
    "Centre-Val de Loire": [
        "Cher", "Eure-et-Loir", "Indre", "Indre-et-Loire", "Loir-et-Cher", "Loiret"
    ],
    "Corse": [
        "Corse-du-Sud", "Haute-Corse"
    ],
    "Grand Est": [
        "Ardennes", "Aube", "Marne", "Haute-Marne", "Meurthe-et-Moselle", 
        "Meuse", "Moselle", "Bas-Rhin", "Haut-Rhin", "Vosges"
    ],
    "Hauts-de-France": [
        "Aisne", "Nord", "Oise", "Pas-de-Calais", "Somme"
    ],
    "Île-de-France": [
        "Paris", "Seine-et-Marne", "Yvelines", "Essonne", "Hauts-de-Seine", 
        "Seine-Saint-Denis", "Val-de-Marne", "Val-d'Oise"
    ],
    "Normandie": [
        "Calvados", "Eure", "Manche", "Orne", "Seine-Maritime"
    ],
    "Nouvelle-Aquitaine": [
        "Charente", "Charente-Maritime", "Corrèze", "Creuse", "Dordogne", 
        "Gironde", "Landes", "Lot-et-Garonne", "Pyrénées-Atlantiques", 
        "Deux-Sèvres", "Vienne", "Haute-Vienne"
    ],
    "Occitanie": [
        "Ariège", "Aude", "Aveyron", "Gard", "Haute-Garonne", "Gers", 
        "Hérault", "Lot", "Lozère", "Hautes-Pyrénées", "Pyrénées-Orientales", 
        "Tarn", "Tarn-et-Garonne"
    ],
    "Pays de la Loire": [
        "Loire-Atlantique", "Maine-et-Loire", "Mayenne", "Sarthe", "Vendée"
    ],
    "Provence-Alpes-Côte d'Azur": [
        "Alpes-de-Haute-Provence", "Hautes-Alpes", "Alpes-Maritimes", 
        "Bouches-du-Rhône", "Var", "Vaucluse"
    ],
    "Guadeloupe": ["Guadeloupe"],
    "Martinique": ["Martinique"],
    "Guyane": ["Guyane"],
    "La Réunion": ["La Réunion"],
    "Mayotte": ["Mayotte"]
}