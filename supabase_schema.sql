# Format JSON original des opportunités d'arbitrage

{
    "group_id": "string",          # ID unique du groupe d'opportunités
    "home_team": "string",         # Équipe à domicile
    "away_team": "string",         # Équipe à l'extérieur
    "date": "string",             # Format: DD/MM/YYYY
    "time": "string",             # Format: HH:MM
    "country": "string",          # Pays du match
    "all_sources": [              # Liste des bookmakers
        "string"
    ],
    "opportunities": [            # Liste des opportunités d'arbitrage
        {
            "complementary_set": "string",    # Type de pari
            "best_odds": {                    # Meilleures cotes
                "odd_type": {
                    "value": "number",
                    "source": "string"
                }
            },
            "arbitrage_percentage": "number",   # Pourcentage d'arbitrage
            "arbitrage_sources": "string",      # Sources utilisées
            "unique_id": "string",              # ID unique de l'opportunité
            "misvalue_source": "string",        # Source de la mauvaise cote
            "group_id": "string",               # ID du groupe
            "home_team": "string",              # Équipe à domicile
            "away_team": "string",              # Équipe à l'extérieur
            "activity_duration": "string"        # Durée de l'activité
        }
    ]
}

# Notes:
# - Les données sont conservées exactement dans le format original
# - L'ID unique est le group_id existant
# - Les fichiers d'activity tracker restent uniquement en local
# - Les doublons sont évités grâce au group_id unique
# - Seul un timestamp created_at est ajouté pour Supabase