from dotenv import load_dotenv
from supabase import create_client
import os

def test_supabase_connection():
    try:
        # Charger les variables d'environnement
        load_dotenv()
        
        # Récupérer les informations de connexion
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_KEY")
        
        if not url or not key:
            raise ValueError("Variables d'environnement SUPABASE_URL ou SUPABASE_KEY manquantes")
            
        # Tenter de créer une connexion
        supabase = create_client(url, key)
        
        # Tester une requête simple
        response = supabase.table('opportunities').select("*").limit(1).execute()
        
        print("✅ Connexion à Supabase réussie !")
        return True
        
    except Exception as e:
        print(f"❌ Erreur de connexion à Supabase : {str(e)}")
        return False

if __name__ == "__main__":
    test_supabase_connection()