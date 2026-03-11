#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script per scaricare il calendario news reale da Financial Modeling Prep (FMP).
Eseguito automaticamente da GitHub Actions ogni giorno.
"""

import os
import csv
import requests
from datetime import datetime, timedelta

API_KEY = os.getenv('FMP_API_KEY')

if not API_KEY:
    print("❌ ERRORE: Variabile d'ambiente FMP_API_KEY non trovata.")
    exit(1)

def fetch_and_save_news(filename='news.csv', days=30):
    """Scarica il calendario economico degli USA per i prossimi 'days' giorni."""
    try:
        # Costruisce l'URL per l'API del calendario economico di FMP
        url = f"https://financialmodelingprep.com/api/v3/economic_calendar"
        params = {
            'apikey': API_KEY,
            'from': datetime.now().strftime('%Y-%m-%d'),
            'to': (datetime.now() + timedelta(days=days)).strftime('%Y-%m-%d')
        }
        
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        if not isinstance(data, list):
            print(f"❌ Risposta API non valida: {data}")
            return False
        
        events_list = []
        for event in data:
            # Verifica che sia per gli Stati Uniti
            if event.get('country') != 'US':
                continue
            
            event_date_str = event.get('date')
            if not event_date_str:
                continue
            
            try:
                event_date = datetime.fromisoformat(event_date_str.replace('Z', '+00:00'))
            except:
                continue
            
            importance_map = {'High': 'high', 'Medium': 'medium', 'Low': 'low'}
            importance = importance_map.get(event.get('impact', 'Low'), 'low')
            title = event.get('event', '')
            is_non_news = 1 if ('Unemployment' in title or 'Jobless' in title) else 0
            
            events_list.append({
                'date': event_date.strftime('%Y-%m-%d'),
                'event': title,
                'importance': importance,
                'is_non_news': is_non_news
            })
        
        # Salva il file CSV
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['date', 'event', 'importance', 'is_non_news'])
            writer.writeheader()
            writer.writerows(events_list)
        
        print(f"✅ Salvati {len(events_list)} eventi in {filename}")
        return True
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Errore di connessione: {e}")
        return False
    except Exception as e:
        print(f"❌ Errore imprevisto: {e}")
        return False

if __name__ == "__main__":
    fetch_and_save_news()