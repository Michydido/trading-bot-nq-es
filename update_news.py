#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script per generare un file news.csv con le principali news economiche USA
basate su regole ricorrenti (CPI, FOMC, NFP, PPI, Unemployment Claims).
Genera eventi per i prossimi N mesi (default 6).
"""

import csv
from datetime import datetime, timedelta, date
import argparse

def nth_weekday(year, month, nth, weekday):
    """
    Restituisce la data dell'n-esimo giorno della settimana (0=lunedì, 6=domenica)
    in un dato mese. Ad esempio, primo venerdì del mese: nth=1, weekday=4.
    """
    first_day = date(year, month, 1)
    # Trova il primo giorno della settimana desiderata
    first_weekday = first_day.weekday()
    offset = (weekday - first_weekday) % 7
    first_occurrence = first_day + timedelta(days=offset)
    # Aggiungi (nth-1) settimane
    return first_occurrence + timedelta(weeks=nth-1)

def generate_events(months=6, start_date=None):
    """
    Genera una lista di eventi economici per i prossimi 'months' mesi.
    Se start_date è None, parte dal mese corrente.
    Restituisce una lista di dizionari con chiavi: date, event, importance, is_non_news.
    """
    if start_date is None:
        start_date = date.today()
    
    events = []
    current_year = start_date.year
    current_month = start_date.month
    
    for i in range(months):
        year = current_year
        month = current_month + i
        while month > 12:
            month -= 12
            year += 1
        
        # CPI: di solito intorno al 10-15 del mese (mettiamo il 10 come placeholder)
        cpi_date = date(year, month, 10)
        events.append({
            'date': cpi_date.isoformat(),
            'event': 'CPI',
            'importance': 'high',
            'is_non_news': 0
        })
        
        # FOMC: circa ogni 6 settimane, ma per semplicità mettiamo il terzo mercoledì del mese
        # (alcuni mesi non hanno FOMC, ma va bene)
        fomc_date = nth_weekday(year, month, 3, 2)  # terzo mercoledì
        events.append({
            'date': fomc_date.isoformat(),
            'event': 'FOMC',
            'importance': 'high',
            'is_non_news': 0
        })
        
        # NFP: primo venerdì del mese
        nfp_date = nth_weekday(year, month, 1, 4)  # primo venerdì
        events.append({
            'date': nfp_date.isoformat(),
            'event': 'Non Farm Payrolls',
            'importance': 'high',
            'is_non_news': 0
        })
        
        # PPI: di solito dopo CPI, mettiamo il 12
        ppi_date = date(year, month, 12)
        events.append({
            'date': ppi_date.isoformat(),
            'event': 'PPI',
            'importance': 'medium',
            'is_non_news': 0
        })
        
        # Unemployment Claims: ogni giovedì (considerate non-news)
        # Aggiungiamo solo alcuni giovedì a caso per avere eventi non-news
        for week in range(1, 5):
            uc_date = nth_weekday(year, month, week, 3)  # giovedì (weekday 3)
            if uc_date.month == month:  # assicuriamoci che sia nello stesso mese
                events.append({
                    'date': uc_date.isoformat(),
                    'event': 'Unemployment Claims',
                    'importance': 'medium',
                    'is_non_news': 1
                })
    
    # Rimuovi eventuali duplicati (se due eventi cadono nello stesso giorno)
    unique = {}
    for ev in events:
        key = (ev['date'], ev['event'])
        if key not in unique:
            unique[key] = ev
    
    # Ordina per data
    sorted_events = sorted(unique.values(), key=lambda x: x['date'])
    return sorted_events

def save_csv(events, filename='news.csv'):
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['date', 'event', 'importance', 'is_non_news'])
        writer.writeheader()
        writer.writerows(events)
    print(f"✅ Salvati {len(events)} eventi in {filename}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Genera file news.csv con le principali news USA.')
    parser.add_argument('--months', type=int, default=6, help='Numero di mesi da generare (default: 6)')
    parser.add_argument('--output', type=str, default='news.csv', help='Nome file output (default: news.csv)')
    args = parser.parse_args()
    
    events = generate_events(months=args.months)
    save_csv(events, args.output)