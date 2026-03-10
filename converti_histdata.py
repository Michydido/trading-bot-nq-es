import pandas as pd
import os

# Nomi dei file originali (modifica se necessario)
file_nq = 'DAT_MT_NSXUSD_M1_202602.csv'
file_es = 'DAT_MT_SPXUSD_M1_202602.csv'
output_nq = 'NQ_1m.csv'
output_es = 'ES_1m.csv'

def converti_file(input_file, output_file):
    if not os.path.exists(input_file):
        print(f"❌ File {input_file} non trovato.")
        return False
    try:
        # Legge il CSV senza intestazione (sep=',', nessuna intestazione)
        df = pd.read_csv(input_file, sep=',', header=None,
                         names=['date', 'time', 'open', 'high', 'low', 'close', 'volume'])
        
        # Combina data e ora e converte in datetime
        df['datetime_str'] = df['date'] + ' ' + df['time']
        df['time'] = pd.to_datetime(df['datetime_str'], format='%Y.%m.%d %H:%M')
        
        # Seleziona colonne nel formato richiesto
        df = df[['time', 'open', 'high', 'low', 'close', 'volume']]
        
        # Salva
        df.to_csv(output_file, index=False)
        print(f"✅ {output_file} creato con {len(df)} righe")
        return True
    except Exception as e:
        print(f"❌ Errore durante la conversione di {input_file}: {e}")
        return False

print("🔄 Conversione file histdata (formato MetaTrader)...")
converti_file(file_nq, output_nq)
converti_file(file_es, output_es)
print("\n🎉 Conversione completata! Ora puoi lanciare il backtest.")