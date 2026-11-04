# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "fccbab2d-4470-4913-bfe2-969b7c966c01",
# META       "default_lakehouse_name": "lh_prova",
# META       "default_lakehouse_workspace_id": "9d151055-5883-45ed-970b-7b6628cf14ce",
# META       "known_lakehouses": [
# META         {
# META           "id": "fccbab2d-4470-4913-bfe2-969b7c966c01"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

print("1. Lettura dati Silver (Locali e Virtualizzati)...")
# Lettura diretta tramite il metastore di Spark (evita problemi di percorsi relativi)
df_ticket = spark.table("silver_ticket")

# Fabric mappa automaticamente la cartella dello shortcut 'dbo_1' come schema database
df_clienti = spark.table("dbo_1.silver_clienti")

print("2. Creazione Modello Gold (Incrocio Dati)...")
# Eseguiamo un Left Join per arricchire i ticket con le informazioni dei clienti.
# La funzione unisce automaticamente tutte le colonne dell'anagrafica al ticket.
df_gold = df_ticket.join(df_clienti, "ID_Cliente", "left")

print("3. Salvataggio in Zona Gold...")
df_gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_ticket_analytics")

print("--- TABELLA GOLD CREATA CON SUCCESSO ---")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
