# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "08c3e674-5bf6-4941-8a80-b4d3149230c2",
# META       "default_lakehouse_name": "lh_prova",
# META       "default_lakehouse_workspace_id": "ca209b34-9112-4faf-a25e-3ccea1b76680",
# META       "known_lakehouses": [
# META         {
# META           "id": "08c3e674-5bf6-4941-8a80-b4d3149230c2"
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
