import pandas as pd
import csv
import os
import json
from src.Tables import Tables
import datetime 

# TODO :
# - gérer les retraits de commmandes
# - refactor 
# - ajouter des contrôles
# - Test scalable
# - historisation

def load_data(data_config,target_table):
    final_data = target_table.data
    for source in data_config["sources"]:   

        table_object = source["TABLES"][target_table.table_name]

        file_name = str(table_object["NOM_FICHIER"])
        file_website_name = source["NOM_SITE"]
        file_delimiter = table_object["DELIMITER"]

        # Problème de colonne avec l'index 
        df_raw = pd.read_csv(f"{os.getcwd()}/src/data/RAW/{file_website_name}/{file_name}.csv",delimiter=file_delimiter)


        if source["NOM_SITE"] == "LYONS-EVANS":         
            if "Unnamed: 0" in list(df_raw.columns):
                df_raw.drop(["Unnamed: 0"], axis=1, inplace=True)
        
        
        # Même nom de colonnes pour différentes sources
        df_raw.rename(columns=dict([(table_object["COLONNES"][column],column) for column in table_object["COLONNES"]]),inplace=True)

        if target_table.table_name ==  "DETAIL_COMMANDE":
            # nettoyage lignes quantite
            temp =  df_raw['QUANTITE']
            df_raw['QUANTITE'] = df_raw['QUANTITE'].astype(int)
            df_raw = df_raw[df_raw['QUANTITE'] == temp]

        if target_table.table_name ==  "DETAIL_COMMANDE" or target_table.table_name ==  "PRODUIT":
            # set le type de la colonne EAN
            df_raw['EAN'] = df_raw['EAN'].astype(str)      
            # Ajoute le nom du site
            df_raw["NOM_SOURCE"] = source["NOM_SITE"]

        final_data = pd.concat([final_data,df_raw])
    target_table.data = final_data


if __name__ == "__main__":

   

    with open('conf/data_config.json') as file_config:
        data_config_dump = file_config.read()

    data_config = json.loads(data_config_dump)


    tables = {}

    for table in data_config["sources"][0]["TABLES"]:
        tables.update({table : Tables(data_config["sources"][0]["TABLES"][table]["COLONNES"].keys(),table)})
    
    load_data(data_config,tables["PRODUIT"])
    load_data(data_config,tables["DETAIL_COMMANDE"])
    load_data(data_config,tables["COMMANDE"])

    df_produit = tables["PRODUIT"].data
    df_commande = tables["COMMANDE"].data
    df_detail_commande = tables["DETAIL_COMMANDE"].data

    df_result = df_produit.filter(["EAN", "PRIX_UNITE","NOM_SOURCE"])

    volume_ventes = pd.DataFrame(["EAN", "NOM_SOURCE","MOIS_VENTE"])

    #Ajout de la date de commande
    minimum_date = df_commande["DATE_COMMANDE"].min()
    maximum_date = df_commande["DATE_COMMANDE"].max()
    month_series = pd.DataFrame(pd.Series(pd.period_range(minimum_date,maximum_date, freq="M")))

    month_series["tmp_key"] = 0
    df_result["tmp_key"] = 0

    df_result = pd.merge(df_result,month_series, on='tmp_key', how='outer')
    df_result.rename(columns={0:"MOIS_VENTES"},inplace=True)
    df_result.drop('tmp_key',axis = 1,inplace = True)
    del month_series



    df_detail_date_commande = pd.merge(df_detail_commande, df_commande,  how='inner', on=["ID_COMMANDE"])
    del df_detail_commande
    del df_commande

    df_detail_date_commande["MOIS_VENTES"] =  pd.to_datetime(df_detail_date_commande['DATE_COMMANDE']).dt.to_period('M')


    #Ajout du volume de ventes
    volume_ventes = df_detail_date_commande.groupby(["EAN", "NOM_SOURCE","MOIS_VENTES"])["QUANTITE"].sum()
    volume_ventes = pd.DataFrame(volume_ventes).rename(columns={"QUANTITE":"VOLUME_VENTES"})

   

    df_result = pd.merge(df_result, volume_ventes,  how='left', on=["EAN", "NOM_SOURCE","MOIS_VENTES"])
    df_result['VOLUME_VENTES'] = df_result['VOLUME_VENTES'].fillna(0)
    
    df_result.to_csv(f"{os.getcwd()}/src/data\PROCESSED\export_{datetime.date.today()}.csv")
 

    








