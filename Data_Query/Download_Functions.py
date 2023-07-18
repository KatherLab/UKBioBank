#general file for UKBioBank download
import sqlite3 as sql
import pandas as pd
import subprocess
import os

#%%
######################################################################
def download_with_table(tbl_path= None, bulkify_path=None, ID_col=None, db_file=None, output="results"):
    assert tbl_path != None, "Path to clinitable not given!"
    assert bulkify_path != None, "Path to bulkify.sh not given!"
    assert ID_col != None, " ID column in table not given!"
    assert db_file != None, " path to ukdatabase not given!"


    IDs= pd.read_csv(tbl_path)[ID_col]
    out_pth= os.path.join(os.path.dirname(tbl_path),output)
    field = assign_field_id()
    #Create a connection to the database
    conn= sql.connect(db_file)
    cur= conn.cursor()
    query= '''SELECT {0}.eid, {0}.instance_index, {0}.array_index
            FROM {0}'''
    query = query.format('''"'''*str(field)+'''"''')
    a= pd.DataFrame(cur.execute().fetchall())
    a.columns= ["ID","Instance","Array"]
    a = a.loc[a["ID"] in IDs] #Get the IDs that we want for the experiment
    a= a.assign(NewID=a.ID.apply(str)+" "+str(field)+"_" + a.Instance.apply(str) + "_" + a.Array.apply(str))
    bulk=a["NewID"]
    #After obtaining the bulk variables we create the .bulk file and then use that to launch the bulkify script with the ukbfetch 
    
    field=str(field)
    subprocess.call(["./bulkify_ind.sh" , field, param2])
    return("Download completed! :)")

def assign_field_id():
    organ= input("What organ do you want? Available ones are brain, heart and liver")
    if organ not in ["heart", "brain","liver"]:
        organ= input("What organ do you want? Available ones are brain, heart and liver")
    
    if organ == "brain":
        modality= input("For brain, available modalities are T1, T2, ASL, tfMRI, dMRI, swMRI, rfMRI")
        if modality not in ["T1","T2","ASL","tfMRI","dMRI","swMRI","rfMRI"]:
                modality= input("For brain, available modalities are T1, T2, ASL, tfMRI, dMRI, swMRI, rfMRI")
        mod= {"T1":20252,
              "T2":20253,
              "ASL":26300,
              "tfMRI":20249,
              "dMRI":20250,
              "rfMRI":20227,
              "swMRI":20251}
        field_id= mod[modality]

    if organ == "heart":
        modality= input("For heart, available modalities are AorticDistensibility, BloodFlow, T1shmolli, LeftVentricle, LongAxis, Scout, ShortAxis, CineTagging")
        if modality not in ["AorticDistensibility", "BloodFlow", "T1shmolli", "LeftVentricle", "LongAxis", "Scout", "ShortAxis", "CineTagging"]:
                modality= input("For heart, available modalities are AorticDistensibility, BloodFlow, T1shmolli, LeftVentricle, LongAxis, Scout, ShortAxis, CineTagging")
        
        mod= {"AorticDistensibility":20210, 
              "BloodFlow":20213, 
              "T1shmolli":20214, 
              "LeftVentricle":20212, 
              "LongAxis":20208, 
              "Scout":20207, 
              "ShortAxis":20209, 
              "CineTagging":20211}
        
        field_id= mod[modality]

    if organ == "liver":
        modality= input("For liver, available modalities are T1shmolli, IdealT1, GradientT1")
        if modality not in ["T1shmolli","IdealT1","GradientT1"]:
                modality= input("For liver, available modalities are T1shmolli, IdealT1, GradientT1")
        mod= {"T1shmolli":20204,
              "IdealT1":20254,
              "GradientT1":20203
              }
        field_id= mod[modality]
    #After getting the field id, we can query the database to create the bulkify file
    return field_id
    
######################################################################