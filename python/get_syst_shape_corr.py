import pandas as pd
import sys

df_ds_ref = pd.read_csv("ds_noCorr.csv")

masses = [1.0,1.25,1.5,1.8]
ctaus = [10.0,100.0,1000.0]
tags = ["muDsPtCorr","muDsIPSCorr","muHnlIPSCorr","muHnlPtCorr","idsfup","idsfdown","recosfup","recosfdown"]


for tag in tags:
    
    df_ds_corr = pd.read_csv("sel_eff/ds_{tag}.csv".format(tag=tag))

    for mass in masses:
        for ctau in ctaus:

            df_hnl_ref  = pd.read_csv("sel_eff/hnl_mN{mass}_ctau{ctau}_noCorr.csv".format(mass=mass,ctau=ctau))
            df_hnl_corr = pd.read_csv("sel_eff/hnl_mN{mass}_ctau{ctau}_{tag}.csv".format(mass=mass,ctau=ctau,tag=tag))

            eff_ds_ref  = df_ds_ref.iloc[0]["eff"]
            eff_ds_corr = df_ds_corr.iloc[0]["eff"]
            
            df = pd.DataFrame()
            df["syst"] = abs((df_hnl_corr["eff"]/eff_ds_corr)-(df_hnl_ref["eff"]/eff_ds_ref))/(df_hnl_ref["eff"]/eff_ds_ref)
            df["m"] = df_hnl_corr["m"]
            df["ctau"] = df_hnl_corr["ctau"]
            df["cat"] = df_hnl_corr["cat"]
            df["tag"] = df_hnl_corr["tag"]
            df = df.fillna(0)
            
            df.to_csv('hnl_syst/dataMC_shape_syst_mN{mass}_ctau{ctau}_{tag}.csv'.format(mass=mass,ctau=ctau,tag=tag))
            df["ctau"] = df_hnl_ref["ctau"]




