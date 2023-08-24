import sys
import ROOT
import os
import subprocess
import hnl_tools
import argparse
import json
import pandas as pd

parser = argparse.ArgumentParser(description="")
parser.add_argument("hnl_mass", help="Mass of the HNL [GeV]")
parser.add_argument("hnl_ctau", help="ctau of the HNL [mm]")
parser.add_argument("--nansToZeros",action='store_true', default=False, help="Replace nans with 0.0s")
args = parser.parse_args()

#input parameters
m      = float(args.hnl_mass)
ct     = float(args.hnl_ctau)

#categories = ["inclusive"]
categories = ["OSlxy0to1","OSlxy1to5","OSlxy5toInf","SSlxy0to1","SSlxy1to5","SSlxy5toInf"]
corr_weights = ["C_ds_pt_shape_sf","C_hnl_pt_shape_sf","C_ds_ips_shape_sf","C_hnl_ips_shape_sf","C_mu_reco_sf_up","C_mu_reco_sf_down","C_mu_id_sf_up","C_mu_id_sf_down","noweight"]
labels = {"C_ds_pt_shape_sf":"muDsPtCorr","C_hnl_pt_shape_sf":"muHnlPtCorr","C_ds_ips_shape_sf":"muDsIPSCorr","C_hnl_ips_shape_sf":"muHnlIPSCorr","C_mu_reco_sf_up":"recosfup","C_mu_reco_sf_down":"recosfdown","C_mu_id_sf_up":"idsfup","C_mu_id_sf_down":"idsfdown","noweight":"noCorr"}

dsToHnlMu_ntuples_cfg_path = "/home/CMS-T3/lunerti/hnlTreeAnalyzer/cfg/DsToHnlMu_HnlToMuPi_prompt_UL_tree_input_fromCrab.json"
dsToPhiPi_ntuples_cfg_path = "/home/CMS-T3/lunerti/hnlTreeAnalyzer/cfg/DsToPhiPi_PhiToMuMu_prompt_UL_tree_input_fromCrab.json"

with open(dsToHnlMu_ntuples_cfg_path, "r") as f:
    dsToHnlMu_ntuples = json.loads(f.read())

with open(dsToPhiPi_ntuples_cfg_path, "r") as f:
    dsToPhiPi_ntuples = json.loads(f.read())

for w in corr_weights:
    tag = labels[w]

    out_hnl_mass = list()
    out_hnl_ctau = list()
    out_hnl_ntot = list()
    out_hnl_nsel = list()
    out_hnl_eff  = list()
    out_hnl_cat  = list()
    out_hnl_tag  = list() 
    
    out_ds_ntot = list()
    out_ds_nsel = list()
    out_ds_eff  = list()
    out_ds_cat  = list()
    out_ds_tag  = list() 
    
    
    for cat in categories:
        
        sig_short_name = "DsToNMu_NToMuPi_mN{mass}_ctau{ctau}mm_incl".format(mass=str(m).replace(".","p"),ctau=str(ct).replace(".","p"))
       
        input_DsPhiPi_ntuple_path = "/gpfs_data/local/cms/lunerti/dsphipi_ntuples/output_tree/PhiToMuMu_prompt_DsToPhiPi_ToMuMu_MuFilter_TuneCP5_13TeV-pythia8-evtgen_tree/tree_PhiToMuMu_prompt_DsToPhiPi_ToMuMu_MuFilter_TuneCP5_13TeV-pythia8-evtgen_tree.csv"
        input_DsHnlMu_ntuple_path = "/gpfs_data/local/cms/lunerti/hnl_ntuples/output_tree/HnlToMuPi_prompt_DsToNMu_NToMuPi_SoftQCDnonD_noQuarkFilter_mN{mass}_ctau{ctau}mm_TuneCP5_13TeV-pythia8-evtgen_tree/tree_HnlToMuPi_prompt_DsToNMu_NToMuPi_SoftQCDnonD_noQuarkFilter_mN{mass}_ctau{ctau}mm_TuneCP5_13TeV-pythia8-evtgen_tree_{cat}.csv".format(cat=cat,mass=str(m).replace(".","p"),ctau=str(ct).replace(".","p"))
        
        print("Ds->PhiPi input: {}".format(input_DsPhiPi_ntuple_path))
        print("Ds->HNL input: {}".format(input_DsHnlMu_ntuple_path))
    
        #sometimes values are set to nan in csv and then they get skipped
        #to temporarily prevent this I set all 'nan' values to '0.0' so that
        #they don't get skipped
        if args.nansToZeros:
            for fn in [input_DsPhiPi_ntuple_path,input_DsHnlMu_ntuple_path]:
                command = "sed -i 's/nan/0.0/g' {}".format(fn)
                print("Running {}".format(command))
                subprocess.call(command,shell=True)
    
        n_DsToPhiPi_gen = float(dsToPhiPi_ntuples["DsToPhiPi_ToMuMu"]["processed_events"])
        n_DsToHnlMu_gen = float(dsToHnlMu_ntuples[sig_short_name]["processed_events"])

        if w != "noweight":
            n_DsToPhiPi_sel = float(hnl_tools.get_weighted_yield_from_csv(input_DsPhiPi_ntuple_path,"tot_weight",w))
            n_DsToHnlMu_sel = float(hnl_tools.get_weighted_yield_from_csv(input_DsHnlMu_ntuple_path,"tot_weight",w))
        else:
            n_DsToPhiPi_sel = float(hnl_tools.get_weighted_yield_from_csv(input_DsPhiPi_ntuple_path,"tot_weight"))
            n_DsToHnlMu_sel = float(hnl_tools.get_weighted_yield_from_csv(input_DsHnlMu_ntuple_path,"tot_weight"))
        
        eff_Ds   = n_DsToPhiPi_sel/n_DsToPhiPi_gen
        eff_Hnl  = n_DsToHnlMu_sel/n_DsToHnlMu_gen
    
        out_hnl_mass.append(float(args.hnl_mass))
        out_hnl_ctau.append(float(args.hnl_ctau))
        out_hnl_ntot.append(n_DsToHnlMu_gen)
        out_hnl_nsel.append(n_DsToHnlMu_sel)
        out_hnl_eff.append(eff_Hnl)
        out_hnl_cat.append(cat)
        out_hnl_tag.append(tag) 
    
        out_ds_ntot.append(n_DsToPhiPi_gen)
        out_ds_nsel.append(n_DsToPhiPi_sel)
        out_ds_eff.append(eff_Ds)
        out_ds_tag.append(tag) 
        
        
        print("*********CATEGORY*************")
        print("{}".format(cat))
        print("*******INPUT*PARAMETERS*******")
        print("input mass: {} [GeV]".format(m))
        print("input ctau: {} [mm]".format(ct))
        print("eff_Ds: {}/{}={}".format(n_DsToPhiPi_sel,n_DsToPhiPi_gen,eff_Ds))
        print("eff_Hnl: {}/{}={}".format(n_DsToHnlMu_sel,n_DsToHnlMu_gen,eff_Hnl))
        print("******************************")
        print('\n')
    
    out_hnl_dict = dict()
    out_hnl_dict["m"] = out_hnl_mass
    out_hnl_dict["ctau"] = out_hnl_ctau
    out_hnl_dict["ntot"] = out_hnl_ntot
    out_hnl_dict["nsel"] = out_hnl_nsel
    out_hnl_dict["eff"] = out_hnl_eff 
    out_hnl_dict["cat"] = out_hnl_cat 
    out_hnl_dict["tag"] = out_hnl_tag 
    
    out_ds_dict = dict()
    out_ds_dict["ntot"] = out_ds_ntot
    out_ds_dict["nsel"] = out_ds_nsel
    out_ds_dict["eff"] = out_ds_eff 
    out_ds_dict["tag"] = out_ds_tag 
    
    df_hnl = pd.DataFrame(out_hnl_dict)
    df_ds  = pd.DataFrame(out_ds_dict)
    
    out_tag = tag
    
    subprocess.call("mkdir -p sel_eff",shell=True)
    df_hnl.to_csv('sel_eff/hnl_mN{}_ctau{}_{}.csv'.format(m,ct,out_tag),index=False) 
    df_ds.to_csv('sel_eff/ds_{}.csv'.format(out_tag),index=False) 
