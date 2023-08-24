#!/cvmfs/sft-nightlies.cern.ch/lcg/views/dev4/Tue/x86_64-centos7-gcc12-opt/bin/python

import ROOT
import sys
import math
import os
import json
import time
import subprocess
import argparse
import numpy
import hnl_tools
import pandas as pd
start_time = time.time()

#script input arguments
parser = argparse.ArgumentParser(description="")
parser.add_argument("cfg_filename"          ,                                     help="Path to the input configuration file")
parser.add_argument("dataset_short_name"    ,                                     help="Short name of the input sample to process")
parser.add_argument("--saveOutputTree"      , action='store_true', default=False, help="Save an output tree (after all the cuts)")
parser.add_argument("--savePreselectedTree" , action='store_true', default=False, help="Save an output tree (before the cuts and after best candidate selection)")
parser.add_argument("--doHistograms"        , action='store_true', default=False, help="Plot and save histograms")
parser.add_argument("--addSPlotWeight"      , action='store_true', default=False, help="Add splot weight for data")
parser.add_argument("--skipPUrw"            , action='store_true', default=False, help="Do not apply PU reweighting")
parser.add_argument("--skipTrigSF"          , action='store_true', default=False, help="Do not apply trigger scale factors")
parser.add_argument("--skipMuIDsf"          , action='store_true', default=False, help="Do not apply muon id scale factors")
parser.add_argument("--skipMuRecosf"        , action='store_true', default=False, help="Do not apply muon reco scale factors")
parser.add_argument("--nThreads"            , type=int           , default=1    , help="Number of threads")
parser.add_argument("--addTag"              , type=str           , default=""   , help="Tag output files")
parser.add_argument("--ctauReweighting"     , action='store_true', default=False, help="Include ctau reweighting")
parser.add_argument("--applyMuDsPtCorr"     , action='store_true', default=False, help="Apply reweighting to mu from Ds to correct data/MC pt discrepancies")
parser.add_argument("--applyMuHnlPtCorr"    , action='store_true', default=False, help="Apply reweighting to mu from Hnl to correct data/MC pt discrepancies")
parser.add_argument("--applyMuDsIPSCorr"    , action='store_true', default=False, help="Apply reweighting to mu from Ds to correct data/MC IPS discrepancies")
parser.add_argument("--applyMuHnlIPSCorr"   , action='store_true', default=False, help="Apply reweighting to mu from Hnl to correct data/MC IPS discrepancies")
parser.add_argument("--bestCandChecks"      , action='store_true', default=False, help="Make checks for best candidate selection")
parser.add_argument("--keep"                , nargs="*"          , default=[]   , help="Select which branches to keep in the final output tree")
args = parser.parse_args()

ROOT.EnableThreadSafety()

if args.nThreads>1:
    ROOT.EnableImplicitMT(args.nThreads)

configFileName     = args.cfg_filename
dataset_to_process = args.dataset_short_name

tag = args.addTag

#open analyzer configuration file
with open(configFileName, "r") as f:
    config = json.loads(f.read())

#get ntuples configuration
with open(config["ntuples_cfg_file_full_path"], "r") as f:
    ntuples = json.loads(f.read())
    
#get selection and categorization cuts
with open(config["selection_cfg_file_full_path"], "r") as f:
    selection = json.loads(f.read())

# let private function to be avaliable in RDataFrame evn
header_path = config["user_defined_function_path"]
ROOT.gInterpreter.Declare('#include "{}"'.format(header_path))

input_file_list = "file_name_list"
tree_name = "wztree"

#get input files
inputFileName_list = ntuples[dataset_to_process][input_file_list]
dataset_category = ntuples[dataset_to_process]["dataset_category"]

#get pu weights histogram
if dataset_category != "data" and not args.skipPUrw:
    if dataset_category=="signal":
        ROOT.gInterpreter.Declare("""
        auto pu_weights_file = TFile::Open("{}");
        auto h_pu_weights = pu_weights_file->Get<TH1D>("{}");
        """.format(config["pu_weight_input_file_signal"],config["pu_weight_histo_name"])
        )
    else:
        ROOT.gInterpreter.Declare("""
        auto pu_weights_file = TFile::Open("{}");
        auto h_pu_weights = pu_weights_file->Get<TH1D>("{}");
        """.format(config["pu_weight_input_file_bkg"],config["pu_weight_histo_name"])
        )

#get trigger scale factors histogram
if dataset_category != "data" and not args.skipTrigSF:
    print("eff data histogram from {}, {}".format(config["trigger_eff_data_input_file"],config["trigger_eff_data_histo_name"]))
    print("eff mc histogram from {}, {}".format(config["trigger_eff_mc_input_file"],config["trigger_eff_mc_histo_name"]))
    ROOT.gInterpreter.Declare("""
    auto trigger_eff_data_file = TFile::Open("{edata_file}");
    auto trigger_eff_mc_file   = TFile::Open("{emc_file}");
    auto h_trigger_eff_data = trigger_eff_data_file->Get<TH2D>("{edatahisto_name}");
    auto h_trigger_eff_mc   = trigger_eff_mc_file->Get<TH2D>("{emchisto_name}");
    """.format(edata_file=config["trigger_eff_data_input_file"],
               emc_file=config["trigger_eff_mc_input_file"],
               edatahisto_name=config["trigger_eff_data_histo_name"],
               emchisto_name=config["trigger_eff_mc_histo_name"])
    )

#get pu weights histogram
if dataset_category != "data" and (not args.skipMuIDsf or not args.skipMuRecosf):
    ROOT.gInterpreter.Declare("""
    #include <boost/property_tree/ptree.hpp>
    #include <boost/property_tree/json_parser.hpp>
    """
    )
    if not args.skipMuIDsf:
        ROOT.gInterpreter.Declare("boost::property_tree::ptree mu_id_sf_cfg;")
        ROOT.gInterpreter.ProcessLine("""
        boost::property_tree::read_json("{infile}",mu_id_sf_cfg);
        """.format(infile=config["mu_id_sf_input_file"])
        )
    if not args.skipMuRecosf:
        ROOT.gInterpreter.Declare("boost::property_tree::ptree mu_reco_sf_cfg;")
        ROOT.gInterpreter.ProcessLine("""
        boost::property_tree::read_json("{infile}",mu_reco_sf_cfg);
        """.format(infile=config["mu_reco_sf_input_file"])
        )

#get mu_Ds pt shape scale factors histogram
if dataset_category != "data" and args.applyMuDsPtCorr:
    print("pt shape correction sf from {}, {}".format(config["ds_pt_shape_sf_input_file"],config["ds_pt_shape_sf_histo_name"]))
    ROOT.gInterpreter.Declare("""
    auto ds_pt_shape_sf_file = TFile::Open("{}");
    auto h_ds_pt_shape_sf = ds_pt_shape_sf_file->Get<TH1D>("{}");
    """.format(config["ds_pt_shape_sf_input_file"],config["ds_pt_shape_sf_histo_name"])
    )

#get mu_Hnl pt shape scale factors histogram
if dataset_category != "data" and args.applyMuHnlPtCorr:
    print("pt shape correction sf from {}, {}".format(config["hnl_pt_shape_sf_input_file"],config["hnl_pt_shape_sf_histo_name"]))
    ROOT.gInterpreter.Declare("""
    auto hnl_pt_shape_sf_file = TFile::Open("{}");
    auto h_hnl_pt_shape_sf = hnl_pt_shape_sf_file->Get<TH1D>("{}");
    """.format(config["hnl_pt_shape_sf_input_file"],config["hnl_pt_shape_sf_histo_name"])
    )

#get mu_Ds IPS shape scale factors histogram
if dataset_category != "data" and args.applyMuDsIPSCorr:
    print("ips shape correction sf from {}, {}".format(config["ds_ips_shape_sf_input_file"],config["ds_ips_shape_sf_histo_name"]))
    ROOT.gInterpreter.Declare("""
    auto ds_ips_shape_sf_file = TFile::Open("{}");
    auto h_ds_ips_shape_sf = ds_ips_shape_sf_file->Get<TH1D>("{}");
    """.format(config["ds_ips_shape_sf_input_file"],config["ds_ips_shape_sf_histo_name"])
    )

#get mu_Hnl IPS shape scale factors histogram
if dataset_category != "data" and args.applyMuHnlIPSCorr:
    print("ips shape correction sf from {}, {}".format(config["hnl_ips_shape_sf_input_file"],config["hnl_ips_shape_sf_histo_name"]))
    ROOT.gInterpreter.Declare("""
    auto hnl_ips_shape_sf_file = TFile::Open("{}");
    auto h_hnl_ips_shape_sf = hnl_ips_shape_sf_file->Get<TH1D>("{}");
    """.format(config["hnl_ips_shape_sf_input_file"],config["hnl_ips_shape_sf_histo_name"])
    )

#define unit weights
mc_weight  = 1.
pu_weight  = 1.

# get generator weight for MC
if dataset_category != "data":
    cross_section      = float(ntuples[dataset_to_process]["cross_section"])
    filter_efficiency  = float(ntuples[dataset_to_process]["filter_efficiency"])
    total_events       = float(ntuples[dataset_to_process]["processed_events"])
    mc_weight = cross_section*filter_efficiency/total_events

print("mc_weight: {}".format(mc_weight))

#initialize chain
chain = ROOT.TChain(tree_name)
tot_file = len(inputFileName_list)
files_in_the_chain = int(0)

#add file to the chain
for inputFileName in inputFileName_list:
    print("Adding {} to the chain...".format(inputFileName))
    chain.Add(inputFileName)
    files_in_the_chain += 1
    print("[{}/{}] files added to the chain".format(files_in_the_chain,tot_file))
    print("{} entries now in the chain".format(chain.GetEntries()))

print("\n")
print("{} total entries ...".format(chain.GetEntries()))

input_file_name = inputFileName_list[0].split("/")[-1].split(".")[0]
dataset_name_label = input_file_name[input_file_name.find("_")+1:input_file_name.rfind("_")]

######################
###### ANALYSIS ######
######################

#initialize data frame
df = ROOT.RDataFrame(chain)
        
#define new variables
for var in selection["new_variables"]:
    df = df.Define(var["name"],var["definition"])

# operations on signal samples
if dataset_category == "signal":
    #define GEN matching variables and reject events w/o GEN matched candidates
    gen_matching_cuts =list()
    mask_var = "C_pass_gen_matching"
    for sel in selection["gen_matching_cuts"]:
        gen_matching_cuts.append(sel["cut"])
    df = df.Define(mask_var," && ".join(gen_matching_cuts))
    df = df.Filter("ROOT::VecOps::Any("+mask_var+")","pass GEN matching cuts")

    # define ctau weights
    if args.ctauReweighting and dataset_category == "signal":
        old_ctau_label = dataset_name_label[dataset_name_label.find("ctau")+4:dataset_name_label.find("mm")]
        hnl_mass_label = dataset_name_label[dataset_name_label.find("mN")+2:dataset_name_label.find("_ctau")]
        for new_ctau in selection["mN"+hnl_mass_label+"_ctau"+old_ctau_label+"mm_rw_points"]:
          old_ctau = float(old_ctau_label.replace("p","."))
          w_expr   = "("+str(old_ctau)+"/"+str(new_ctau)+")*"+"exp(C_Hnl_gen_l_prop*("+str(1./old_ctau)+"-"+str(1./new_ctau)+"))"
          df = df.Define("C_ctau_weight_"+old_ctau_label+"TO"+str(new_ctau).replace(".","p"),w_expr)


#################
#### WEIGHTS ####
#################

#define cross section normalization mc_weight
df = df.Define("mc_weight",str(mc_weight))    

# define pu weight for MC only
if dataset_category != "data" and not args.skipPUrw:
    pu_weight = "h_pu_weights->GetBinContent(h_pu_weights->FindBin(nPU_trueInt))"
df = df.Define("pu_weight",str(pu_weight)) 
df = df.Define("tot_weight","mc_weight*pu_weight")

# define trigger scale factors for MC only
if dataset_category != "data" and not args.skipTrigSF:
    trigger_eff_data_ds  = "get_2D_binContent(h_trigger_eff_data,C_{mu1l}_pt,C_{mu1l}_BS_ips_xy,99.0,499.0)".format(mu1l=config["mu1_label"])
    trigger_eff_mc_ds    = "get_2D_binContent(h_trigger_eff_mc,C_{mu1l}_pt,C_{mu1l}_BS_ips_xy,99.0,499.0)".format(mu1l=config["mu1_label"])
    trigger_eff_data_hnl = "get_2D_binContent(h_trigger_eff_data,C_{mu2l}_pt,C_{mu2l}_BS_ips_xy,99.0,499.0)".format(mu2l=config["mu2_label"])
    trigger_eff_mc_hnl   = "get_2D_binContent(h_trigger_eff_mc,C_{mu2l}_pt,C_{mu2l}_BS_ips_xy,99.0,499.0)".format(mu2l=config["mu2_label"])

    df = df.Define("C_trigger_eff_data_ds",str(trigger_eff_data_ds)) 
    df = df.Define("C_trigger_eff_data_hnl",str(trigger_eff_data_hnl)) 
    df = df.Define("C_trigger_eff_mc_ds",str(trigger_eff_mc_ds))
    df = df.Define("C_trigger_eff_mc_hnl",str(trigger_eff_mc_hnl)) 
    df = df.Define("C_{mu2l}_matched_HLT".format(mu2l=config["mu2_label"]),"(C_{mu2l}_matched_MU7_IP4>0 && C_{mu2l}_dr_MU7_IP4<0.05 && C_{mu2l}_dptopt_MU7_IP4<0.1 && C_{mu2l}_pt >7.5 && C_{mu2l}_eta>-1.45 && C_{mu2l}_eta<1.45) || (C_{mu2l}_matched_MU8_IP3>0 && C_{mu2l}_dr_MU8_IP3<0.05 && C_{mu2l}_dptopt_MU8_IP3<0.1 && C_{mu2l}_pt >7.5 && C_{mu2l}_eta>-1.45 && C_{mu2l}_eta<1.45) || (C_{mu2l}_matched_MU8_IP5>0 && C_{mu2l}_dr_MU8_IP5<0.05 && C_{mu2l}_dptopt_MU8_IP5<0.1 && C_{mu2l}_pt >7.5 && C_{mu2l}_eta>-1.45 && C_{mu2l}_eta<1.45) || (C_{mu2l}_matched_MU8_IP6>0 && C_{mu2l}_dr_MU8_IP6<0.05 && C_{mu2l}_dptopt_MU8_IP6<0.1 && C_{mu2l}_pt >7.5 && C_{mu2l}_eta>-1.45 && C_{mu2l}_eta<1.45) || (C_{mu2l}_matched_MU9_IP4>0 && C_{mu2l}_dr_MU9_IP4<0.05 && C_{mu2l}_dptopt_MU9_IP4<0.1 && C_{mu2l}_pt >7.5 && C_{mu2l}_eta>-1.45 && C_{mu2l}_eta<1.45) || (C_{mu2l}_matched_MU9_IP5>0 && C_{mu2l}_dr_MU9_IP5<0.05 && C_{mu2l}_dptopt_MU9_IP5<0.1 && C_{mu2l}_pt >7.5 && C_{mu2l}_eta>-1.45 && C_{mu2l}_eta<1.45) || (C_{mu2l}_matched_MU9_IP6>0 && C_{mu2l}_dr_MU9_IP6<0.05 && C_{mu2l}_dptopt_MU9_IP6<0.1 && C_{mu2l}_pt >7.5 && C_{mu2l}_eta>-1.45 && C_{mu2l}_eta<1.45) || (C_{mu2l}_matched_MU10p5_IP3p5>0 && C_{mu2l}_dr_MU10p5_IP3p5<0.05 && C_{mu2l}_dptopt_MU10p5_IP3p5<0.1 && C_{mu2l}_pt >7.5 && C_{mu2l}_eta>-1.45 && C_{mu2l}_eta<1.45) || (C_{mu2l}_matched_MU12_IP6>0 && C_{mu2l}_dr_MU12_IP6<0.05 && C_{mu2l}_dptopt_MU12_IP6<0.1 && C_{mu2l}_pt >7.5 && C_{mu2l}_eta>-1.45 && C_{mu2l}_eta<1.45)".format(mu2l=config["mu2_label"])) 
    df = df.Define("C_{mu1l}_matched_HLT".format(mu1l=config["mu1_label"]),"(C_{mu1l}_matched_MU7_IP4>0 && C_{mu1l}_dr_MU7_IP4<0.05 && C_{mu1l}_dptopt_MU7_IP4<0.1 && C_{mu1l}_pt >7.5 && C_{mu1l}_eta>-1.45 && C_{mu1l}_eta<1.45) || (C_{mu1l}_matched_MU8_IP3>0 && C_{mu1l}_dr_MU8_IP3<0.05 && C_{mu1l}_dptopt_MU8_IP3<0.1 && C_{mu1l}_pt >7.5 && C_{mu1l}_eta>-1.45 && C_{mu1l}_eta<1.45) || (C_{mu1l}_matched_MU8_IP5>0 && C_{mu1l}_dr_MU8_IP5<0.05 && C_{mu1l}_dptopt_MU8_IP5<0.1 && C_{mu1l}_pt >7.5 && C_{mu1l}_eta>-1.45 && C_{mu1l}_eta<1.45) || (C_{mu1l}_matched_MU8_IP6>0 && C_{mu1l}_dr_MU8_IP6<0.05 && C_{mu1l}_dptopt_MU8_IP6<0.1 && C_{mu1l}_pt >7.5 && C_{mu1l}_eta>-1.45 && C_{mu1l}_eta<1.45) || (C_{mu1l}_matched_MU9_IP4>0 && C_{mu1l}_dr_MU9_IP4<0.05 && C_{mu1l}_dptopt_MU9_IP4<0.1 && C_{mu1l}_pt >7.5 && C_{mu1l}_eta>-1.45 && C_{mu1l}_eta<1.45) || (C_{mu1l}_matched_MU9_IP5>0 && C_{mu1l}_dr_MU9_IP5<0.05 && C_{mu1l}_dptopt_MU9_IP5<0.1 && C_{mu1l}_pt >7.5 && C_{mu1l}_eta>-1.45 && C_{mu1l}_eta<1.45) || (C_{mu1l}_matched_MU9_IP6>0 && C_{mu1l}_dr_MU9_IP6<0.05 && C_{mu1l}_dptopt_MU9_IP6<0.1 && C_{mu1l}_pt >7.5 && C_{mu1l}_eta>-1.45 && C_{mu1l}_eta<1.45) || (C_{mu1l}_matched_MU10p5_IP3p5>0 && C_{mu1l}_dr_MU10p5_IP3p5<0.05 && C_{mu1l}_dptopt_MU10p5_IP3p5<0.1 && C_{mu1l}_pt >7.5 && C_{mu1l}_eta>-1.45 && C_{mu1l}_eta<1.45) || (C_{mu1l}_matched_MU12_IP6>0 && C_{mu1l}_dr_MU12_IP6<0.05 && C_{mu1l}_dptopt_MU12_IP6<0.1 && C_{mu1l}_pt >7.5 && C_{mu1l}_eta>-1.45 && C_{mu1l}_eta<1.45)".format(mu1l=config["mu1_label"])) 
    df = df.Define("C_trigger_sf","vcompute_total_sf(C_trigger_eff_data_ds,C_trigger_eff_mc_ds,C_{mu1l}_matched_HLT,C_trigger_eff_data_hnl,C_trigger_eff_mc_hnl,C_{mu2l}_matched_HLT)".format(mu1l=config["mu1_label"],mu2l=config["mu2_label"]))

# define mu id factors for MC only
if dataset_category != "data" and not args.skipMuIDsf:
    mu1_id_sf = "vget_mu_id_sf(mu_id_sf_cfg,C_{mu1l}_pt,C_{mu1l}_eta)".format(mu1l=config["mu1_label"])
    mu2_id_sf = "vget_mu_id_sf(mu_id_sf_cfg,C_{mu2l}_pt,C_{mu2l}_eta)".format(mu2l=config["mu2_label"])
    mu1_id_sf_up = "vget_mu_id_sf(mu_id_sf_cfg,C_{mu1l}_pt,C_{mu1l}_eta,1.)".format(mu1l=config["mu1_label"])
    mu2_id_sf_up = "vget_mu_id_sf(mu_id_sf_cfg,C_{mu2l}_pt,C_{mu2l}_eta,1.)".format(mu2l=config["mu2_label"])
    mu1_id_sf_down = "vget_mu_id_sf(mu_id_sf_cfg,C_{mu1l}_pt,C_{mu1l}_eta,-1.)".format(mu1l=config["mu1_label"])
    mu2_id_sf_down = "vget_mu_id_sf(mu_id_sf_cfg,C_{mu2l}_pt,C_{mu2l}_eta,-1.)".format(mu2l=config["mu2_label"])
    df = df.Define("C_{mu1l}_id_sf".format(mu1l=config["mu1_label"]),mu1_id_sf)
    df = df.Define("C_{mu2l}_id_sf".format(mu2l=config["mu2_label"]),mu2_id_sf)
    df = df.Define("C_{mu1l}_id_sf_up".format(mu1l=config["mu1_label"]),mu1_id_sf_up)
    df = df.Define("C_{mu2l}_id_sf_up".format(mu2l=config["mu2_label"]),mu2_id_sf_up)
    df = df.Define("C_{mu1l}_id_sf_down".format(mu1l=config["mu1_label"]),mu1_id_sf_down)
    df = df.Define("C_{mu2l}_id_sf_down".format(mu2l=config["mu2_label"]),mu2_id_sf_down)

# define mu id factors for MC only
if dataset_category != "data" and not args.skipMuRecosf:
    mu1_reco_sf      = "vget_mu_reco_sf(mu_reco_sf_cfg,C_{mu1l}_pt,C_{mu1l}_eta)".format(mu1l=config["mu1_label"])
    mu2_reco_sf      = "vget_mu_reco_sf(mu_reco_sf_cfg,C_{mu2l}_pt,C_{mu2l}_eta)".format(mu2l=config["mu2_label"])
    mu1_reco_sf_up   = "vget_mu_reco_sf(mu_reco_sf_cfg,C_{mu1l}_pt,C_{mu1l}_eta,1.)".format(mu1l=config["mu1_label"])
    mu2_reco_sf_up   = "vget_mu_reco_sf(mu_reco_sf_cfg,C_{mu2l}_pt,C_{mu2l}_eta,1.)".format(mu2l=config["mu2_label"])
    mu1_reco_sf_down = "vget_mu_reco_sf(mu_reco_sf_cfg,C_{mu1l}_pt,C_{mu1l}_eta,-1.)".format(mu1l=config["mu1_label"])
    mu2_reco_sf_down = "vget_mu_reco_sf(mu_reco_sf_cfg,C_{mu2l}_pt,C_{mu2l}_eta,-1.)".format(mu2l=config["mu2_label"])
    df = df.Define("C_{mu1l}_reco_sf".format(mu1l=config["mu1_label"]),mu1_reco_sf)
    df = df.Define("C_{mu2l}_reco_sf".format(mu2l=config["mu2_label"]),mu2_reco_sf)
    df = df.Define("C_{mu1l}_reco_sf_up".format(mu1l=config["mu1_label"]),mu1_reco_sf_up)
    df = df.Define("C_{mu2l}_reco_sf_up".format(mu2l=config["mu2_label"]),mu2_reco_sf_up)
    df = df.Define("C_{mu1l}_reco_sf_down".format(mu1l=config["mu1_label"]),mu1_reco_sf_down)
    df = df.Define("C_{mu2l}_reco_sf_down".format(mu2l=config["mu2_label"]),mu2_reco_sf_down)


# define mu_Ds pt shape scale factors for MC only
if dataset_category != "data" and args.applyMuDsPtCorr:
    ds_pt_shape_sf  = "get_1D_binContent(h_ds_pt_shape_sf,C_{}_pt)".format(config["mu1_label"])
    df = df.Define("C_ds_pt_shape_sf",str(ds_pt_shape_sf)) 

# define mu_Hnl pt shape scale factors for MC only
if dataset_category != "data" and args.applyMuHnlPtCorr:
    hnl_pt_shape_sf  = "get_1D_binContent(h_hnl_pt_shape_sf,C_{}_pt)".format(config["mu2_label"])
    df = df.Define("C_hnl_pt_shape_sf",str(hnl_pt_shape_sf)) 

# define mu_Ds IPS shape scale factors for MC only
if dataset_category != "data" and args.applyMuDsIPSCorr:
    ds_ips_shape_sf  = "get_1D_binContent(h_ds_ips_shape_sf,C_{}_BS_ips_xy)".format(config["mu1_label"])
    df = df.Define("C_ds_ips_shape_sf",str(ds_ips_shape_sf)) 

# define mu_Hnl IPS shape scale factors for MC only
if dataset_category != "data" and args.applyMuHnlIPSCorr:
    hnl_ips_shape_sf  = "get_1D_binContent(h_hnl_ips_shape_sf,C_{}_BS_ips_xy)".format(config["mu2_label"])
    df = df.Define("C_hnl_ips_shape_sf",str(hnl_ips_shape_sf)) 

if args.bestCandChecks:
    hmodel = ("h_mult_input",";Candidate multiplicity;Normalized to unit",20,1.,21.)
    df_check = df.Define("mult_input","get_cand_multiplicity(C_Hnl_mass)")
    df_check.Histo1D(hmodel,"mult_input").SaveAs("h_cand_mult_input_{}.root".format(dataset_to_process))
    df_check = df_check.Filter("mult_input>1","fraction of input events with more than 1 candidate")
    # define a index selecting best candidate in the event
    df_check = df_check.Define(selection["best_cand_var"]["name"],selection["best_cand_var"]["definition"])
    
    # redefine variables so that only best candidate is saved
    for c in df_check.GetColumnNames():
        col_name = str(c)
        col_type = df_check.GetColumnType(col_name)
        # choose candidate branches (beginning with 'C_')
        if (not col_name.find("C_")<0) and (not col_type.find("ROOT::VecOps")<0):
            idx = str(selection["best_cand_var"]["name"])
            df_check = df_check.Redefine(col_name,col_name+"["+idx+"]")
            continue
    if dataset_category == "signal":
        df_check = df_check.Filter("C_pass_gen_matching","best-cand-input events have a GEN-matched candidate")
    df_check.Report().Print()

######################
#### PRESELECTION ####
######################

#apply common pre-selection
presel_i = 0
presel_cuts = list()
for sel in selection["preselection_cuts"]:
    mask_var = "pass_presel_"+str(presel_i)
    presel_cuts.append(mask_var)
    df = df.Define(mask_var,sel["cut"])
    df = df.Filter("ROOT::VecOps::Any("+mask_var+")",sel["printout"])
    presel_i+=1

# skim vectors to retain only candidates passing preselection
for c in df.GetColumnNames():
    col_name = str(c)
    col_type = df.GetColumnType(col_name)
    # choose candidate branches (beginning with 'C_')
    if(hnl_tools.is_good_cand_var(col_name) and (not col_type.find("ROOT::VecOps")<0)): 
        presel_cuts_AND = "&&".join(presel_cuts)  
        df = df.Redefine(col_name,col_name+"["+presel_cuts_AND+"]")
        continue

# count how many preselected events have at least a GEN-matched candidate
if args.bestCandChecks:
    hmodel = ("h_mult_presel",";Candidate multiplicity;Normalized to unit",20,1.,21.)
    df_check = df.Define("mult_presel","get_cand_multiplicity(C_Hnl_mass)")
    df_check.Histo1D(hmodel,"mult_presel").SaveAs("h_cand_mult_presel_{}.root".format(dataset_to_process))
    df_check = df_check.Filter("mult_presel>1","fraction of preselected events with more than 1 candidate")
    # define a index selecting best candidate in the event
    df_check = df_check.Define(selection["best_cand_var"]["name"],selection["best_cand_var"]["definition"])
    
    # redefine variables so that only best candidate is saved
    for c in df_check.GetColumnNames():
        col_name = str(c)
        col_type = df_check.GetColumnType(col_name)
        # choose candidate branches (beginning with 'C_')
        if (not col_name.find("C_")<0) and (not col_type.find("ROOT::VecOps")<0):
            idx = str(selection["best_cand_var"]["name"])
            df_check = df_check.Redefine(col_name,col_name+"["+idx+"]")
            continue
    if dataset_category == "signal":
        df_check = df_check.Filter("C_pass_gen_matching","best-cand-preselected events have at least a GEN-matched candidate")
    df_check.Report().Print()

#save preselected tree
if args.savePreselectedTree:
    finalTree_outputFileName = "tree_"+dataset_name_label+".root"
    finalTree_outputDirName = os.path.join(config["preselected_tree_output_dir_name"],dataset_name_label)
    if tag!="":
        finalTree_outputFileName = "tree_"+tag+"_"+dataset_name_label+".root"
    subprocess.call(['mkdir','-p',finalTree_outputDirName])
    finalTree_outFullPath = os.path.join(finalTree_outputDirName,finalTree_outputFileName)
    var_list = df.GetColumnNames()
    var_keep_list = args.keep
    if len(var_keep_list)>0:
        var_list = var_keep_list
    df.Snapshot(config["tree_output_name"],finalTree_outFullPath,var_list)
    print("Preselected tree saved in {}".format(finalTree_outFullPath))

###################
#### SELECTION ####
###################

#apply optimized selection for each category
sel_cuts = dict()
for cat in selection["categories"]:
    l = list()
    sel_i = 0
    for cut in cat["selection_cuts"]:
        mask_var = "pass_sel_"+cat["label"]+"_"+str(sel_i)
        l.append(mask_var)
        df = df.Define(mask_var,cut["cut"]+' && '+cat["cut"])
        sel_i+=1
    sel_cuts[cat["label"]] = l

# build AND of each category's selection cuts
sel_cuts_AND = list()
for cat in sel_cuts:
    s = "&&".join(sel_cuts[cat])
    sel_cuts_AND.append(s)

# filter events which do not pass any of the categories' selection cuts
df = df.Filter("ROOT::VecOps::Any("+"||".join(sel_cuts_AND)+")","selection cuts") 

# skim vectors to retain only candidates passing selection cuts
for c in df.GetColumnNames():
    col_name = str(c)
    col_type = df.GetColumnType(col_name)
    # choose candidate branches (beginning with 'C_')
    if hnl_tools.is_good_cand_var(col_name) and col_type.find("ROOT::VecOps")==0: 
        df = df.Redefine(col_name,col_name+"["+"||".join(sel_cuts_AND)+"]")

# count how many selected events have at least a GEN-matched candidate
if args.bestCandChecks:
    hmodel = ("h_mult_sel",";Candidate multiplicity;Normalized to unit",20,1.,21.)
    df_check = df.Define("mult_sel","get_cand_multiplicity(C_Hnl_mass)")
    df_check.Histo1D(hmodel,"mult_sel").SaveAs("h_cand_mult_sel_{}.root".format(dataset_to_process))
    df_check = df_check.Filter("mult_sel>1","fraction of selected events with more than 1 candidate")
    #vl = df.GetColumnNames()
    #df_check.Snapshot("test","test.root",vl)
    # define a index selecting best candidate in the event
    df_check = df_check.Define(selection["best_cand_var"]["name"],selection["best_cand_var"]["definition"])
    
    # redefine variables so that only best candidate is saved
    for c in df_check.GetColumnNames():
        col_name = str(c)
        col_type = df_check.GetColumnType(col_name)
        # choose candidate branches (beginning with 'C_')
        if (not col_name.find("C_")<0) and (not col_type.find("ROOT::VecOps")<0):
            idx = str(selection["best_cand_var"]["name"])
            df_check = df_check.Redefine(col_name,col_name+"["+idx+"]")
            continue
    if dataset_category == "signal":
        df_check = df_check.Filter("C_pass_gen_matching","best-cand-selected events have at least a GEN-matched candidate")
    df_check.Report().Print()

###################
#### BEST CAND ####
###################

# define a index selecting best candidate in the event
df = df.Define(selection["best_cand_var"]["name"],selection["best_cand_var"]["definition"])

# redefine variables so that only best candidate is saved
for c in df.GetColumnNames():
    col_name = str(c)
    col_type = df.GetColumnType(col_name)
    # choose candidate branches (beginning with 'C_')
    if col_name.find("C_")==0 and col_type.find("ROOT::VecOps")==0:
        idx = str(selection["best_cand_var"]["name"])
        df = df.Redefine(col_name,col_name+"["+idx+"]")
        continue

if dataset_category == "signal":
    df = df.Filter("C_pass_gen_matching","best-candidate-selected events have at least a GEN-matched candidate")

##define categories
#df = df.Define("C_cat","get_lxy_categories(C_Hnl_vertex_2DDist_BS,C_mu_Hnl_charge,C_mu_Ds_charge)")
df = df.Define("C_cat",selection["categorization"])

# redefine here the total weight as the product of all weights previously defined

# Include trigger SF
if dataset_category != "data" and not args.skipTrigSF:
    df = df.Redefine("tot_weight","tot_weight*C_trigger_sf")

# Include ID scale factors
if dataset_category != "data" and not args.skipMuIDsf:
    df = df.Redefine("tot_weight","tot_weight*C_{mu1l}_id_sf*C_{mu2l}_id_sf".format(mu1l=config["mu1_label"],mu2l=config["mu2_label"]))
    df = df.Define("C_mu_id_sf_up","C_{mu1l}_id_sf_up*C_{mu2l}_id_sf_up".format(mu1l=config["mu1_label"],mu2l=config["mu2_label"]))
    df = df.Define("C_mu_id_sf_down","C_{mu1l}_id_sf_down*C_{mu2l}_id_sf_down".format(mu1l=config["mu1_label"],mu2l=config["mu2_label"]))

# Include RECO scale factors
if dataset_category != "data" and not args.skipMuRecosf:
    df = df.Redefine("tot_weight","tot_weight*C_{mu1l}_reco_sf*C_{mu2l}_reco_sf".format(mu1l=config["mu1_label"],mu2l=config["mu2_label"]))
    df = df.Define("C_mu_reco_sf_up","C_{mu1l}_reco_sf_up*C_{mu2l}_reco_sf_up".format(mu1l=config["mu1_label"],mu2l=config["mu2_label"]))
    df = df.Define("C_mu_reco_sf_down","C_{mu1l}_reco_sf_down*C_{mu2l}_reco_sf_down".format(mu1l=config["mu1_label"],mu2l=config["mu2_label"]))

#################
##### SAVE ######
#################

if args.saveOutputTree:
    finalTree_outputFileName = "tree_"+dataset_name_label+".root"
    finalCSV_outputFileName  = "tree_"+dataset_name_label+".csv"
    finalTree_outputDirName = os.path.join(config["tree_output_dir_name"],dataset_name_label)
    if tag!="":
        finalTree_outputFileName = "tree_"+tag+"_"+dataset_name_label+".root"
        finalCSV_outputFileName  = "tree_"+tag+"_"+dataset_name_label+".csv"
    subprocess.call(['mkdir','-p',finalTree_outputDirName])
    finalTree_outFullPath = os.path.join(finalTree_outputDirName,finalTree_outputFileName)
    finalCSV_outFullPath = os.path.join(finalTree_outputDirName,finalCSV_outputFileName)

    #save output tree
    var_list = df.GetColumnNames()
    csv_var_list = [x for x in df.GetColumnNames() if hnl_tools.is_good_cand_var(x) or x.find("tot_weight")==0 or x.find("mc_weight")==0]
    var_keep_list = args.keep
    if len(var_keep_list)>0:
        var_list = var_keep_list
        csv_var_list = var_keep_list
    df.Snapshot(config["tree_output_name"],finalTree_outFullPath,var_list)

    #save output csv
    a = df.AsNumpy(csv_var_list)
    pdf = pd.DataFrame(a)
    pdf.to_csv(finalCSV_outFullPath, index=False)
    print("Output tree saved in {}".format(finalTree_outFullPath))
    print("Output csv saved in {}".format(finalCSV_outFullPath))

    #update json
    key = "final_file_name_list"
    if args.addTag != "":
        key = key+"_"+str(args.addTag)
    ntuples[dataset_to_process][key] = [str(finalTree_outFullPath)]
    with open(config["ntuples_cfg_file_full_path"], "w") as f:
        json.dump(ntuples,f, indent=4, sort_keys=True)
    print("{} updated".format(config["ntuples_cfg_file_full_path"]))

##################
##### SPLOT ######
##################

if dataset_category=="data" and args.addSPlotWeight:
    print("Input splot weight file: {}".format(ntuples[dataset_to_process]["splot_weight_input_file"]))
    sdf = ROOT.RDataFrame(ntuples[dataset_to_process]["splot_weight_tree_name"],ntuples[dataset_to_process]["splot_weight_input_file"]) # get tree containing splot weights
    asw = sdf.AsNumpy([ntuples[dataset_to_process]["splot_weight_variable"]])[ntuples[dataset_to_process]["splot_weight_variable"]] # get column of splot weights
    print("entries in splot tree: {}".format(len(asw)))
    print("entries in analyzed tree: {}".format(df.Count().GetValue()))
    if len(asw) != df.Count().GetValue():
        print("!!! splot tree and analyzed tree do not have the same number of entries !!!")
        print("!!! please check that you are using the correct input tree !!!")
        exit()
    df = df.Define("splot_weight",'auto to_eval = std::string("asw[") + std::to_string(rdfentry_) + "]"; return float(TPython::Eval(to_eval.c_str()));') 
    df = df.Redefine("tot_weight","tot_weight*splot_weight")

#######################
##### HISTOGRAMS ######
#######################

if args.doHistograms:
    hnl_tools.do_histos(df, config, dataset_name_label, tag="")

# TODO:define per category reports
print("+++ FINAL REPORT +++")
report= df.Report()
report.Print()
print("--- Analysis completed in {} seconds ---".format(time.time() - start_time))


