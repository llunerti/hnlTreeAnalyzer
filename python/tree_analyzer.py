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
start_time = time.time()

#script input arguments
parser = argparse.ArgumentParser(description="")
parser.add_argument("cfg_filename"        ,                                     help="Path to the input configuration file")
parser.add_argument("dataset_short_name"  ,                                     help="Short name of the input sample to process")
parser.add_argument("--saveOutputTree"    , action='store_true', default=False, help="Save an output tree (after all the cuts)")
parser.add_argument("--saveSlimmedTree"   , action='store_true', default=False, help="Save an output tree (before the cuts and after best candidate selection)")
parser.add_argument("--noHistograms"      , action='store_true', default=False, help="Do not save histogram as output")
parser.add_argument("--addSPlotWeight"    , action='store_true', default=False, help="Add splot weight for data")
parser.add_argument("--skipSlimCuts"      , action='store_true', default=False, help="Skip best candidate selection (N.B. a tree where the best candidate has been already been selected has to be provided in the input cfg file)")
parser.add_argument("--skipSelCuts"       , action='store_true', default=False, help="Do not apply selection cuts")
parser.add_argument("--skipPUrw"          , action='store_true', default=False, help="Do not apply PU reweighting")
parser.add_argument("--skipTrigSF"        , action='store_true', default=False, help="Do not apply trigger scale factors")
parser.add_argument("--skipMuIDsf"        , action='store_true', default=False, help="Do not apply muon id scale factors")
parser.add_argument("--skipMuRecosf"      , action='store_true', default=False, help="Do not apply muon reco scale factors")
parser.add_argument("--nThreads"          , type=int           , default=1    , help="Number of threads")
parser.add_argument("--addTag"            , type=str           , default=""   , help="Tag output files")
parser.add_argument("--ctauReweighting"   , action='store_true', default=False, help="Include ctau reweighting")
parser.add_argument("--applyMuDsPtCorr"   , action='store_true', default=False, help="Apply reweighting to mu from Ds to correct data/MC pt discrepancies")
parser.add_argument("--applyMuHnlPtCorr"  , action='store_true', default=False, help="Apply reweighting to mu from Hnl to correct data/MC pt discrepancies")
parser.add_argument("--applyMuDsIPSCorr"  , action='store_true', default=False, help="Apply reweighting to mu from Ds to correct data/MC IPS discrepancies")
parser.add_argument("--applyMuHnlIPSCorr" , action='store_true', default=False, help="Apply reweighting to mu from Hnl to correct data/MC IPS discrepancies")
parser.add_argument("--varyMuIDSf"        , type=float         , default=0.0  , help="Muon ID sf w/ variations: sf = sf+variation*error")
parser.add_argument("--varyMuRecoSf"      , type=float         , default=0.0  , help="Muon reco sf w/ variations: sf = sf+variation*error")
parser.add_argument("--keep"              , nargs="*"          , default=[]   , help="Select which branches to keep in the final output tree")
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

#get histogram configuration
with open(config["histogram_cfg_file_full_path"], "r") as f:
    histos = json.loads(f.read())

#get selection and categorization cuts
with open(config["selection_cfg_file_full_path"], "r") as f:
    selection = json.loads(f.read())

# let private function to be avaliable in RDataFrame evn
header_path = config["user_defined_function_path"]
ROOT.gInterpreter.Declare('#include "{}"'.format(header_path))

input_file_list = "file_name_list"
tree_name = "wztree"

if args.skipSlimCuts:
    input_file_list = "slimmed_file_name_list"
    tree_name = "slimmed_tree"

if args.addSPlotWeight and args.skipSelCuts and args.skipSlimCuts:
    input_file_list = "final_file_name_list"
    tree_name = "final_tree"

#get input files
inputFileName_list = ntuples[dataset_to_process][input_file_list]
dataset_category = ntuples[dataset_to_process]["dataset_category"]

#get pu weights histogram
if dataset_category != "data" and not args.skipPUrw:
    ROOT.gInterpreter.Declare("""
    auto pu_weights_file = TFile::Open("{}");
    auto h_pu_weights = pu_weights_file->Get<TH1D>("{}");
    """.format(config["pu_weight_input_file"],config["pu_weight_histo_name"])
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

# save slimmed tree only once without applying categorization
slimmed_tree_has_been_saved = False
if args.saveOutputTree:
    key = "final_file_name_list"
    if args.addTag != "":
        key = key+"_"+str(args.addTag)
    ntuples[dataset_to_process][key] = []

reports = {}
weighted_events_reports = {}
for cat in selection["categories"]:

    #initialize data frame
    df = ROOT.RDataFrame(chain)
    
    if not args.skipSlimCuts:
        # define a index selecting best candidate in the event
        df = df.Define(selection["best_cand_var"]["name"],selection["best_cand_var"]["definition"])
          
        #build new variables and get bet candidate idx
        for var in selection["new_variables"]:
            df = df.Define(var["name"],var["definition"])

    
        # redefine variables so that only best candidate is saved
        for c in df.GetColumnNames():
            col_name = str(c)
            candidate_columns = []
            col_type = df.GetColumnType(col_name)
            # choose candidate branches (beginning with 'C_')
            if col_name.find("C_")<0: 
                continue
            # save best candidate only
            if not col_type.find("ROOT::VecOps")<0:
                idx = str(selection["best_cand_var"]["name"])
                df = df.Redefine(col_name,col_name+"["+idx+"]")
                continue

        # save slimmed tree: only the best candidate is saved for each event
        if args.saveSlimmedTree and not slimmed_tree_has_been_saved:
            slim_outputFileName = "slimmed_"+dataset_name_label+".root"
            if tag != "":
                slim_outputFileName = "slimmed_"+tag+"_"+dataset_name_label+".root"
            slim_outputDirName = os.path.join(config["slimmed_tree_output_dir_name"],dataset_name_label)
            subprocess.call(['mkdir','-p',slim_outputDirName])
            slim_outFullPath = os.path.join(slim_outputDirName,slim_outputFileName)
            df.Snapshot(config["slimmed_tree_output_name"],slim_outFullPath,df.GetColumnNames())
            slimmed_tree_has_been_saved = True
            print("Slimmed tree saved in {}".format(slim_outFullPath))
            key = "slimmed_file_name_list"
            if args.addTag != "":
                key = key+"_"+str(args.addTag)
            ntuples[dataset_to_process][key] = [str(slim_outFullPath)]
            with open(config["ntuples_cfg_file_full_path"], "w") as f:
                json.dump(ntuples,f, indent=4, sort_keys=True)
            print("{} updated".format(config["ntuples_cfg_file_full_path"])) 
 
    if args.skipSelCuts and args.noHistograms:
        break

    if not args.skipSelCuts:
                
        #get mc truth in case of signal sample
        if dataset_category == "signal":
            for sel in selection["gen_matching_cuts"]:
                df = df.Filter(sel["cut"],sel["printout"])
            if args.ctauReweighting and dataset_category == "signal":
                old_ctau_label = dataset_name_label[dataset_name_label.find("ctau")+4:dataset_name_label.find("mm")]
                hnl_mass_label = dataset_name_label[dataset_name_label.find("mN")+2:dataset_name_label.find("mN")+5]
                for new_ctau in selection["mN"+hnl_mass_label+"_ctau"+old_ctau_label+"mm_rw_points"]:
                  old_ctau = float(old_ctau_label.replace("p","."))
                  w_expr   = "("+str(old_ctau)+"/"+str(new_ctau)+")*"+"exp(C_Hnl_gen_l_prop*("+str(1./old_ctau)+"-"+str(1./new_ctau)+"))"
                  df = df.Define("ctau_weight_"+old_ctau_label+"TO"+str(new_ctau).replace(".","p"),w_expr)
                  
        #apply categorization 
        df = df.Filter(cat["cut"] ,cat["printout"])
        #apply selection
        for sel in cat["selection_cuts"]:
            df = df.Filter(sel["cut"],sel["printout"])

        #define cross section normalization mc_weight
        df = df.Define("mc_weight",str(mc_weight))    
    
        # define pu weight for MC only
        if dataset_category != "data" and not args.skipPUrw:
            pu_weight = "h_pu_weights->GetBinContent(h_pu_weights->FindBin(nPU_trueInt))"
        df = df.Define("pu_weight",str(pu_weight)) 
    
        df = df.Define("tot_weight","mc_weight*pu_weight")

        # define trigger scale factors for MC only
        if dataset_category != "data" and not args.skipTrigSF:
            trigger_eff_data_ds = "h_trigger_eff_data->GetBinContent(h_trigger_eff_data->FindBin(C_{mu1l}_pt>100.0?99.9:C_{mu1l}_pt,C_{mu1l}_BS_ips_xy>500.0?499.9:C_{mu1l}_BS_ips_xy))".format(mu1l=config["mu1_label"])
            trigger_eff_mc_ds   = "h_trigger_eff_mc->GetBinContent(h_trigger_eff_mc->FindBin(C_{mu1l}_pt>100.0?99.9:C_{mu1l}_pt,C_{mu1l}_BS_ips_xy>500.0?499.9:C_{mu1l}_BS_ips_xy))".format(mu1l=config["mu1_label"])
            trigger_eff_data_hnl = "h_trigger_eff_data->GetBinContent(h_trigger_eff_data->FindBin(C_{mu2l}_pt>100.0?99.9:C_{mu2l}_pt,C_{mu2l}_BS_ips_xy>500.0?499.9:C_{mu2l}_BS_ips_xy))".format(mu2l=config["mu2_label"])
            trigger_eff_mc_hnl   = "h_trigger_eff_mc->GetBinContent(h_trigger_eff_mc->FindBin(C_{mu2l}_pt>100.0?99.9:C_{mu2l}_pt,C_{mu2l}_BS_ips_xy>500.0?499.9:C_{mu2l}_BS_ips_xy))".format(mu2l=config["mu2_label"])
            df = df.Define("trigger_eff_data_ds",str(trigger_eff_data_ds)) 
            df = df.Define("trigger_eff_data_hnl",str(trigger_eff_data_hnl)) 
            df = df.Define("trigger_eff_mc_ds",str(trigger_eff_mc_ds))
            df = df.Define("trigger_eff_mc_hnl",str(trigger_eff_mc_hnl)) 
            df = df.Define("C_{mu2l}_matched_HLT".format(mu2l=config["mu2_label"]),"(C_{mu2l}_matched_MU7_IP4>0 && C_{mu2l}_dr_MU7_IP4<0.005) || (C_{mu2l}_matched_MU8_IP3>0 && C_{mu2l}_dr_MU8_IP3<0.005) || (C_{mu2l}_matched_MU8_IP5>0 && C_{mu2l}_dr_MU8_IP5<0.005) || (C_{mu2l}_matched_MU8_IP6>0 && C_{mu2l}_dr_MU8_IP6<0.005) || (C_{mu2l}_matched_MU9_IP4>0 && C_{mu2l}_dr_MU9_IP4<0.005) || (C_{mu2l}_matched_MU9_IP5>0 && C_{mu2l}_dr_MU9_IP5<0.005) || (C_{mu2l}_matched_MU9_IP6>0 && C_{mu2l}_dr_MU9_IP6<0.005) || (C_{mu2l}_matched_MU10p5_IP3p5>0 && C_{mu2l}_dr_MU10p5_IP3p5<0.005) || (C_{mu2l}_matched_MU12_IP6>0 && C_{mu2l}_dr_MU12_IP6<0.005)".format(mu2l=config["mu2_label"])) 
            df = df.Define("C_{mu1l}_matched_HLT".format(mu1l=config["mu1_label"]),"(C_{mu1l}_matched_MU7_IP4>0 && C_{mu1l}_dr_MU7_IP4<0.005) || (C_{mu1l}_matched_MU8_IP3>0 && C_{mu1l}_dr_MU8_IP3<0.005) || (C_{mu1l}_matched_MU8_IP5>0 && C_{mu1l}_dr_MU8_IP5<0.005) || (C_{mu1l}_matched_MU8_IP6>0 && C_{mu1l}_dr_MU8_IP6<0.005) || (C_{mu1l}_matched_MU9_IP4>0 && C_{mu1l}_dr_MU9_IP4<0.005) || (C_{mu1l}_matched_MU9_IP5>0 && C_{mu1l}_dr_MU9_IP5<0.005) || (C_{mu1l}_matched_MU9_IP6>0 && C_{mu1l}_dr_MU9_IP6<0.005) || (C_{mu1l}_matched_MU10p5_IP3p5>0 && C_{mu1l}_dr_MU10p5_IP3p5<0.005) || (C_{mu1l}_matched_MU12_IP6>0 && C_{mu1l}_dr_MU12_IP6<0.005)".format(mu1l=config["mu1_label"])) 
            df = df.Define("trigger_sf","compute_total_sf(trigger_eff_data_ds,trigger_eff_mc_ds,C_{mu1l}_matched_HLT,trigger_eff_data_hnl,trigger_eff_mc_hnl,C_{mu2l}_matched_HLT)".format(mu1l=config["mu1_label"],mu2l=config["mu2_label"]))
            df = df.Redefine("tot_weight","tot_weight*trigger_sf")

        # define mu id factors for MC only
        if dataset_category != "data" and not args.skipMuIDsf:
            variation = args.varyMuIDSf
            mu1_id_sf = "get_mu_id_sf(mu_id_sf_cfg,C_{mu1l}_pt,C_{mu1l}_eta,{variation})".format(mu1l=config["mu1_label"],variation=variation)
            mu2_id_sf = "get_mu_id_sf(mu_id_sf_cfg,C_{mu2l}_pt,C_{mu2l}_eta,{variation})".format(mu2l=config["mu2_label"],variation=variation)
            df = df.Define("mu1_id_sf",mu1_id_sf) 
            df = df.Define("mu2_id_sf",mu2_id_sf) 
            df = df.Redefine("tot_weight","tot_weight*mu1_id_sf*mu2_id_sf")
        
        # define mu id factors for MC only
        if dataset_category != "data" and not args.skipMuRecosf:
            variation = args.varyMuRecoSf
            mu1_reco_sf = "get_mu_reco_sf(mu_reco_sf_cfg,C_{mu1l}_pt,C_{mu1l}_eta,{variation})".format(mu1l=config["mu1_label"],variation=variation)
            mu2_reco_sf = "get_mu_reco_sf(mu_reco_sf_cfg,C_{mu2l}_pt,C_{mu2l}_eta,{variation})".format(mu2l=config["mu2_label"],variation=variation)
            df = df.Define("mu1_reco_sf",mu1_reco_sf) 
            df = df.Define("mu2_reco_sf",mu2_reco_sf) 
            df = df.Redefine("tot_weight","tot_weight*mu1_reco_sf*mu2_reco_sf")

        # define mu_Ds pt shape scale factors for MC only
        if dataset_category != "data" and args.applyMuDsPtCorr:
            ds_pt_shape_sf  = "h_ds_pt_shape_sf->GetBinContent(h_ds_pt_shape_sf->FindBin(C_{}_pt))".format(config["mu1_label"])
            df = df.Define("ds_pt_shape_sf",str(ds_pt_shape_sf)) 
            df = df.Redefine("tot_weight","tot_weight*ds_pt_shape_sf")

        # define mu_Hnl pt shape scale factors for MC only
        if dataset_category != "data" and args.applyMuHnlPtCorr:
            hnl_pt_shape_sf  = "h_hnl_pt_shape_sf->GetBinContent(h_hnl_pt_shape_sf->FindBin(C_{}_pt))".format(config["mu2_label"])
            df = df.Define("hnl_pt_shape_sf",str(hnl_pt_shape_sf)) 
            df = df.Redefine("tot_weight","tot_weight*hnl_pt_shape_sf")

        # define mu_Ds IPS shape scale factors for MC only
        if dataset_category != "data" and args.applyMuDsIPSCorr:
            ds_ips_shape_sf  = "h_ds_ips_shape_sf->GetBinContent(h_ds_ips_shape_sf->FindBin(C_{}_BS_ips_xy))".format(config["mu1_label"])
            df = df.Define("ds_ips_shape_sf",str(ds_ips_shape_sf)) 
            df = df.Redefine("tot_weight","tot_weight*ds_ips_shape_sf")

        # define mu_Hnl IPS shape scale factors for MC only
        if dataset_category != "data" and args.applyMuHnlIPSCorr:
            hnl_ips_shape_sf  = "h_hnl_ips_shape_sf->GetBinContent(h_hnl_ips_shape_sf->FindBin(C_{}_BS_ips_xy))".format(config["mu2_label"])
            df = df.Define("hnl_ips_shape_sf",str(hnl_ips_shape_sf)) 
            df = df.Redefine("tot_weight","tot_weight*hnl_ips_shape_sf")

        if args.saveOutputTree and cat["save"]=="yes":
            finalTree_outputFileName = "tree_"+dataset_name_label+"_"+cat["label"]+".root"
            finalCSV_outputFileName  = "tree_"+dataset_name_label+"_"+cat["label"]+".csv"
            finalTree_outputDirName = os.path.join(config["tree_output_dir_name"],dataset_name_label)
            if tag!="":
                finalTree_outputFileName = "tree_"+tag+"_"+dataset_name_label+"_"+cat["label"]+".root"
                finalCSV_outputFileName  = "tree_"+tag+"_"+dataset_name_label+"_"+cat["label"]+".csv"
            subprocess.call(['mkdir','-p',finalTree_outputDirName])
            finalTree_outFullPath = os.path.join(finalTree_outputDirName,finalTree_outputFileName)
            finalCSV_outFullPath = os.path.join(finalTree_outputDirName,finalCSV_outputFileName)
            #save output tree
            var_list = list(df.GetColumnNames())
            csv_var_list = [x for x in df.GetColumnNames() if x.find("C_")==0 or x.find("ctau_weight")==0 or x.find("tot_weight")==0 or x.find("mc_weight")==0]
            var_keep_list = args.keep
            if len(var_keep_list)>0:
                var_list = var_keep_list
                csv_var_list = var_keep_list
            df.Snapshot(config["tree_output_name"],finalTree_outFullPath,var_list)
            #save output csv
            a = df.AsNumpy(csv_var_list)
            arr = numpy.array([x for x in a.values()]).transpose()
            numpy.savetxt(finalCSV_outFullPath, arr, delimiter=',', header=",".join([str(x) for x in a.keys()]), comments='')
            print("Output tree saved in {}".format(finalTree_outFullPath))
            print("Output csv saved in {}".format(finalCSV_outFullPath))
            key = "final_file_name_list"
            if args.addTag != "":
                key = key+"_"+str(args.addTag)
            ntuples[dataset_to_process][key] += [str(finalTree_outFullPath)]
            with open(config["ntuples_cfg_file_full_path"], "w") as f:
                json.dump(ntuples,f, indent=4, sort_keys=True)
            print("{} updated".format(config["ntuples_cfg_file_full_path"]))

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

    #save histograms
    if not args.noHistograms:
        histo_outputFileName = "histograms_"+dataset_name_label+"_"+cat["label"]+".root"
        if tag != "":
            histo_outputFileName = "histograms_"+tag+"_"+dataset_name_label+"_"+cat["label"]+".root"
        histo_outputDirName = config["output_dir_name"]        
        subprocess.call(['mkdir','-p',histo_outputDirName])
        histo_outFullPath = os.path.join(histo_outputDirName,histo_outputFileName)
        histo_outputFile = ROOT.TFile.Open(histo_outFullPath,"RECREATE")

        #book histograms
        histo_dict = {}
        for histo_name in histos:
            title = str(histos[histo_name]["title"])
            nbins = int(histos[histo_name]["nbins"])
            xlow  = float(histos[histo_name]["xlow"])
            xhigh = float(histos[histo_name]["xhigh"])
            histo_model = (histo_name,title,nbins,xlow,xhigh)

            var_name = str(histos[histo_name]["var"])

            histo_dict[histo_name]= df.Histo1D(histo_model,var_name,"tot_weight")

        #Add ctau reweighted histograms
        if args.ctauReweighting and dataset_category=="signal":
            for w in [str(x) for x in df.GetColumnNames() if not str(x).find("ctau_weight_")<0]:
                weight_label = w.split("_")[-1]
                df = df.Define("tot_weight_"+weight_label,"tot_weight*"+w)
                for histo_name in histos:
                    weighted_histo_name = histo_name+"_"+weight_label
                    title = str(histos[histo_name]["title"])+"_"+weight_label
                    nbins = int(histos[histo_name]["nbins"])
                    xlow  = float(histos[histo_name]["xlow"])
                    xhigh = float(histos[histo_name]["xhigh"])
                    histo_model = (weighted_histo_name,title,nbins,xlow,xhigh)

                    var_name = str(histos[histo_name]["var"])
                    #print("---> {}".format(var_name))

                    histo_dict[weighted_histo_name]= df.Histo1D(histo_model,var_name,"tot_weight_"+weight_label)

        # write histograms on file
        for histo_name in histo_dict:
            histo_outputFile.cd()
            histo_dict[histo_name].Write()
        
        histo_outputFile.Close()
        print("Output histograms saved in {}".format(os.path.join(histo_outputDirName,histo_outputFileName)))
    
    # Fill in output reports per category
    reports[cat["label"]] = df.Report()
    if args.ctauReweighting and dataset_category=="signal":
        old_ctau_label = dataset_name_label[dataset_name_label.find("ctau")+4:dataset_name_label.find("mm")]
        hnl_mass_label = dataset_name_label[dataset_name_label.find("mN")+2:dataset_name_label.find("mN")+5]
        events = {}
        for new_ctau in selection["mN"+hnl_mass_label+"_ctau"+old_ctau_label+"mm_rw_points"]:
            weight_var = "ctau_weight_"+old_ctau_label+"TO"+str(new_ctau).replace(".","p")
            weighted_events = df.Sum(weight_var).GetValue()
            events[weight_var+"_weighted_events"] = weighted_events
            weighted_events_reports[cat["label"]] = events

print("+++ FINAL REPORT +++")
for c in reports:
    print("--> {} category".format(c))
    reports[c].Print()
    if args.ctauReweighting and dataset_category=="signal":
        for w in weighted_events_reports[c]:
            print("{} weighted events: {}".format(w,weighted_events_reports[c][w]))

print("--- Analysis completed in {} seconds ---".format(time.time() - start_time))


