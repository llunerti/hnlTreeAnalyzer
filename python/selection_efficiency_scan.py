import ROOT
import sys
import math
import os
import json
import time
import subprocess
import numpy
import argparse
start_time = time.time()

#script input arguments
parser = argparse.ArgumentParser(description="")
parser.add_argument("cfg_filename"         ,                                     help="Path to the input configuration file")
parser.add_argument("--datasetsToProcess"  , nargs='*'          , default=[]   , help="Short name of the input sample to process")
parser.add_argument("--nThreads"           , type=int           , default=1    , help="Number of threads")
parser.add_argument("--nCuts"              , type=int           , default=20   , help="Number of cuts for the efficiency scan")
parser.add_argument('--optVariables'       , nargs='*'          , default=[]   , help='List of variables to cut on before running the optimization. The scan will not be performed for these variables')
args = parser.parse_args()

ROOT.EnableThreadSafety()
ROOT.EnableImplicitMT(args.nThreads)

configFileName      = args.cfg_filename
datasets_to_process = args.datasetsToProcess
opt_var_list = args.optVariables

#open analyzer configuration file
with open(configFileName, "r") as f:
    config = json.loads(f.read())

#get ntuples configuration
with open(config["ntuples_cfg_file_full_path"], "r") as f:
    ntuples = json.loads(f.read())

#get selection and categorization cuts
with open(config["selection_cfg_file_full_path"], "r") as f:
    selection = json.loads(f.read())

# if it's true assign a label to the output file
isasignalsample = False
mass_label = str()
ctau_label = str()
if len(datasets_to_process)==1 and ntuples[datasets_to_process[0]]["dataset_category"]=="signal":
    isasignalsample = True
    ss = datasets_to_process[0]
    mass_label = ss[ss.find("mN")+2:ss.find("mN")+5]
    ctau_label = ss[ss.find("ctau")+4:ss.rfind("p0")+2]

input_file_list = "final_file_name_list_noOpt"
tree_name = "final_tree"

#get input files
dataset_category = ntuples[datasets_to_process[0]]["dataset_category"]
inputFileName_list = []
for dataset_to_process in datasets_to_process:
    inputFileName_list += ntuples[dataset_to_process][input_file_list]
    if ntuples[dataset_to_process]["dataset_category"] != dataset_category:
        print("Please provide input samples of the same category (e.g. all background samples)")
        exit(0)
 
n_cuts = args.nCuts
reports = {}


#build a dictionary that groups all the files
#of the same catogory togheter
input_file_catOrdered = {}
for cat in selection["categories"]:
    file_list = []
    for inputFileName in inputFileName_list:
        file_cat_label = inputFileName.split("/")[-1].split(".")[0].split("_")[-1]
        if file_cat_label == cat["label"]:
            file_list.append(inputFileName)
    input_file_catOrdered[cat["label"]] = file_list

print(input_file_catOrdered)



for cat in selection["categories"]:

    print("*** {} category".format(cat["label"]))

    if cat["label"] == "inclusive":
        continue

    chain = ROOT.TChain(tree_name)
    
    for inputFileName in input_file_catOrdered[cat["label"]]:
        print("Adding {} to the chain...".format(inputFileName))
        success = chain.Add(inputFileName)

    print("\n")
    print("{} total entries ...".format(chain.GetEntries()))
    
    outputDirName = os.path.join(config["output_dir_name"])
    subprocess.call(['mkdir','-p',outputDirName])

    df = ROOT.RDataFrame(chain)

    #get mc truth in case of signal sample
    if dataset_category == "signal":
        for sel in selection["gen_matching_cuts"]:
            df = df.Filter(sel["cut"],sel["printout"])

    df = df.Redefine("tot_weight","tot_weight/mc_weight")
    tot_events = df.Sum("tot_weight").GetValue()

    
    string_match = lambda list_of_strings, target_string: any(s in target_string for s in list_of_strings)
    opt_cuts_list = [x["cut"] for x in cat["selection_cuts"] if string_match(opt_var_list,x["cut"])]
    for opt_cut in opt_cuts_list:
        df = df.Filter(opt_cut)

    #build signal selection efficiency csv file 
    var_header = "entry"
    fmt = "%d"
    df_array = numpy.arange(n_cuts)
    for var in selection["selection_eff_scan"]:
        if string_match(opt_var_list,var["name"]):
            continue
        a_cuts   = numpy.array(numpy.linspace(var["low_edge"],var["up_edge"],n_cuts))
        pass_events = [df.Filter(str(var["loperand"])+var["logic"]+str(cut)).Sum("tot_weight").GetValue() for cut in a_cuts]
        fail_events = [tot_events-float(df.Filter(str(var["loperand"])+var["logic"]+str(cut)).Sum("tot_weight").GetValue()) for cut in a_cuts]
        a_pass = numpy.array(pass_events)
        a_fail = numpy.array(fail_events)
        var_header += ","+var["name"]+"_cut,"+var["name"]+"_pass,"+var["name"]+"_fail"
        fmt += ", %.2f, %.1f, %.1f"
        df_array = numpy.column_stack((df_array,a_cuts,a_pass,a_fail))
    output_path = os.path.join(outputDirName,"sel_eff_tree_"+dataset_category+"_"+"-".join(opt_var_list)+"_"+cat["label"]+".csv")
    if isasignalsample:
        output_path = os.path.join(outputDirName,"sel_eff_tree_"+dataset_category+"_mN{}_ctau{}_{}".format(mass_label,ctau_label,"-".join(opt_var_list))+"_"+cat["label"]+".csv")
    numpy.savetxt(output_path, df_array, delimiter=',', header=var_header, comments='',fmt=fmt)
    print("Output saved in {}".format(output_path))

print("--- Analysis completed in {} seconds ---".format(time.time() - start_time))

