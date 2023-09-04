import subprocess
import sys
import os
import json
import argparse

#script input arguments
parser = argparse.ArgumentParser(description="")
parser.add_argument("cfg_filename"   , help="Path to the input configuration file")
parser.add_argument("--saveRatioPlot", action='store_true', default=False, help="Save data/MC ratio plots")
parser.add_argument("--tag"          , default="",type=str,help="add tag to output file name")
parser.add_argument("--intLumi"      , default="",type=str,help="Print integrated luminosity (1/fb)")
args = parser.parse_args()

configFileName = args.cfg_filename

with open(configFileName, "r") as f:
    config = json.loads(f.read())
import ROOT

inputDirName = str(config["inputDirName"])
outDirName = str(os.path.join(config["outDirName"],inputDirName.split("/")[-1])) 

inputMCFileName = os.path.join(inputDirName,"hadd_bkg.root")
if args.tag != "":
    inputMCFileName = os.path.join(inputDirName,"hadd_bkg_{}.root".format(args.tag))    
subprocess.call(["hadd","-f",inputMCFileName] + [os.path.join(inputDirName,bkgFileName) for bkgFileName in config["background"].keys()])

inputDataFileName = os.path.join(inputDirName,"hadd_data.root")
if args.tag != "":
    inputDataFileName = os.path.join(inputDirName,"hadd_data_{}.root".format(args.tag)) 
subprocess.call(["hadd","-f",inputDataFileName] + [os.path.join(inputDirName,dataFileName) for dataFileName in config["data"].keys()])

for plotName in config["plotNameList"]:

    ROOT.gROOT.SetBatch(ROOT.kTRUE)

    c = ROOT.TCanvas("c","c",900,800)

    padUpper = ROOT.TPad('padUpper', 'padUpper', 0, 0.3, 1, 1.0)
    padUpper.SetBottomMargin(0.01)
    padUpper.SetTopMargin(0.12)
    padUpper.Draw()
    
    padLower = ROOT.TPad('padLower', 'padLower', 0, 0.0, 1, 0.3)
    padLower.SetBottomMargin(0.35)
    padLower.SetTopMargin(0.12)
    padLower.SetGridy()
    padLower.Draw()
    
    histoName = str(plotName)
    iColor = 0
    iLineStyle=0
    leg_dataVSmc = ROOT.TLegend(0.65, 0.65, 0.87, 0.87)
    
    histoStacked = ROOT.THStack("histoStacked","histoStacked")
    
    keyList_bkg = [key for key in config["background"]]
    inputHistoList_bkg = [ROOT.TH1D() for key in config["background"]]
    inputFileList_bkg = [ROOT.TFile() for key in config["background"]]
    inputHistoDic_bkg = dict(zip(keyList_bkg,inputHistoList_bkg))
    inputFileDic_bkg  = dict(zip(keyList_bkg,inputFileList_bkg))

    xaxis_label = str()
    data_filename = list(config["data"].keys())[0]
    lumi_data = float(config["data"][data_filename]["integrated_lumi"]) #/pb
    tot_integral = float(0)

    for filename in config["background"]:
        inputFileDic_bkg [filename] = ROOT.TFile.Open(str(os.path.join(inputDirName,filename)))
        inputHistoDic_bkg[filename] = ROOT.TH1D(inputFileDic_bkg[filename].Get(histoName))
        #if filename.find("DsToPhiPi_ToMuMu")>0:
        #    BR_BToDs = 0.20509380
        #    BR_Ds = 1.3e-5
        #    prompt_fraction_in_mc = 0.0928
        #    nonprompt_fraction_in_mc = 0.0832
        #    if filename.find("_nonprompt.root")>0:
        #        weight = BR_Ds*BR_BToDs/nonprompt_fraction_in_mc
        #        print("--> nonprompt weight: {}".format(weight))
        #        inputHistoDic_bkg[filename].Scale(weight)
        #    elif filename.find("_prompt.root")>0:
        #        weight = BR_Ds/prompt_fraction_in_mc
        #        print("--> prompt weight: {}".format(weight))
        #        inputHistoDic_bkg[filename].Scale(weight)
        tot_integral += inputHistoDic_bkg[filename].Integral()

    #Stacking background histos
    #for filename in sorted(config["background"]):
    for filename in config["background"]:
        inputFileDic_bkg [filename] = ROOT.TFile.Open(str(os.path.join(inputDirName,filename)))
        inputHistoDic_bkg[filename] = ROOT.TH1D(inputFileDic_bkg[filename].Get(histoName))
        #inputHistoDic_bkg[filename].Scale(lumi_data)
        histo_integral = inputHistoDic_bkg[filename].Integral()

        #if filename.find("DsToPhiPi_ToMuMu")>0:
        #    BR_BToDs = 0.20509380
        #    BR_Ds = 1.3e-5
        #    prompt_fraction_in_mc = 0.0928
        #    nonprompt_fraction_in_mc = 0.0832
        #    if filename.find("_nonprompt.root")>0:
        #        weight = BR_Ds*BR_BToDs/nonprompt_fraction_in_mc
        #        print("--> nonprompt weight: {}".format(weight))
        #        inputHistoDic_bkg[filename].Scale(weight)
        #    elif filename.find("_prompt.root")>0:
        #        weight = BR_Ds/prompt_fraction_in_mc
        #        print("--> prompt weight: {}".format(weight))
        #        inputHistoDic_bkg[filename].Scale(weight)

        inputHistoDic_bkg[filename].Scale(1./tot_integral)
        inputHistoDic_bkg[filename].SetLineColor(ROOT.kBlack)
        inputHistoDic_bkg[filename].SetFillColor(ROOT.kGreen+iColor)
        #overflowBin = inputHistoDic_bkg[filename].GetXaxis().GetLast() + 1
        overflowBin = inputHistoDic_bkg[filename].GetXaxis().GetLast()
        inputHistoDic_bkg[filename].GetXaxis().SetRange(1,overflowBin)
        xaxis_label = str(inputHistoDic_bkg[filename].GetXaxis().GetTitle())
        histoStacked.Add(inputHistoDic_bkg[filename])
        iColor += 1
        bkgLabel = config["background"][filename]["label"]
        leg_dataVSmc.AddEntry(inputHistoDic_bkg[filename],bkgLabel)
    

    #Drawing stacked histos
    padUpper.cd()
    histoStacked.Draw("hist")
    histoStacked.SetTitle("")
    histoStacked.GetYaxis().SetTitle("Normalized to unit")
    histoStacked.GetYaxis().SetLabelSize(0.05)
    histoStacked.GetYaxis().SetTitleSize(0.06)
    histoStacked.GetYaxis().SetTitleOffset(0.8)
    histoStacked.GetXaxis().SetLabelSize(0)
    histoStacked.SetMinimum(0.)
    hmax = float(histoStacked.GetMaximum())
    histoStacked.SetMaximum(hmax+(hmax/2))
    #overflowBin = histoStacked.GetXaxis().GetLast() + 1
    overflowBin = histoStacked.GetXaxis().GetLast()
    histoStacked.GetXaxis().SetRange(1,overflowBin)
    padUpper.Update()
    
    #Superimposing data
    inputDataFile = ROOT.TFile.Open(str(inputDataFileName))
    inputDataHisto = ROOT.TH1D(inputDataFile.Get(histoName))
    inputDataHisto.Scale(1./inputDataHisto.Integral())
    padUpper.cd()
    inputDataHisto.SetLineColor(ROOT.kBlack)
    inputDataHisto.SetMarkerStyle(20)
    inputDataHisto.GetXaxis().SetRange(1,overflowBin)
    a_filename = list(config["data"].keys())[0]
    leg_dataVSmc.AddEntry(inputDataHisto,config["data"][a_filename]["label"])
    inputDataHisto.Draw("ex0p same")
    leg_dataVSmc.Draw("same")


    if not args.intLumi =="":
        latex = ROOT.TLatex()
        latex.SetTextAlign(12)
        latex.SetTextSize(0.04)
        latex.DrawLatexNDC(0.70,0.91,str(args.intLumi)+" fb^{-1} (13 TeV)")
    
    #latexpv = ROOT.TLatex()
    #latexpv.SetTextAlign(12)
    #latexpv.SetTextSize(0.04)
    #latexpv.DrawLatexNDC(0.1,0.91,"CMS Private Work")

    #inputMCFile = ROOT.TFile.Open(str(inputMCFileName))
    #inputMCHisto = ROOT.TH1D(inputMCFile.Get(histoName))
    #inputMCHisto.Scale(1./tot_integral)
    #inputMCHisto = histoStacked.GetHistogram()

    inputMCHisto = inputHistoDic_bkg[list(config["background"].keys())[0]].Clone()
    for i in range(1,len(list(config["background"].keys()))):
        inputMCHisto.Add(inputHistoDic_bkg[list(config["background"].keys())[i]])
    
    hRatio = inputDataHisto.Clone()
    hRatio.Divide(inputMCHisto)

    if args.saveRatioPlot:
        hRatio.SaveAs(os.path.join(outDirName,histoName +"_dataMCratio.root"))

    padLower.cd()
    hRatio.SetTitle("")
    hRatio.GetYaxis().SetTitle("")
    hRatio.SetLineColor(ROOT.kBlack)
    hRatio.SetLineWidth(1)
    hRatio.SetMarkerStyle(20)
    hRatio.GetYaxis().SetRangeUser(0.5,1.5)
    hRatio.GetYaxis().SetTitle("DATA/MC")
    hRatio.GetXaxis().SetTitle(inputMCHisto.GetXaxis().GetTitle())
    hRatio.GetXaxis().SetTitleSize(0.12)
    hRatio.GetYaxis().SetTitleSize(0.12)
    hRatio.GetYaxis().SetLabelSize(0.12)
    hRatio.GetXaxis().SetLabelSize(0.12)

    xtitle = inputMCHisto.GetXaxis().GetTitle()
    if xtitle.find("IPS")>0:
        xtitle = xtitle.replace("[cm] ","")
        hRatio.GetXaxis().SetTitle(xtitle)

    hRatio.GetXaxis().SetRange(1,overflowBin)
    hRatio.GetYaxis().SetTitleOffset(0.4)
    hRatio.GetYaxis().SetNdivisions(5)
    ROOT.gStyle.SetOptStat(0)
    hRatio.Draw()
    
    subprocess.call(["mkdir","-p",outDirName])

    outputfilename = histoName +"_dataVSmc"
    if not args.tag == "":
        outputfilename = histoName +"_"+args.tag+"_dataVSmc"
    c.SaveAs(os.path.join(outDirName,outputfilename +".png"))
    c.SaveAs(os.path.join(outDirName,outputfilename +".pdf"))
    c.SaveAs(os.path.join(outDirName,outputfilename +".root"))
    del c

