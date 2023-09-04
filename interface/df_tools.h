#include <ROOT/RDataFrame.hxx>
#include <ROOT/RVec.hxx>
#include <TLorentzVector.h>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

using namespace ROOT;
using namespace ROOT::VecOps;
namespace pt = boost::property_tree;
typedef pt::ptree::path_type path;

size_t get_best_cand_idx(RVec<double> var)
{
  float x_best = -99999.;
  size_t idx_best = 9999;

  //select best candidate based on cand_var
  for (unsigned i=0; i<var.size(); ++i)
  {
    float x = var.at(i);
    if(x>x_best)
    {
      x_best = x;
      idx_best = i;
    }
  }

  return idx_best;
}

size_t get_maxRatio_wPosCond_cand_idx(RVec<double> cand_var1, RVec<double> cand_var2, RVec<double> cand_var3)
{
  float ratio_best = -99999.;
  size_t idx_best = 9999;
  //size_t idx_best = 0;

  //select best candidate based on cand_var
  for (unsigned i=0; i<cand_var1.size(); ++i)
  {
    if(cand_var3.at(i)<0) continue;
    float ratio = cand_var1.at(i)/cand_var2.at(i);
    if(ratio>ratio_best)
    {
      ratio_best = ratio;
      idx_best = i;
    }
  }

  //if no candidates with positive condition has been found,
  //the condition is released
  if (idx_best==9999)
  {
    for (unsigned i=0; i<cand_var1.size(); ++i)
    {
      float ratio = cand_var1.at(i)/cand_var2.at(i);
      if(ratio>ratio_best)
      {
        ratio_best = ratio;
        idx_best = i;
      }
    }
  }

  return idx_best;
}

size_t get_maxProd_cand_idx(RVec<double> cand_var1, RVec<double> cand_var2)
{
  float prod_best = -99999.;
  size_t idx_best = 9999;
  //size_t idx_best = 0;

  //select best candidate based on cand_var
  for (unsigned i=0; i<cand_var1.size(); ++i)
  {
    float prod = cand_var1.at(i)*cand_var2.at(i);
    if(prod>prod_best)
    {
      prod_best = prod;
      idx_best = i;
    }
  }

  return idx_best;
}

size_t get_DsCompatible_cand_idx(RVec<double> mass)
{
  float res_best = 99999.;
  size_t idx_best = 9999;
  //size_t idx_best = 0;

  //select best candidate based on cand_var
  for (unsigned i=0; i<mass.size(); ++i)
  {
    float res = std::abs(mass.at(i)-1.96835);
    if(res<res_best)
    {
      res_best = res;
      idx_best = i;
    }
  }

  return idx_best;
}

RVec<short> get_mu_trigger_matching(RVec<short> trigger_match, RVec<unsigned> trig_mu_idx, RVec<unsigned> mu_idx)
{
  RVec<short> mu_has_matched_trigger(mu_idx.size());

  for(unsigned i_trigMatch=0; i_trigMatch<trigger_match.size(); ++i_trigMatch){

    if (!(trigger_match.at(i_trigMatch)>0)) continue;

    for(unsigned i_mu=0; i_mu<mu_idx.size(); ++i_mu){
      if (mu_idx[i_mu] == trig_mu_idx[i_trigMatch])
        mu_has_matched_trigger[i_mu] = 1;
    }
  }

  return mu_has_matched_trigger;
}

RVec<short> get_mu_trigger_matching(RVec<short> trigger_match, RVec<float> trigger_dr, RVec<unsigned> trig_mu_idx, RVec<unsigned> mu_idx)
{
  RVec<short> mu_has_matched_trigger(mu_idx.size());

  for(unsigned i_trigMatch=0; i_trigMatch<trigger_match.size(); ++i_trigMatch){

    if (!(trigger_match.at(i_trigMatch)>0)) continue;
    if (trigger_dr.at(i_trigMatch)>0.005) continue;

    for(unsigned i_mu=0; i_mu<mu_idx.size(); ++i_mu){
      if (mu_idx[i_mu] == trig_mu_idx[i_trigMatch])
        mu_has_matched_trigger[i_mu] = 1;
    }
  }

  return mu_has_matched_trigger;
}

RVec<float> get_mu_trigger_dr(RVec<short> trigger_match, RVec<float> trigger_dr, RVec<unsigned> trig_mu_idx, RVec<unsigned> mu_idx)
{
  RVec<float> muTrig_dr(mu_idx.size(),9999.);

  for(unsigned i_trigMatch=0; i_trigMatch<trigger_match.size(); ++i_trigMatch){

    if (!(trigger_match.at(i_trigMatch)>0)) continue;

    for(unsigned i_mu=0; i_mu<mu_idx.size(); ++i_mu){
      if (mu_idx[i_mu] == trig_mu_idx[i_trigMatch])
        muTrig_dr[i_mu] = trigger_dr[i_trigMatch];
    }
  }

  return muTrig_dr;
}

RVec<float> get_mu_trigger_dptopt(RVec<short> trigger_match, RVec<float> trigger_pt, RVec<unsigned> trig_mu_idx, RVec<float> mu_pt, RVec<unsigned> mu_idx)
{
  RVec<float> muTrig_dptopt(mu_idx.size(),9999.);

  for(unsigned i_trigMatch=0; i_trigMatch<trigger_match.size(); ++i_trigMatch){

    if (!(trigger_match.at(i_trigMatch)>0)) continue;

    for(unsigned i_mu=0; i_mu<mu_idx.size(); ++i_mu){
      if (mu_idx[i_mu] == trig_mu_idx[i_trigMatch])
      {
        float dpt = std::abs(trigger_pt[i_trigMatch] - mu_pt[i_mu]);
        float pt = mu_pt[i_mu];
        muTrig_dptopt[i_mu] = dpt/pt;
      }
    }
  }

  return muTrig_dptopt;
}

RVec<float> get_2D_binContent(TH2D* h, RVec<float> xv, RVec<float> yv, float xlim, float ylim)
{
  RVec<float> bin_content(xv.size());

  for(unsigned i=0; i<xv.size(); ++i){
    float x = xv[i];
    float y = yv[i];
    float bc = h->GetBinContent(h->FindBin(x>xlim?xlim:x,y>ylim?ylim:y));
    bin_content[i] = bc;
  }

  return bin_content;
}

RVec<float> get_1D_binContent(TH1D* h, RVec<float> xv)
{
  RVec<float> bin_content(xv.size());

  for(unsigned i=0; i<xv.size(); ++i){
    float x = xv[i];
    float bc = h->GetBinContent(h->FindBin(x));
    bin_content[i] = bc;
  }

  return bin_content;
}

float get_mu_id_sf(pt::ptree cfg, double pt, double eta, double mult=0.)
{
  std::string key = "NUM_SoftID_DEN_TrackerMuons|abseta_pt";
  float sf = 1.;
  for(auto veta : cfg.get_child(path(key,'|')))
  {
    std::string e = veta.first.data();
    std::string el = e.substr(e.find("[")+1,e.find(",")-e.find("[")-1);
    std::string eh = e.substr(e.find(",")+1,e.find("]")-e.find(",")-1);
    float eta_low  = std::stof(el);
    float eta_high = std::stof(eh);
    if (std::abs(eta)<eta_low || std::abs(eta)>eta_high) continue;
    key = key+"|"+e;
    for(auto vpt : cfg.get_child(path(key,'|')))
    {
      std::string p = vpt.first.data();
      std::string pl = p.substr(p.find("[")+1,p.find(",")-p.find("[")-1);
      std::string ph = p.substr(p.find(",")+1,p.find("]")-p.find(",")-1);
      float pt_low  = std::stof(pl);
      float pt_high = std::stof(ph);
      if (pt<pt_low || pt>pt_high) continue;
      key = key+"|"+p;
      sf = cfg.get<float>(path(key+"|value",'|'));
      float err = cfg.get<float>(path(key+"|error",'|'));
      sf = sf + mult*err;
    }

  }
  return sf;
}

float get_mu_reco_sf(pt::ptree cfg, double pt, double eta, double mult=0.)
{
  std::string key = "NUM_TrackerMuons_DEN_genTracks|abseta_pt";
  float sf = 1.;
  for(auto veta : cfg.get_child(path(key,'|')))
  {
    std::string e = veta.first.data();
    std::string el = e.substr(e.find("[")+1,e.find(",")-e.find("[")-1);
    std::string eh = e.substr(e.find(",")+1,e.find("]")-e.find(",")-1);
    float eta_low  = std::stof(el);
    float eta_high = std::stof(eh);
    if (std::abs(eta)<eta_low || std::abs(eta)>eta_high) continue;
    key = key+"|"+e;
    for(auto vpt : cfg.get_child(path(key,'|')))
    {
      std::string p = vpt.first.data();
      std::string pl = p.substr(p.find("[")+1,p.find(",")-p.find("[")-1);
      std::string ph = p.substr(p.find(",")+1,p.find("]")-p.find(",")-1);
      float pt_low  = std::stof(pl);
      float pt_high = std::stof(ph);
      if (pt<pt_low || pt>pt_high) continue;
      key = key+"|"+p;
      sf = cfg.get<float>(path(key+"|value",'|'));
      float err = cfg.get<float>(path(key+"|error",'|'));
      sf = sf + mult*err;
    }

  }
  return sf;
}

RVec<float> vget_mu_reco_sf(pt::ptree cfg, RVec<float> vpt, RVec<float> veta, float mult=0.){
  RVec<float> sf(vpt.size());
  for(unsigned i=0; i<vpt.size(); ++i){
    float pt = vpt[i];
    float eta = veta[i];
    sf[i] = get_mu_reco_sf(cfg,pt,eta,mult);
  }
  return sf;
}

RVec<float> vget_mu_id_sf(pt::ptree cfg, RVec<float> vpt, RVec<float> veta, float mult=0.){
  RVec<float> sf(vpt.size());
  for(unsigned i=0; i<vpt.size(); ++i){
    float pt = vpt[i];
    float eta = veta[i];
    sf[i] = get_mu_id_sf(cfg,pt,eta,mult);
  }
  return sf;
}

float compute_total_sf(float eff_data_1, float eff_mc_1, short match_1, float eff_data_2, float eff_mc_2, short match_2)
{
  float eff_data = eff_data_1+eff_data_2-eff_data_1*eff_data_2;
  float eff_mc = eff_mc_1+eff_mc_2-eff_mc_1*eff_mc_2;
  float total_sf = eff_data/eff_mc;

  return total_sf;
}

RVec<float> vcompute_total_sf(RVec<float> eff_data_1, RVec<float> eff_mc_1, RVec<short> match_1, RVec<float> eff_data_2, RVec<float> eff_mc_2, RVec<short> match_2){
  RVec<float> total_sf(eff_data_1.size());
  for(unsigned i=0; i<eff_data_1.size(); ++i){
    float ed1 = eff_data_1[i];
    float emc1 = eff_mc_1[i];
    short m1 = match_1[i];
    float ed2 = eff_data_2[i];
    float emc2 = eff_mc_2[i];
    short m2 = match_2[i];
    total_sf[i] = compute_total_sf(ed1,emc1,m1,ed2,emc2,m2);
  }
  return total_sf;
}

RVec<float> xdotv(float x, RVec<float> v){
  RVec<float> ov(v.size());
  for(unsigned i=0; i<v.size(); ++i){
    ov[i] = v[i]*x;
  }
  return ov;
}

RVec<float> vdotv(RVec<float> v1, RVec<float> v2){
  if(v1.size()!=v2.size()) 
  {
    std::cout<<"Vectors must have the same size!"<<std::endl;
    std::cout<<"Exiting..."<<std::endl;
    exit(1);
  }
  RVec<float> ov(v1.size());
  for(unsigned i=0; i<v1.size(); ++i){
    ov[i] = v1[i]*v2[i];
  }
  return ov;
}

//this is to get candidate invariant mass with different 
//final state particle mass ipothesis
RVec<Double_t> get_PxPyPzE_newM_2PartInvMass(RVec<Double_t> px_1, RVec<Double_t> py_1, RVec<Double_t> pz_1, RVec<Double_t> E_1, Double_t m_1,
		                                 RVec<Double_t> px_2, RVec<Double_t> py_2, RVec<Double_t> pz_2, RVec<Double_t> E_2, Double_t m_2)
{
  size_t n_cand = px_1.size();
  RVec<Double_t> cand_inv_mass(n_cand,0.);

  for(unsigned i=0; i<n_cand; ++i)
  {
    TLorentzVector p4_1;
    TLorentzVector p4_2;
    p4_1.SetPxPyPzE(px_1[i],py_1[i],pz_1[i],E_1[i]);
    p4_2.SetPxPyPzE(px_2[i],py_2[i],pz_2[i],E_2[i]);
    TLorentzVector p4_newM_1;
    TLorentzVector p4_newM_2;
    p4_newM_1.SetPtEtaPhiM(p4_1.Pt(),p4_1.Eta(),p4_1.Phi(),m_1);
    p4_newM_2.SetPtEtaPhiM(p4_2.Pt(),p4_2.Eta(),p4_2.Phi(),m_2);
    cand_inv_mass[i]=(p4_newM_1+p4_newM_2).M();
  }

  return cand_inv_mass;

}

RVec<Double_t> get_PtEtaPhi_newM_3PartInvMass(RVec<Double_t> pt_1, RVec<Double_t> eta_1, RVec<Double_t> phi_1, Double_t newM_1,
		                              RVec<Double_t> pt_2, RVec<Double_t> eta_2, RVec<Double_t> phi_2, Double_t newM_2,
		                              RVec<Double_t> pt_3, RVec<Double_t> eta_3, RVec<Double_t> phi_3, Double_t newM_3)
{
  size_t n_cand = pt_1.size();
  RVec<Double_t> cand_inv_mass(n_cand,0.);

  for(unsigned i=0; i<n_cand; ++i)
  {
    TLorentzVector p4_1;
    TLorentzVector p4_2;
    TLorentzVector p4_3;
    p4_1.SetPtEtaPhiM(pt_1[i],eta_1[i],phi_1[i],newM_1);
    p4_2.SetPtEtaPhiM(pt_2[i],eta_2[i],phi_2[i],newM_2);
    p4_3.SetPtEtaPhiM(pt_3[i],eta_3[i],phi_3[i],newM_3);
    cand_inv_mass[i]=(p4_1+p4_2+p4_3).M();
  }

  return cand_inv_mass;

}

RVec<Double_t> get_PtEtaPhi_newM_2PartInvMass(RVec<Double_t> pt_1, RVec<Double_t> eta_1, RVec<Double_t> phi_1, Double_t newM_1,
		                              RVec<Double_t> pt_2, RVec<Double_t> eta_2, RVec<Double_t> phi_2, Double_t newM_2)
{
  size_t n_cand = pt_1.size();
  RVec<Double_t> cand_inv_mass(n_cand,0.);

  for(unsigned i=0; i<n_cand; ++i)
  {
    TLorentzVector p4_1;
    TLorentzVector p4_2;
    p4_1.SetPtEtaPhiM(pt_1[i],eta_1[i],phi_1[i],newM_1);
    p4_2.SetPtEtaPhiM(pt_2[i],eta_2[i],phi_2[i],newM_2);
    cand_inv_mass[i]=(p4_1+p4_2).M();
  }

  return cand_inv_mass;

}

RVec<Double_t> get_PxPyPzE_newM_3PartInvMass(RVec<Double_t> px_1, RVec<Double_t> py_1, RVec<Double_t> pz_1, RVec<Double_t> E_1, Double_t m_1,
		                                 RVec<Double_t> px_2, RVec<Double_t> py_2, RVec<Double_t> pz_2, RVec<Double_t> E_2, Double_t m_2,
		                                 RVec<Double_t> px_3, RVec<Double_t> py_3, RVec<Double_t> pz_3, RVec<Double_t> E_3, Double_t m_3)
{
  size_t n_cand = px_1.size();
  RVec<Double_t> cand_inv_mass(n_cand,0.);

  for(unsigned i=0; i<n_cand; ++i)
  {
    TLorentzVector p4_1;
    TLorentzVector p4_2;
    TLorentzVector p4_3;
    p4_1.SetPxPyPzE(px_1[i],py_1[i],pz_1[i],E_1[i]);
    p4_2.SetPxPyPzE(px_2[i],py_2[i],pz_2[i],E_2[i]);
    p4_3.SetPxPyPzE(px_3[i],py_3[i],pz_3[i],E_3[i]);
    TLorentzVector p4_newM_1;
    TLorentzVector p4_newM_2;
    TLorentzVector p4_newM_3;
    p4_newM_1.SetPtEtaPhiM(p4_1.Pt(),p4_1.Eta(),p4_1.Phi(),m_1);
    p4_newM_2.SetPtEtaPhiM(p4_2.Pt(),p4_2.Eta(),p4_2.Phi(),m_2);
    p4_newM_3.SetPtEtaPhiM(p4_3.Pt(),p4_3.Eta(),p4_3.Phi(),m_3);
    cand_inv_mass[i]=(p4_newM_1+p4_newM_2+p4_newM_3).M();
  }

  return cand_inv_mass;

}

RVec<Double_t> get_PxPyPzE_HnlInvariantMass(RVec<Double_t> px_1, RVec<Double_t> py_1, RVec<Double_t> pz_1, RVec<Double_t> E_1,
		                            RVec<Double_t> px_2, RVec<Double_t> py_2, RVec<Double_t> pz_2, RVec<Double_t> E_2)
{
  size_t n_cand = px_1.size();
  RVec<Double_t> cand_inv_mass(n_cand,0.);

  for(unsigned i=0; i<n_cand; ++i)
  {
    TLorentzVector p4_1;
    TLorentzVector p4_2;
    p4_1.SetPxPyPzE(px_1[i],py_1[i],pz_1[i],E_1[i]);
    p4_2.SetPxPyPzE(px_2[i],py_2[i],pz_2[i],E_2[i]);
    cand_inv_mass[i]=(p4_1+p4_2).M();
  }

  return cand_inv_mass;

}

//Custom filter
RVec<short> CustomFilter(RVec<unsigned> mu1_idx, RVec<unsigned> mu2_idx, RVec<unsigned> pi_idx)
{
  RVec<short> cand_has_multiple_permutations(mu1_idx.size(),0);

  for (unsigned i=0; i<mu1_idx.size(); ++i)
  {
    for (unsigned j=+1; j<mu1_idx.size(); ++j)
    {
      if (mu1_idx.at(i) == mu2_idx.at(j) && mu2_idx.at(i) == mu1_idx.at(j) && pi_idx.at(i) == pi_idx.at(j))
      {
        cand_has_multiple_permutations[i] = 1;
        cand_has_multiple_permutations[j] = 1;
      }
    }
  }

  return cand_has_multiple_permutations;

}

RVec<std::string> get_lxy_categories(RVec<float> hnl_lxy, RVec<int> mu_hnl_charge, RVec<int> mu_ds_charge)
{
  RVec<std::string> categories(hnl_lxy.size());

  for(unsigned i=0; i<categories.size(); ++i){
    float lxy = hnl_lxy.at(i);
    int q1  = mu_hnl_charge.at(i);
    int q2  = mu_ds_charge.at(i);

    std::string cat;

    if (q1*q2>0){
      if(lxy<1) cat = "SSlxy0to1";
      else if(lxy>1 && lxy<5) cat = "SSlxy1to5";
      else if(lxy>5) cat = "SSlxy5toInf";
    }
    else if (q1*q2<0){
      if(lxy<1) cat = "OSlxy0to1";
      else if(lxy>1 && lxy<5) cat = "OSlxy1to5";
      else if(lxy>5) cat = "OSlxy5toInf";
    }
    categories[i] = cat;
  }

  return categories;
}

std::string get_lxy_categories(float hnl_lxy, int mu_hnl_charge, int mu_ds_charge)
{
  std::string cat;
  float lxy = hnl_lxy;
  float q1  = mu_hnl_charge;
  float q2  = mu_ds_charge;

  if (q1*q2>0){
    if(lxy<1) cat = "SSlxy0to1";
    else if(lxy>1 && lxy<5) cat = "SSlxy1to5";
    else if(lxy>5) cat = "SSlxy5toInf";
  }
  else if (q1*q2<0){
    if(lxy<1) cat = "OSlxy0to1";
    else if(lxy>1 && lxy<5) cat = "OSlxy1to5";
    else if(lxy>5) cat = "OSlxy5toInf";
  }

  return cat;
}

std::string get_lxys_categories(float hnl_lxys, int mu_hnl_charge, int mu_ds_charge)
{
  std::string cat;
  float lxys = hnl_lxys;
  float q1  = mu_hnl_charge;
  float q2  = mu_ds_charge;

  if (q1*q2>0){
    if(lxys<50) cat = "SSlxys0to50";
    else if(lxys>50 && lxys<150) cat = "SSlxys50to150";
    else if(lxys>150) cat = "SSlxys150toInf";
  }
  else if (q1*q2<0){
      if(lxys<50) cat = "OSlxys0to50";
      else if(lxys>50 && lxys<150) cat = "OSlxys50to150";
      else if(lxys>150) cat = "OSlxys150toInf";

  }

  return cat;
}

std::string get_inclusive_categories()
{
  return "inclusive";
}

//for debugging
RVec<short> get_multiple_permutations(RVec<unsigned> mu1_idx, RVec<unsigned> mu2_idx, RVec<unsigned> pi_idx)
{
  RVec<short> cand_has_multiple_permutations(mu1_idx.size(),0);

  for (unsigned i=0; i<mu1_idx.size(); ++i)
  {
    for (unsigned j=+1; j<mu1_idx.size(); ++j)
    {
      if (mu1_idx.at(i) == mu2_idx.at(j) && mu2_idx.at(i) == mu1_idx.at(j) && pi_idx.at(i) == pi_idx.at(j))
      {
        cand_has_multiple_permutations[i] = 1;
        cand_has_multiple_permutations[j] = 1;
      }
    }
  }

  return cand_has_multiple_permutations;

}

int get_cand_multiplicity(RVec<float> var){
  int mult;
  mult = var.size();
  return mult;
}