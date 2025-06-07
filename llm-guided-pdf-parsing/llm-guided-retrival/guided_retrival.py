from evaluate_chunks import evaluate_chunks, evaluate_chunks_with_step

# Example chunks, to be updated with the actual retrival
query = "How many studies in total met inclusion criteria for analysis of FEV 1?"
# 1, 10 ,15
chunks = {
    "DjkRDVj9YwkfmxAFfgXgJ":
        "Despite reduced resting lung volumes and D LCO , patients with long COVID and dyspnoea have similar physiological response to exercise to healthy subjects. D LCO impairment can marginally explain heterogeneity of complex syndromes such as long COVID. https://bit.ly/40j4aX6",
    "e7d6C_gDSEQt5fb6rj8Nt":
        "Studies were identified using the systematic review methods described previously by KOTECHA et al. [4, 13] which followed the Preferred Reporting Items for Systematic Reviews and Meta-analysis (PRISMA) guidelines [14] . Briefly, 86 studies in total met inclusion criteria for analysis of FEV 1 . Although this systematic review was designed to capture studies to answer questions specifically related to FEV 1 , the search criteria were subsequently deemed acceptable to capture appropriately other spirometry measures including FVC, FEV 1 /FVC ratio and FEF 25-75 . Studies were included for this analysis if they fulfilled the following criteria: 1) FEV 1 /FVC reported in survivors of preterm birth (with or without BPD) and those born healthy at term; or if 2) FEV 1 /FVC were reported separately in survivors of preterm birth with and without BPD.",
    "Fv2taY_bTmimdP-LE8ldk":
        "Publication bias was observed when comparing Preterm (All) with Term groups subjectively with an asymmetrical distribution noted on funnel plots, and objectively with Egger's test reaching significance ( p<0.01). When preterm groups were separated into those with and without BPD, however, a symmetrical distribution was noted on all funnel plots and Egger's test did not reach significance (supplementary figure S1 ). This could imply that asymmetry seen in the combined preterm group may be due to the heterogeneity of having two different disease populations defined by the presence or absence of BPD."
}
outline = {
    "DjkRDVj9YwkfmxAFfgXgJ":
        "0: Abstract\n1: ",
    "e7d6C_gDSEQt5fb6rj8Nt":
        "0: Abstract\n1: Introduction\n10: Meta-regression\n11: Discussion\n12: \n2: Research questions\n3: Study identification and selection\n4: Publication bias and study quality\n5: Data collection\n6: Data analysis\n7: Study selection and study quality\n8: Publication bias\n9: Meta-analysis",
    "Fv2taY_bTmimdP-LE8ldk":
        "0: Abstract\n1: Introduction\n10: Meta-regression\n11: Discussion\n12: \n2: Research questions\n3: Study identification and selection\n4: Publication bias and study quality\n5: Data collection\n6: Data analysis\n7: Study selection and study quality\n8: Publication bias\n9: Meta-analysis"
}

chunks_to_keep = []
is_visited = {}

for chunk_id in chunks:
  chunks = evaluate_chunks_with_step(query, chunk_id, 0, chunks_to_keep, is_visited)
  if chunks is not None:
    chunks_to_keep.append(chunks)

print(chunks_to_keep)
