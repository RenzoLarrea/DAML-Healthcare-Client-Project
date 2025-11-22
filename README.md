# DAML-Healthcare-Client-Project
Workflow 
Goal - Retreve key EOB and pde data for one synthetic patient in CMS Blue Button and store in an accessible csv
1. extract_all pulls data from CMS Blue button using base url and bearer authorization token and stores it into a json.
2. transform converts json data retreived by extract_all into a csv while prioritizing pde information (eob_part_d_clean)
3. filter_csv drops negligible columns that are empty or have the exact same value for every entry (eob_part_d_clean => eob_part_d_clean_v2)
4a. Item column extraction -> Since item's entries are a json, extract_items_p1 + extract_items_p2 unpacks the csv column entries into a csv and 
recreates column labels (item_raw => item_extracted)
4b. Patient history -> (WIP) Using patient_ref variable, extract_patient_1 + extract_patient_p2 retrieve patient information from different base url
4c. Supporting Info - (WIP) 
5. merge_item merges unpacked item columns to create final csv (eob_part_d_clean_v2 + item_extracted.csv => eob_part_d_final)