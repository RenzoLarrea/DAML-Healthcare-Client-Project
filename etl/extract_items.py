import json
import csv
import pandas as pd

df = pd.read_csv('/item_raw.csv')

extracted_data = []

for idx, row in df.iterrows():
    item_json_str = row['item']
    
    try:
        items = json.loads(item_json_str)
    except:
        print(f"Error parsing row {idx}")
        continue
    
    for item in items:
        record = {}
        
        record['sequence'] = item.get('sequence', '')
        record['service_date'] = item.get('servicedDate', '')
        record['quantity'] = item.get('quantity', {}).get('value', '')
        
        product_coding = item.get('productOrService', {}).get('coding', [])
        if product_coding:
            record['ndc_code'] = product_coding[0].get('code', '')
            record['product_display'] = product_coding[0].get('display', '')
        else:
            record['ndc_code'] = ''
            record['product_display'] = ''
        
        adjudications = item.get('adjudication', [])
        
        record['benefit_amount'] = ''
        record['coinsurance_below_threshold'] = ''
        record['coinsurance_above_threshold'] = ''
        record['patient_paid'] = ''
        record['other_troop'] = ''
        record['low_income_subsidy'] = ''
        record['prior_payer_paid'] = ''
        record['total_drug_cost'] = ''
        record['gap_discount'] = ''
        
        for adj in adjudications:
            category_codings = adj.get('category', {}).get('coding', [])
            amount_value = adj.get('amount', {}).get('value', '')
            
            for coding in category_codings:
                code = coding.get('code', '')    
            
                if 'cvrd_d_plan_pd_amt' in code:
                    record['benefit_amount'] = amount_value
                
                elif 'gdc_blw_oopt_amt' in code:
                    record['coinsurance_below_threshold'] = amount_value
                
                elif 'gdc_abv_oopt_amt' in code:
                    record['coinsurance_above_threshold'] = amount_value
                
                elif 'ptnt_pay_amt' in code:
                    record['patient_paid'] = amount_value
                
                elif 'othr_troop_amt' in code:
                    record['other_troop'] = amount_value
                
                elif 'lics_amt' in code:
                    record['low_income_subsidy'] = amount_value
                
                elif 'plro_amt' in code:
                    record['prior_payer_paid'] = amount_value
                
                elif 'tot_rx_cst_amt' in code:
                    record['total_drug_cost'] = amount_value
                
                elif 'rptd_gap_dscnt_num' in code:
                    record['gap_discount'] = amount_value
        
        extracted_data.append(record)

output_df = pd.DataFrame(extracted_data)

column_order = [
    'sequence',
    'service_date',
    'ndc_code',
    'product_display',
    'quantity',
    'total_drug_cost',
    'benefit_amount',
    'patient_paid',
    'prior_payer_paid',
    'low_income_subsidy',
    'coinsurance_below_threshold',
    'coinsurance_above_threshold',
    'other_troop',
    'gap_discount'
]

output_df = output_df[column_order]

output_df.to_csv('/mnt/user-data/outputs/item_extracted.csv', index=False)

print(f"Successfully extracted {len(extracted_data)} item records")
print(f"\nColumns in output:")
for i, col in enumerate(column_order, 1):
    print(f"  {i}. {col}")
print(f"\nFirst few rows:")
print(output_df.head(3).to_string())
s