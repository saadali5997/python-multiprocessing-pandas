"""
This program is a tool-kit kind of program which uses
- Pandas
- Fuzzy String Matching in Python
- Multiprocessing in Python

I have deleted many confidential information revealing statements from it and the code will most probably not run for this dataset.
The data pre-processing or handling part will change but the overall functioning will remain the same. Like multi processing, string matching etc.
"""
import pandas as pd
import csv
from fuzzywuzzy import fuzz
import numpy as np
import time
import multiprocessing as mp

address = """/home"""
rFile = pd.read_csv(address+'/tR.csv', encoding="ISO-8859-1", dtype=str)
imFile = pd.read_csv(address+'/tIM.csv', encoding="ISO-8859-1")

rFile = rFile.rename(columns = {'Firm Name':'FirmName','CRD #':'CRD','Person Account: Mailing Address Line1':'Address','Person Account: Email':'Email','Person Account: Mailing City':'City','Person Account: Mailing State/Province':'State','Person Account: Mailing Zip/Postal Code' : 'Zip'})
bsFile = imFile.rename(columns = {'add':'Address','ADVISOR_CITY':'City','ADVISOR_FIRM_NAME':'Firm Name','ADVISOR_CRD':'CRD'})

all_columns = list(rFile)
rFile[all_columns] = rFile[all_columns].astype(str)

all_columns = list(imFile)
imFile[all_columns] = imFile[all_columns].astype(str)

print(rFile.columns)
print(imFile.columns)

# Here I check various fuzzy match scores and decide by their mean if two strings should be given the status of match or not.
def fuzzymatch(str1, str2, ratio):
    scores = []
    scores.append(fuzz.partial_ratio(str1, str2) > ratio)
    scores.append(fuzz.token_sort_ratio(str1, str2) > ratio)
    scores.append(fuzz.token_set_ratio(str1, str2) > ratio)
    if (str1 in str2) or (str2 in str1):
        return True
    else:
        scores.append(False)
    t_count = 0
    f_count = 0
    
    for i in scores:
        if i:
            t_count+=1
        else:
            f_count+=1
    return t_count >= f_count

columns_newdf = ['tim_id', 'tr_id', 'name', 'name_match', 'address', 'address_match', 'email', 'email_match', 'telephone', 'crd', 'crd_match']

noOfCores = mp.cpu_count()
partition_by = int(len(rFile)/noOfCores)

"""
Why am I using multi-processing?

Well the dataframe had so many rows and for the use case I was trying to do, there was no other option but to run the two dataframes
in nested loop and compare each row of one with two. Simple for loop was using only one core and for more than 0.2 million rows in each, it was 
taking so much time. That's why I divided the rows to subprocesses and compiled the returned dataframes in later stage,
"""

# Each thread will retrun a dataframe
def checkMatches(args):
    global rFile
    global imFile
    start, end = args
    df = pd.DataFrame(columns=columns_newdf)
    rdf = rFile.iloc[start:end]
    for index, row in rdf.iterrows():
        for i,r in imFile.iterrows():
            tim_id = r['index']
            tr_id = index

            fname_match = fuzzymatch(row['FirmName'],r['FirmName'], 90) and row['FirmName'] != "nan"
            fname = row['FirmName']

            address = row['Address']
            address_match = fuzzymatch(row['Address'],r['Address'], 60) and row['Address'] != "nan"

            email_match = (row['Email'] == r['Email']) and row['Email'] != "nan"
            email = row['Email']

            crd_match = (row['CRD'] == r['CRD'])  and row['CRD'] != "nan"
            crd = row['CRD']

            telephone = False
            if fname_match or address_match or email_match or crd_match:
                temp = pd.DataFrame([[tim_id, tr_id, str(fname), str(fname_match), str(address), str(address_match), str(email), str(email_match), str(telephone), str(crd), str(crd_match)]], columns=columns_newdf)
                df = df.append(temp, ignore_index=True)
    return df

st = time.time()
pool = mp.Pool(processes = (mp.cpu_count()))
args_list = []
for i in range(i, noOfCores):
    temp = ( i * partition_by, partition_by * (i+1) )
    args_list.append(temp)

results = pool.map(checkMatches, args_list)
pool.close()

# Wait for all subprocesses to finish
pool.join()

end = time.time()

# Concatinate returned dataframes of all subprocesses.
results_df = pd.concat(results)
results_df.to_csv("/home/output_mt.csv")
print("Done in "+ str((end-st)/60) +" minutes.") 