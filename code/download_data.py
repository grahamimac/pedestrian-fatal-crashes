import pandas as pd
import urllib 
import zipfile
import time

# Download and subset relevant data to produce single dataset of non-motor vehicle fatalities over time

# Set defaults
years = list(range(2005,2019)) # Latitude and Longitude start in 2005
acc_keep = ["ST_CASE","LATITUDE","LONGITUD","YEAR","CITY"]
merge_var = "ST_CASE"
dl_path = "../data/raw/{}"
out_path = "../data/{}"
url_base = "ftp://ftp.nhtsa.dot.gov/fars/{}/National/FARS{}NationalCSV.zip"
fname_base = "FARS{}NationalCSV.zip"

# Define getting and reading into pandas a zipped CSV from a directory
def get_pd_from_zip(fname,z):
	with z.open(fname) as f:
		return pd.read_csv(f)

# Download, unzip, and subset
year_files = []
for year in years:
	print(year)
	time.sleep(1)
	out_year = dl_path.format(fname_base.format(year))
	urllib.request.urlretrieve(url_base.format(year,year), out_year)
	with zipfile.ZipFile(out_year) as z:
		try:
			temp_acc = get_pd_from_zip("ACCIDENT.csv",z)
		except KeyError:
			try:
				temp_acc = get_pd_from_zip("ACCIDENT.CSV",z)
			except KeyError:
				temp_acc = get_pd_from_zip("accident.csv",z)
		try:
			temp_per = get_pd_from_zip("PERSON.csv",z)
		except KeyError:
			try:
				temp_per = get_pd_from_zip("PERSON.CSV",z)
			except KeyError:
				temp_per = get_pd_from_zip("person.csv",z)
	temp_acc.columns = [x.upper() for x in temp_acc.columns]		
	year_files.append(temp_per.merge(temp_acc[acc_keep], on = merge_var, how = "left"))

# Combine files and write out
data = pd.concat(year_files, ignore_index = True)
peds = data.loc[data["VEH_NO"] == 0] # Keep only pedestrian person records
peds = peds.dropna(how="all", axis="columns") # Drop all columns with no data
peds.to_csv(out_path.format("Pedestrian_Data_{}_to_{}.csv".format(years[0],years[-1])), index = False)