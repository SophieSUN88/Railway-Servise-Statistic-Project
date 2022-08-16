import datetime
import pandas as pd

from insight_engine_etl.fetchable import HTTPFetchable, FetchError
from insight_engine_etl.transform import PandasTransformFromExcel
from insight_engine_etl.common import sanitize_key, ds_to_datetime

#ffgfgfd
class Datasource(HTTPFetchable):
    url = "https://www.stb.gov/reports-data/rail-service-data/#railroads-tab-content-1-"
    key = "/All Class 1 Railroads/"

    # These variables below are no longer needed, because we've moved them into function arguments
    # This allows us to remove some unneeded import statements as well

    # attachment_path = r's3://' + config.get('S3', 'bucket_name') + '/airflow_file_drop/ie_ep724_data/'
    # tz = timezone('America/New_York')
    # # file_date = today(tzinfo=tz)
    # file_date = datetime.datetime.today()

    def get_latest_date(self):
        '''
        This method will return a tuple containing the latest timestamp and download link from the STB website.

        Raises a FetchError if no file is found
        '''
        doc = self.get_soup(self.url)
        # doc = BeautifulSoup(requests.get(self.url).text, "html.parser")
        for link in doc.find_all("a"):
            file_link = link.get('href')
            if file_link == None:
                continue
            if self.key in file_link:
                date = link.text[:-5]
                latest_date = datetime.datetime.strptime(date,"%m-%d-%y")

                # Return both the date and link, that way we can re-use this method below in self.fetch too
                return latest_date, file_link

        # We'll raise an error instead of returning None
        # If no file is found, that means something changed on the website
        raise FetchError('File link not found')

    # This should move inside of the method body
    # latest_date = get_latest_date()

    def wait_for_data(self,target_date):
        '''
        Checks if provided target_date is availabe on STB website. Returns boolean
        '''
        # You can use ds_to_datetime function in insight_engine_etl.common
        # to convert date string to Python datetime object
        target_date = ds_to_datetime(target_date, "%Y-%m-%d")

        # We'll call self.get_latest_date() here
        # It now returns a two-value tuple containing the date + download link
        latest_date, link = self.get_latest_date()

        if latest_date >= target_date:
            # HTTPFetchable class as a log method that you can use, instead of print
            self.log("latest_date == target_date, update the new data.")
            return True
        else:
            self.log("latest_date != target_date, wait for the new data.")
            return False

    def fetch(self,target_path):
        '''
        Download latest available data from STB webiste to target_path
        '''

        # This method becomes much simpler now, because we can re-use:
        # 1. self.get_latest_date() to get the date and link
        # 2. self.download (defined in HTTPFetchable) to actually download the data to a local or s3 path

        latest_date, file_link = self.get_latest_date()
        self.log("lastest date:",latest_date)
        self.log("file link:", file_link)
        self.download(file_link, target_path)
        self.log("Download original file successfully!")


# We'll move the transform logic to a separate class
# By inheriting from PandasTransformFromExcel, we get some useful functionality
# This class handles reading an excel file to a pandas DataFrame (self.load_as_dataframe)
# and writing the transformed data back to a new file. (self.dump_dataframe)
# Our job is to just implement the in-between steps, in a method self.transform_dataframe
# And in this case self.transform_dataframe just needs to combine self.clean_data and self.get_observation
class TransformToObservations(PandasTransformFromExcel):

    pandas_kwargs = {'engine': 'openpyxl'}  # required to read xlsx files

    # The parent class handles reading the excel file to a dataframe, so we can just pass this method a dataframe
    def clean_data(self,data):
        # delete /n in name 
        data.rename(columns={'Railroad/\nRegion':'Railroad/Region'},inplace = True)

        # drop the last row which are all nan
        # data = data.dropna(axis=0 , how='all',inplace=True)


        # data = data.drop(labels=3310,axis=0) 
        data = data.dropna(subset=['Category No.'])

        # convert the "Category No." type from float to int
        data['Category No.']=data['Category No.'].astype(int)
        data = data.reset_index(drop= True)

        # add sr_name in data
        data["sr_name"] = data["Railroad/Region"].astype(str).apply(sanitize_key)+"_"+data['Variable'].astype(str).apply(sanitize_key)+"_"+ data['Sub-Variable'].astype(str).apply(sanitize_key) +"_"+data['Category No.'].astype(str).apply(sanitize_key) 
        data["sr_name"] = data['sr_name'].str.replace("nan","").apply(sanitize_key)
        
        # drop the duplicated rows
        data.drop_duplicates(subset = 'sr_name',inplace = True)
        data.reset_index(drop = True, inplace = True)
        print("Row number after clean : ",len(data)," !")
        return data

    # add target_date and release_date as an arguments to this method
    def get_observation(self, data, target_date, release_date):

        target_date = ds_to_datetime(target_date)

        # Step1: get sr_name and convert series to dataframe
        observation = data["sr_name"]
        observation = observation.to_frame()
        print(type(observation))
        # print(observation.head(3))
        # add cl_name, key colums
        observation["cl_name"]='ep724'
        observation['key']='value'
        print(observation.head(3))

        # step2: add the latest date column
        # get the namekeep before I add the date column
        namekeep= list(observation.columns.values)
        print("Row number before get ob_obs_date columns : ",len(observation)," !")
        # assume we will check whethe new data updated, so we only need to update the data of the latest date
        observation[target_date.strftime("%Y-%m-%d")]=data[target_date]
        # convert date from column to row to create "ob_obs_date" column
        observation = observation.set_index(namekeep).stack()
        observation.index.names=namekeep + ["ob_obs_date"]
        observation =observation.reset_index(name="value")
        print("Row number after get ob_obs_date and values columns : ",len(observation)," !")
        # step 3: Clean the "value" , some contain ",", some are ".", " "
        for i in range(len(observation["value"])):
            try:
                float(str(observation["value"][i]))
            except:
                if "," in str(observation["value"][i]):
                    print("value with ',' before replace:",observation['value'][i])
                    observation["value"][i] = observation["value"][i].replace(",","")
                    print("value with ',' after replace:",observation['value'][i])
                else:
                    print("value with other charactor before replace:",observation['value'][i],"!")
                    observation["value"][i] = "NaN"
                    print("value with other charactor after replace:",observation['value'][i],"!")
        observation= observation.astype({"value":float})

        # step 4: add "ob_create_date" column
        observation['ob_release_date'] = release_date

        # drop NaN
        observation = observation.dropna(subset=["value"])
        observation.reset_index(drop=True, inplace = True)
        print("Row number after observation : ",len(observation)," !")
        print(observation.head(3))
        return observation

    # Now we implement transform_dataframe method by combining clean_data and get_observations
    def transform_dataframe(self, df, target_date, release_date, **kwargs):
        data = self.clean_data(df)
        observations_to_upload = self.get_observation(data, target_date=target_date, release_date=release_date)
        return observations_to_upload

    # And finally, this method is no longer needed now.
    # PandasTransformFromExcel has a method called "run" that does exactly these steps



class TransformToSeries(PandasTransformFromExcel):

    pandas_kwargs = {'engine': 'openpyxl'}  # required to read xlsx files   

    def get_series(self,data):
        n = len(data)
        # get sr_desc
        sr_desc=[]
        for i in range(n):
            desc = "Railroad_Region : "
            if str(data['Railroad/Region'][i]) != "nan" :
                desc = desc + data['Railroad/Region'][i] 
            desc = desc + ", Variable : " 
            if str(data['Variable'][i]) != "nan":
                desc = desc + data['Variable'][i]
            desc = desc + ", SubVariable : " 
            if str(data['Sub-Variable'][i]) != "nan":
                desc = desc + data['Sub-Variable'][i]
            desc = desc + ", Measure : " 
            if str(data['Measure'][i]) != "nan":
                desc = desc + data['Measure'][i]
            sr_desc.append(desc)

        # get the series_freq
        series_freq=["weekly"] * n 

        sn_name = ["unknown"] * n
        cl_name = ["ep724"] * n
        sr_field = ["[{\"desc\": \"Value\", \"name\": \"value\", \"type\": \"number\", \"units\": \"unit\"}]"] * n
        # create the final series
        # initialize data of lists.
        new_table = {"sr_desc":sr_desc,"sn_name":sn_name,"cl_name":cl_name,"fr_name":series_freq,"sr_field":sr_field}
        # Create DataFrame
        series = pd.DataFrame(new_table)

        # add columns from data to sereies
        series["sr_name"] = data["sr_name"]
        series["railroad_region"] = data["Railroad/Region"]
        series["variable"]=data['Variable']
        series["sub_variable"]=data['Sub-Variable']
        series["measure"]=data['Measure']
        series["category_no"]=data['Category No.']

        return series

    def transform_dataframe(self, df, **kwargs):
        data = TransformToObservations().clean_data(df)
        series_to_upload = self.get_series(data)
        return series_to_upload
