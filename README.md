# Algotrading Project
### Created by:
Lin Burg<br/>
Inbal Geva Oren

<u> System prerequisites: </u>

**OS**: Windows 10<br/>
**IDE**: PyCharm 2022.3.1 (Professional Edition)<br/>
**MySql Server**: MySql server version 8.0

## Project initial setup:

1. After cloning the project and opening it in Pycharm, create a virtual environment directory for the project:
   1. Press ctrl+alt+s
   2. "Project: XXX"
   3. "Python Interpreter"
   4. "Add Interpreter" => "Add Local Interpreter"
   5. Select "Virtual Environment" => "New"
   6. in Location, name it "venv" and save
   

2. Then, run in pycharm's terminal: "pip install -r requirements.txt"


3. Define two environment variables for each python file in the project : 

   1. Set up an environment variable named "DB_CONNECTION_STRING", which enables the connection 
      to the MySql database via sqlalchemy, as following:

      1. Open the "Edit Run/Debug configuration" dialog
      2. Press "Edit configuration"
      3. Under configuration tab - look for "Environment Variables" ->
         press the edit button to the right
      4. In the new opened window -> manually add another environment variable named: "DB_CONNECTION_STRING".
         Then, in the "value" section, copy and paste the following statement - replacing the "username" and
         "password" with your own, in the correct order:
         "mysql+pymysql://username:password@localhost/stock_database"

   2. Set up the connection with Tiingo API: create an environment variable named "TIINGO_API_TOKEN".<br/>
      To retrieve the necessary token - enter the following link: 
      https://api.tiingo.com/documentation/general/connecting -> click on the
      "click here to see your API Token" button and copy the token. Then, similarly: 
   
      1. Open the "Edit Run/Debug configuration" dialog
      2. Press "Edit configuration"
      3. Under configuration tab - look for "Environment Variables" ->
         press the edit button to the right
      4. In the new opened window -> manually add another environment variable named: "TIINGO_API_TOKEN".
         Then, in the value section, insert the API token retrieved from Tiingo API.


4. Jupyter notebook environment setup: create a ".env" file (locate it in the same subdirectory ("src" / "random forest model") whenever working with the jupyter files) in order to allow the jupyter files to connect to the database and access other files.
   The ".env" file should include the following environment variables (same as the general project configuration): 
   - "DB_CONNECTION_STRING"
   - "TIINGO_API_TOKEN"
   

   ***It's very important to keep the ".env" a local project file (do not upload to GitHub) as it holds your database password***


## Once the initial setup is ready, follow the next steps to run the project:

### Database Creation and Setup of the Routine Update Task:
   Each of the following steps needs to be run once: notice, this stage has an extended runtime.
1. First, create the empty database schemas - run the "src/scripts/create_database_tables.py" script.
2. Populate the database schemas with S&P500 historical data* by running the "src/scripts/populate_db.py" script.
3. Set up a **scheduled task** which will be responsible for the daily database update.
   (**important notice**: this method will only work on Windows operating system)
   To do so, edit the following powershell script according to the instructions (found within the script itself):
   "powershell_scripts/schedule_daily_db_update.sps1" <br/>
   Once the task is scheduled, it'll independently run the "update_db.py" script - make sure you don't remove / locate it in a different path! 

   
   For further information: https://www.makeuseof.com/windows-powershell-scheduled-task/
   Once editing is complete - run the script.

*Tiingo's website offers complimentary 3 years of "DOW30" fundamental historical data. Accessing a longer period of time, to all 500 S&P stocks, requires payment:
https://www.tiingo.com/account/billing/pricing

### Run the Random Forest model

1. To run the Random Forest model, run the following jupyter notebook template according to necessity ("src" directory): 
   1. Initial model with **standard uniform parameters** template: "src/rf_model_standard_params.ipynb"
   2. Model optimization (train, validation, test) template: "src/rf_model_optimization.ipynb"


### Additional project scripts

1. To test the credibility of the data preprocessing, we have created a python script which creates 
   similar empty database schemas (that needs to be also populated as described in the process above) yet these tables are meant to be 
   manually sporadically filled with ***Nan*** values by the user (in order to simulate data that has Nan values). Then, upon running the Random Forest Model jupyter notebook - it
   is possible to test the outcome results of a stock that has undergone Nan values handling compared to same stock that had no missing values.
2. Data structures apprehension from Tiingo's website: during the development process we have sent http requests using the "miscellaneous/http_requests/http_request_tiingo.http" file in order to better understand the data structures 
   that were returned from Tiingo's websites (using Tiingo's API). 
3. Our model optimization results are all found in the **excluded directory**: "" 

**Important notice - if you wish to run any python script or jupyter notebook that is located ***outside*** the "src" directory, make sure to refactor the file's location to the "src" directory first - otherwise it won't run properly.