# lockdown_facilitator

This is a tool to fetch data from API https://www.thecocktaildb.com/api.php into SQLite DB.
Components for different processing stages are placed in api_utilities and db_utilities files respectively.

## Tool run
The tool can be run by following command from repo root directory:
```
pip3 install -r requirements.txt && python3 main.py
```

### Important
Initially, DB file is already created and placed in the repo. Running the tool with DB file present will produce incorrect results.
To produce new DB file, existing one should be deleted.
