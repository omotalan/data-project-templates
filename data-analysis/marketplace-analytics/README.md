



# Instructions

## Connecting to Kaggle API for easier file management
1. Install kaggle API (```pip install kaggle```), preferably in a virtual environment (aka venv)
2. Generate API key from your Kaggle account settings: ```Kaggle → Account → “Create New API Token”```. Follow instructions
3. Run command to download files ```kaggle datasets download -d mkechinov/ecommerce-behavior-data-from-multi-category-store```
   1. It will generate a zip file -> unzip it by running ```unzip file.zip -d data/```
   2. Add your /data directory to .gitignore, so you won't load it to the repo
   3. Notice this will download the dataset to disk, so make sure you have space

## Connecting to duck db, loading the data (see notebook for details)
1. Install duck db (```pip install duckdb```) in your venv
2. Sample the dataset (the full file is too large) for optimized processing
   1. Notice that if you want to analyze the whole file, you'll need to load all the data. More on that later
3. Load duck db, create your db file and schemas for dbt
4. Load the sampled dataset to duck db

## Set up dbt, create dbt schema
1. Install duck db's dbt adapter (```pip install dbt-duckdb```) in your venv
2. Initialize dbt project
   1. Define your project name when asked
3. Sample the dataset (the full file is too large) for optimized processing
   1. Notice that if you want to analyze the whole file, you'll need to load all the data. More on that later
4. Load duck db, create your db file and schemas for dbt
5. Load the sampled dataset to duck db