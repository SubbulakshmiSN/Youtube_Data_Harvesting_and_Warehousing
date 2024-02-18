# Youtube_Data_Harvesting_and_Warehousing
YouTube Data Harvesting and Warehousing using SQL, MongoDB, and Streamlit.
A Python-based tool called YouTube Data Harvesting and Warehousing enables users to obtain and examine data from different YouTube channels. With the help of SQL, MongoDB, and Streamlit, the program creates an interactive and user-friendly interface that makes it simple for users to retrieve, save, and query data related to YouTube channels and videos.

## Installations
pip install pandas
pip install streamlit
pip install psycopg
pip install pymongo
pip install google-api-python-client
### to run streamlit application- streamlit run youtube.py

## Tools and Libraries Used:

## Streamlit:
Using Streamlit, users can interact with the application more easily and retrieve and analyze data more quickly. Here i used streamlit application to create a simple user interface where users can easily input various youtube channel id to extract data and analyze it.

## Python:
Throughout the whole development process, from data retrieval and processing to analysis and visualization, the project makes use of Python's ease of use and versatility. i used vs code to develop the application. Different functional blocks of codes written to compile a final outuput.

## Google API Client: 
The Python googleapiclient module makes it easier to communicate with YouTube's Data API v3, allowing for the retrieval of crucial data including channel and video details as well as comments.

## MongoDB:
MongoDB is a document database that can store structured or unstructured data in a format similar to JSON. It offers flexibility and scalability. MongoDB compass has been helpful in forming a datalake and for json handling.

## PostgreSQL:
With support for multiple data types and sophisticated SQL features, PostgreSQL is a sophisticated and scalable database management system (DBMS) used for managing and storing structured data. Helpful in manipulaing and arranging data into a proper dataframe. certain queries were developed to understand manipulated data which gave more insights.

## YouTube Data Scraping and Ethical Perspective:
The ethical perspective on YouTube data scraping involves following YouTube's terms and conditions, gaining the necessary authorization, and dealing with data protection rules ethically and responsibly. The project places a strong emphasis on protecting confidentiality and privacy, managing data responsibly, and avoiding abuse or deception. To maintain integrity and reduce the impact on the platform and its community, ethical principles are adhered to.

## Libraries Needed:
1. googleapiclient.discovery
2. pandas 
3. psycopg2 
4. streamlit
5. pymongo
   
## Qualities:
1. Using the YouTube API to retrieve channel and video data from YouTube.
2. Data lake-style data storage in a MongoDB database.
3. Data transfer from the data lake to a SQL database so that analysis and querying may be done quickly.
4. Data is searched for and retrieved from the SQL database using various search parameters.
