# WARM SQL AI Agent

A Python-based AI agent that interfaces with a Microsoft SQL Server database (WARM database from Prarie Research Institue) and enables natural language querying using OpenAI's GPT-3.5 model. The agent translates natural language questions into SQL queries and provides formatted, analytical responses.

## Features

- Dual-approach query processing system:
  - Fast LCEL (LangChain Expression Language) chain for simple queries
  - Comprehensive SQL agent for complex queries
- Natural language to SQL query conversion using GPT-4o-mini
- Interactive query verification before execution
- Automatic database schema detection and integration
- Secure database connection handling with Kerberos authentication
- Built-in safety constraints:
  - Read-only operations (SELECT queries only)
  - No data modification capabilities
  - Query verification prompts
- Comprehensive error handling and logging
- Custom prompt templates for accurate SQL generation

## Prerequisites

- Python 3.x
- ODBC Driver 18 for SQL Server
- OpenAI API key
- Access to WARM database
- Kerberos authentication configured

## Quick Installation

1. Clone the repository
2. Run the setup script:
```bash
bash
chmod +x setup.sh
./setup.sh
```

This will:
- Create the conda environment
- Install ODBC Driver
- Install Kerberos
- Create a template .env file

3. Update the OPENAI_API_KEY in .env file

## Manual Installation

If you prefer to install components manually or the setup script fails, see [manual_installation.md](docs/manual_installation.md) for step-by-step instructions.

## Usage

1. Ensure your Kerberos ticket is valid and you have access to the PRI WARM database:
```bash
kinit {netid}@AD.UILLINOIS.EDU
```
2. Run the agent:
```bash
conda activate warm_ai_agent
python -m warm_ai.main
```

## Configuration

Create a `.env` file in the project root with the following variables:
```
DB_DRIVER="{ODBC Driver 18 for SQL Server}"  
DB_SERVER=your_server_name
DB_NAME=your_database_name
DB_TRUSTED_CONNECTION=yes
DB_TRUST_SERVER_CERT=yes
OPENAI_API_KEY=your_openai_api_key
```

## Usage

1. Ensure your Kerberos ticket is valid

```bash
kinit   {netid}@AD.UILLINOIS.EDU
```

2. Run the agent:
```bash
python -m warm_ai.main
```

3. Enter your questions in natural language when prompted. Type 'quit' to exit.

EXAMPLE:

```python
> python3 WARM_ai_agent.py
Starting WARM AI Agent...
Initializing agent with connection string...
Connecting to database...
Successfully connected to the database

Enter your question (or 'quit' to exit): filtering out any data that is above or below 3 standard deviations of the average temperature in champaign, what are some basic summary statistics of the temperature in champaign last year?

Processing natural language query...
Error in workflow: No intent found in state


> Entering new SQL Agent Executor chain...

Invoking: `sql_db_list_tables` with `{}`


WarmICNDaily, WarmICNData, WarmICNFiveMin
Invoking: `sql_db_schema` with `{'table_names': 'WarmICNDaily'}`



CREATE TABLE [WarmICNDaily] (
        [ID] INTEGER NOT NULL IDENTITY(1,1), 
        [nStationCode] NCHAR(10) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL, 
        [nDateTime] DATETIME NOT NULL, 
        [nWindSpeed] DECIMAL(10, 4) NULL, 
        [nWindDirectionDeg] DECIMAL(10, 4) NULL, 
        [nAirTempMax] DECIMAL(10, 4) NULL, 
        [nAirTempMin] DECIMAL(10, 4) NULL, 
        [nRelHumidMax] DECIMAL(10, 4) NULL, 
        [nRelHumidMin] DECIMAL(10, 4) NULL, 
        [nPrecipDaily] DECIMAL(10, 4) NULL, 
        [n2InBareMax] DECIMAL(10, 4) NULL, 
        [n2InBareMaxTm] DATETIME NULL, 
        [n2InBareMin] DECIMAL(10, 4) NULL, 
        [n2InBareMinTm] DATETIME NULL, 
        [n4InSoilMax] DECIMAL(10, 4) NULL, 
        [n4InSoilMin] DECIMAL(10, 4) NULL, 
        [n8InSoilMax] DECIMAL(10, 4) NULL, 
        [n8InSoilMin] DECIMAL(10, 4) NULL, 
        [n4InBareSoilMax] DECIMAL(10, 4) NULL, 
        [n4InBareSoilMin] DECIMAL(10, 4) NULL, 
        [nSoilTemp0Max] DECIMAL(10, 4) NULL, 
        [nSoilTemp0Min] DECIMAL(10, 4) NULL, 
        [nSoilTemp5Max] DECIMAL(10, 4) NULL, 
        [nSoilTemp5Min] DECIMAL(10, 4) NULL, 
        [nSoilTemp10Max] DECIMAL(10, 4) NULL, 
        [nSoilTemp10Min] DECIMAL(10, 4) NULL, 
        [nSoilTemp20Max] DECIMAL(10, 4) NULL, 
        [nSoilTemp20Min] DECIMAL(10, 4) NULL, 
        [nTS] DATETIME NULL, 
        CONSTRAINT [PK_WarmICNDaily] PRIMARY KEY ([nStationCode], [nDateTime])
)

/*
3 rows from WarmICNDaily table:
ID      nStationCode    nDateTime       nWindSpeed      nWindDirectionDeg       nAirTempMax     nAirTempMin     nRelHumidMax    nRelHumidMin    nPrecipDaily    n2InBareMax     n2InBareMaxTm   n2InBareMin  n2InBareMinTm    n4InSoilMax     n4InSoilMin     n8InSoilMax     n8InSoilMin     n4InBareSoilMax n4InBareSoilMin nSoilTemp0Max   nSoilTemp0Min   nSoilTemp5Max   nSoilTemp5Min   nSoilTemp10Max  nSoilTemp10MinnSoilTemp20Max  nSoilTemp20Min  nTS
124690   dek            2021-05-01 00:00:00     50.0700 235.5000        30.8100 5.6080  67.4800 24.3000 0.0000  21.2900 2021-05-01 16:07:30     10.9500 2021-05-01 06:08:30     14.1600 10.9400 13.1400 10.8900       17.2300 11.7000 0.0000  0.0000  0.0000  0.0000  0.0000  0.0000  0.0000  0.0000  2021-05-02 00:00:00
5407    bbc             2012-01-01 00:00:00     38.1600 307.3000        6.4820  -3.6360 91.3000 51.2900 0.0820  None    None    None    None    4.3740  3.0630  3.4470  2.5330  -0.1130 -3.1440 3.6810  0.13703.5020  1.0750  3.5020  1.5200  3.4660  2.0430  None
5408    bbc             2012-01-02 00:00:00     31.8100 304.4000        -3.5370 -9.2800 89.7000 42.7200 0.0050  None    None    None    None    3.1040  1.8340  2.5430  1.3820  -2.9200 -4.5600 0.6840  -0.7200       1.6260  -0.1160 2.0960  0.2770  2.6710  0.8020  None
*/
Invoking: `sql_db_schema` with `{'table_names': 'WarmICNData'}`



CREATE TABLE [WarmICNData] (
        [ID] INTEGER NOT NULL IDENTITY(1,1), 
        [nStationCode] NCHAR(10) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL, 
        [nDateTime] DATETIME NOT NULL, 
        [nAirTemp] DECIMAL(10, 4) NULL, 
        [nRelHumid] DECIMAL(10, 4) NULL, 
        [nWindDirectionDeg] DECIMAL(10, 4) NULL, 
        [nWindSpeed] DECIMAL(10, 4) NULL, 
        [nWindGust] DECIMAL(10, 4) NULL, 
        [nSolRadiat] DECIMAL(10, 4) NULL, 
        [n2InBareSoil] DECIMAL(10, 4) NULL, 
        [n2InBareMax] DECIMAL(10, 4) NULL, 
        [n2InBareMaxTm] DATETIME NULL, 
        [n2InBareMin] DECIMAL(10, 4) NULL, 
        [n2InBareMinTm] DATETIME NULL, 
        [n4InSoil] DECIMAL(10, 4) NULL, 
        [n8InSoil] DECIMAL(10, 4) NULL, 
        [n4InBareSoil] DECIMAL(10, 4) NULL, 
        [nBarPress] DECIMAL(10, 4) NULL, 
        [nBarPressCalc] DECIMAL(10, 4) NULL, 
        [nDewPoint] DECIMAL(10, 4) NULL, 
        [nPrecipHrly] DECIMAL(10, 4) NULL, 
        [nPotEvapoTrans] DECIMAL(10, 4) NULL, 
        [nSoilMoist5] DECIMAL(10, 4) NULL, 
        [nSoilMoist10] DECIMAL(10, 4) NULL, 
        [nSoilMoist20] DECIMAL(10, 4) NULL, 
        [nSoilMoist50] DECIMAL(10, 4) NULL, 
        [nSoilMoist100] DECIMAL(10, 4) NULL, 
        [nSoilMoist150] DECIMAL(10, 4) NULL, 
        [nAirTempMax] DECIMAL(10, 4) NULL, 
        [nAirTempMin] DECIMAL(10, 4) NULL, 
        [nWellLevel] DECIMAL(10, 4) NULL, 
        [nSDWellLevel] DECIMAL(10, 4) NULL, 
        [nRelHumidMax] DECIMAL(10, 4) NULL, 
        [nRelHumidMin] DECIMAL(10, 4) NULL, 
        [nBarPressMax] DECIMAL(10, 4) NULL, 
        [nBarPressMin] DECIMAL(10, 4) NULL, 
        [nDirMaxGust] DECIMAL(10, 4) NULL, 
        [nTS] DATETIME NULL, 
        CONSTRAINT [PK_WarmICNData] PRIMARY KEY ([nStationCode], [nDateTime])
)

/*
3 rows from WarmICNData table:
ID      nStationCode    nDateTime       nAirTemp        nRelHumid       nWindDirectionDeg       nWindSpeed      nWindGust       nSolRadiat      n2InBareSoil    n2InBareMax     n2InBareMaxTm   n2InBareMin  n2InBareMinTm    n4InSoil        n8InSoil        n4InBareSoil    nBarPress       nBarPressCalc   nDewPoint       nPrecipHrly     nPotEvapoTrans  nSoilMoist5     nSoilMoist10    nSoilMoist20    nSoilMoist50 nSoilMoist100    nSoilMoist150   nAirTempMax     nAirTempMin     nWellLevel      nSDWellLevel    nRelHumidMax    nRelHumidMin    nBarPressMax    nBarPressMin    nDirMaxGust     nTS
972918  bbc             2010-07-16 12:00:00     29.6800 52.3100 204.8000        7.6850  12.9800 0.9550  None    None    None    None    None    25.0500 23.7300 34.1200 1014.0743       29.9458 18.8581 0.00009999.0000       0.2370  0.2700  0.1540  0.1670  0.1640  0.0770  None    None    None    None    None    None    None    None    None    None
972919  bbc             2010-07-16 13:00:00     30.0000 51.8500 206.3000        9.4100  14.3600 0.9460  None    None    None    None    None    25.4600 23.8900 35.3000 1013.6743       29.9340 19.0109 0.00009999.0000       0.2360  0.2660  0.1520  0.1680  0.1640  0.0770  None    None    None    None    None    None    None    None    None    None
972920  bbc             2010-07-16 14:00:00     30.2400 51.3200 212.4000        10.2300 15.9400 0.8910  None    None    None    None    None    26.0400 24.1800 36.6900 1013.2743       29.9222 19.0667 0.00009999.0000       0.2350  0.2660  0.1520  0.1670  0.1620  0.0790  None    None    None    None    None    None    None    None    None    None
*/
Invoking: `sql_db_schema` with `{'table_names': 'WarmICNFiveMin'}`



CREATE TABLE [WarmICNFiveMin] (
        [ID] INTEGER NOT NULL IDENTITY(1,1), 
        [nStationCode] NCHAR(10) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL, 
        [nDateTime] DATETIME NOT NULL, 
        [nAirTemp] DECIMAL(10, 4) NULL, 
        [nRelHumid] DECIMAL(10, 4) NULL, 
        [nWindDirectionDeg] DECIMAL(10, 4) NULL, 
        [nWindSpeed] DECIMAL(10, 4) NULL, 
        [nWindGust] DECIMAL(10, 4) NULL, 
        [nBarPress] DECIMAL(10, 4) NULL, 
        [nBarPressCalc] DECIMAL(10, 4) NULL, 
        [nDewPoint] DECIMAL(10, 4) NULL, 
        [nPrecip] DECIMAL(10, 4) NULL, 
        [nPotEvapoTrans] DECIMAL(10, 4) NULL, 
        [nSolRadiat] DECIMAL(10, 4) NULL, 
        [nWellLevel] DECIMAL(10, 4) NULL, 
        [nAirTemp18] DECIMAL(10, 4) NULL, 
        [nAirTemp120] DECIMAL(10, 4) NULL, 
        [nWindSpd120] DECIMAL(10, 4) NULL, 
        [nWindDir120] DECIMAL(10, 4) NULL, 
        [nTS] DATETIME NULL, 
        CONSTRAINT [PK_WarmICNFiveMin] PRIMARY KEY ([nStationCode], [nDateTime])
)

/*
3 rows from WarmICNFiveMin table:
ID      nStationCode    nDateTime       nAirTemp        nRelHumid       nWindDirectionDeg       nWindSpeed      nWindGust       nBarPress       nBarPressCalc   nDewPoint       nPrecip nPotEvapoTrans  nSolRadiat    nWellLevel      nAirTemp18      nAirTemp120     nWindSpd120     nWindDir120     nTS
2888647 bbc             2015-06-15 16:05:00     23.7600 91.4000 266.6000        11.3000 0.0000  0.0000  0.0000  22.2641 0.0290  0.0000  None    0.0000  None    None    None    None    2015-07-23 12:16:14
2888648 bbc             2015-06-15 16:10:00     23.0000 87.6000 284.0000        13.8400 0.0000  0.0000  0.0000  20.8208 0.0170  0.0000  None    0.0000  None    None    None    None    2015-07-23 12:16:14
2888649 bbc             2015-06-15 16:15:00     22.2100 89.7000 273.8000        14.4700 0.0000  0.0000  0.0000  20.4274 0.0070  0.0000  None    0.0000  None    None    None    None    2015-07-23 12:16:14
*/
Invoking: `sql_db_query_checker` with `{'query': "WITH TempStats AS (\n    SELECT \n        AVG(nAirTempMax) AS AvgMaxTemp,\n        AVG(nAirTempMin) AS AvgMinTemp,\n        STDEV(nAirTempMax) AS StdDevMaxTemp,\n        STDEV(nAirTempMin) AS StdDevMinTemp\n    FROM WarmICNDaily\n    WHERE nDateTime >= '2022-01-01' AND nDateTime < '2023-01-01'\n),\nFilteredTemps AS (\n    SELECT \n        nAirTempMax,\n        nAirTempMin\n    FROM WarmICNDaily, TempStats\n    WHERE nDateTime >= '2022-01-01' AND nDateTime < '2023-01-01'\n    AND nAirTempMax BETWEEN (AvgMaxTemp - 3 * StdDevMaxTemp) AND (AvgMaxTemp + 3 * StdDevMaxTemp)\n    AND nAirTempMin BETWEEN (AvgMinTemp - 3 * StdDevMinTemp) AND (AvgMinTemp + 3 * StdDevMinTemp)\n)\nSELECT \n    AVG(nAirTempMax) AS FilteredAvgMaxTemp,\n    AVG(nAirTempMin) AS FilteredAvgMinTemp,\n    MIN(nAirTempMax) AS FilteredMinMaxTemp,\n    MAX(nAirTempMax) AS FilteredMaxMaxTemp,\n    MIN(nAirTempMin) AS FilteredMinMinTemp,\n    MAX(nAirTempMin) AS FilteredMaxMinTemp\nFROM FilteredTemps;"}`
responded: The relevant table for temperature data in Champaign is likely the `WarmICNDaily` table, which contains columns for maximum and minimum air temperatures. 

To calculate summary statistics for the temperature in Champaign last year, I will query the average, standard deviation, minimum, and maximum temperatures, filtering out any data that is above or below 3 standard deviations from the average temperature.

I will first check the query for correctness before executing it. Here’s the query I plan to use:

```sql
WITH TempStats AS (
    SELECT 
        AVG(nAirTempMax) AS AvgMaxTemp,
        AVG(nAirTempMin) AS AvgMinTemp,
        STDEV(nAirTempMax) AS StdDevMaxTemp,
        STDEV(nAirTempMin) AS StdDevMinTemp
    FROM WarmICNDaily
    WHERE nDateTime >= '2022-01-01' AND nDateTime < '2023-01-01'
),
FilteredTemps AS (
    SELECT 
        nAirTempMax,
        nAirTempMin
    FROM WarmICNDaily, TempStats
    WHERE nDateTime >= '2022-01-01' AND nDateTime < '2023-01-01'
    AND nAirTempMax BETWEEN (AvgMaxTemp - 3 * StdDevMaxTemp) AND (AvgMaxTemp + 3 * StdDevMaxTemp)
    AND nAirTempMin BETWEEN (AvgMinTemp - 3 * StdDevMinTemp) AND (AvgMinTemp + 3 * StdDevMinTemp)
)
SELECT 
    AVG(nAirTempMax) AS FilteredAvgMaxTemp,
    AVG(nAirTempMin) AS FilteredAvgMinTemp,
    MIN(nAirTempMax) AS FilteredMinMaxTemp,
    MAX(nAirTempMax) AS FilteredMaxMaxTemp,
    MIN(nAirTempMin) AS FilteredMinMinTemp,
    MAX(nAirTempMin) AS FilteredMaxMinTemp
FROM FilteredTemps;
```

Now, I will check this query for correctness.

```sql
WITH TempStats AS (
    SELECT 
        AVG(nAirTempMax) AS AvgMaxTemp,
        AVG(nAirTempMin) AS AvgMinTemp,
        STDEV(nAirTempMax) AS StdDevMaxTemp,
        STDEV(nAirTempMin) AS StdDevMinTemp
    FROM WarmICNDaily
    WHERE nDateTime >= '2022-01-01' AND nDateTime < '2023-01-01'
),
FilteredTemps AS (
    SELECT 
        nAirTempMax,
        nAirTempMin
    FROM WarmICNDaily
    CROSS JOIN TempStats
    WHERE nDateTime >= '2022-01-01' AND nDateTime < '2023-01-01'
    AND nAirTempMax BETWEEN (AvgMaxTemp - 3 * StdDevMaxTemp) AND (AvgMaxTemp + 3 * StdDevMaxTemp)
    AND nAirTempMin BETWEEN (AvgMinTemp - 3 * StdDevMinTemp) AND (AvgMinTemp + 3 * StdDevMinTemp)
)
SELECT 
    AVG(nAirTempMax) AS FilteredAvgMaxTemp,
    AVG(nAirTempMin) AS FilteredAvgMinTemp,
    MIN(nAirTempMax) AS FilteredMinMaxTemp,
    MAX(nAirTempMax) AS FilteredMaxMaxTemp,
    MIN(nAirTempMin) AS FilteredMinMinTemp,
    MAX(nAirTempMin) AS FilteredMaxMinTemp
FROM FilteredTemps;
```
Invoking: `sql_db_query` with `{'query': "WITH TempStats AS (\n    SELECT \n        AVG(nAirTempMax) AS AvgMaxTemp,\n        AVG(nAirTempMin) AS AvgMinTemp,\n        STDEV(nAirTempMax) AS StdDevMaxTemp,\n        STDEV(nAirTempMin) AS StdDevMinTemp\n    FROM WarmICNDaily\n    WHERE nDateTime >= '2022-01-01' AND nDateTime < '2023-01-01'\n),\nFilteredTemps AS (\n    SELECT \n        nAirTempMax,\n        nAirTempMin\n    FROM WarmICNDaily\n    CROSS JOIN TempStats\n    WHERE nDateTime >= '2022-01-01' AND nDateTime < '2023-01-01'\n    AND nAirTempMax BETWEEN (AvgMaxTemp - 3 * StdDevMaxTemp) AND (AvgMaxTemp + 3 * StdDevMaxTemp)\n    AND nAirTempMin BETWEEN (AvgMinTemp - 3 * StdDevMinTemp) AND (AvgMinTemp + 3 * StdDevMinTemp)\n)\nSELECT \n    AVG(nAirTempMax) AS FilteredAvgMaxTemp,\n    AVG(nAirTempMin) AS FilteredAvgMinTemp,\n    MIN(nAirTempMax) AS FilteredMinMaxTemp,\n    MAX(nAirTempMax) AS FilteredMaxMaxTemp,\n    MIN(nAirTempMin) AS FilteredMinMinTemp,\n    MAX(nAirTempMin) AS FilteredMaxMinTemp\nFROM FilteredTemps;"}`


[(Decimal('17.202171'), Decimal('5.509305'), Decimal('-19.8200'), Decimal('37.8800'), Decimal('-28.1800'), Decimal('27.3900'))]The summary statistics of the temperature in Champaign for the last year, after filtering out any data that is above or below 3 standard deviations from the average, are as follows:

- **Filtered Average Maximum Temperature:** 17.20 °C
- **Filtered Average Minimum Temperature:** 5.51 °C
- **Filtered Minimum Maximum Temperature:** -19.82 °C
- **Filtered Maximum Maximum Temperature:** 37.88 °C
- **Filtered Minimum Minimum Temperature:** -28.18 °C
- **Filtered Maximum Minimum Temperature:** 27.39 °C

These statistics provide a basic overview of the temperature range and averages for Champaign over the past year.

> Finished chain.
```

## Key Components

### SQLAIAgent Class
- Manages database connections and AI interactions
- Implements both simple and complex query processing chains
- Uses LangChain Expression Language (LCEL) for efficient query processing
- Falls back to more complex agent for complicated queries
- Provides SQL query verification before execution

### Main Functions
- `query()`: Processes natural language queries using AI
- `execute_sql()`: Safely executes verified SQL queries
- `extract_sql_query()`: Extracts SQL queries from AI responses
- `connect()`: Establishes database connection with proper configuration

## Security Features

- Environment variable-based configuration
- Kerberos authentication support
- Trusted connections by default
- Parameterized query execution
- Certificate validation options

## Error Handling

The agent includes comprehensive error handling for:
- Database connection issues
- Query execution errors
- API communication problems
- Schema detection failures
- Data type conversion errors

## AI Implementation

The agent uses a dual-approach system for query processing:
1. Simple LCEL chain for straightforward queries
2. Complex SQL agent for more sophisticated requests

The system uses:
- OpenAI's GPT-4-mini model for query understanding
- LangChain's SQL Database Toolkit
- Custom prompt templates for accurate SQL generation
- Interactive query verification system

## Contributing

1. Fork the repository
2. Create a feature branch
3. Submit a pull request

For issues and enhancement requests, please use the GitHub Issues tracker.

## Further Reading & Resources

### UIUC Resources
- [WARM Database Documentation](https://warm.isws.illinois.edu/warm/) - Official documentation for the WARM database
- [UIUC VPN Setup Guide](https://cybersecurity.illinois.edu/change-to-campus-vpn-login-process-coming-march-12/) - Official VPN configuration guide
- [Kerberos at UIUC](https://answers.uillinois.edu/illinois/page.php?id=47575) - UIUC Kerberos authentication guide

### Technical Documentation
- [OpenAI API Documentation](https://platform.openai.com/docs/) - OpenAI API reference
- [LangChain Documentation](https://python.langchain.com/docs/get_started/introduction) - LangChain framework guide
- [SQL Server on Linux](https://learn.microsoft.com/en-us/sql/linux/sql-server-linux-overview) - Microsoft's guide for SQL Server on Linux
- [ODBC Driver Documentation](https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server) - Microsoft ODBC driver setup

### Related Research
- [Prairie Research Institute](https://prairie.illinois.edu/) - Home organization for the WARM database
- [Illinois State Water Survey](https://www.isws.illinois.edu/) - Source of water and atmospheric data
- [Water and Atmospheric Resources Monitoring Data](https://www.isws.illinois.edu/dat/) - WARM program data