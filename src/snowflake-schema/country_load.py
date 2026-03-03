from Config import Config
from Logger import Logger
from Variable import Variables

v = Variables()
v.set("SCRIPT_NAME", "COUNTRY_LOAD")
v.set("LOG", Logger(v))
v.set("STG_VIEW", "STG_D_COUNTRY")
v.set("TMP_TABLE", "TMP_D_COUNTRY")
v.set("TGT_TABLE", "TGT_D_COUNTRY")

sf = Config(v)

# 1️⃣ Truncate Temporary Table
truncate_query = f"""
    TRUNCATE TABLE {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')}
"""
sf.execute_query(truncate_query)

# 2️⃣ Load Distinct Countries into Temp
temp_query = f"""
    INSERT INTO {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')} (COUNTRY_NAME)
    SELECT DISTINCT COUNTRY
    FROM {v.get('STG_SCHEMA')}.{v.get('STG_VIEW')}
"""
sf.execute_query(temp_query)

# 3️⃣ Merge into Target (Insert Only New Countries)
merge_query = f"""
    MERGE INTO {v.get('TGT_SCHEMA')}.{v.get('TGT_TABLE')} AS TGT
    USING {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')} AS TMP
        ON TGT.COUNTRY_NAME = TMP.COUNTRY_NAME
    WHEN NOT MATCHED THEN
        INSERT (COUNTRY_NAME)
        VALUES (TMP.COUNTRY_NAME);
"""
sf.execute_query(merge_query)

# 4️⃣ Close Logger
v.get('LOG').close()
