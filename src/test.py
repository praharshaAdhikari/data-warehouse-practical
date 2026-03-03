import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.Snowflake import Config

sf = Config()

print(sf.execute_query("select * from IOE.LANDING.SALES"))
