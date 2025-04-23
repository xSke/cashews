import hashlib
import json
from pathlib import Path
import sys
from datetime import datetime, timedelta, timezone
import pandas as pd
import utils
import requests
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy import create_engine
import os

import utils


def fetch_team_stats():
    utils.init_db()
    for team in utils.fetch_all_teams():
