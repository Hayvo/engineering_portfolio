from hashlib import sha256
import time as t
import os 
from dotenv import load_dotenv

load_dotenv()

class Config:
    URL = 'http://localhost:8080'
    JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY")
    JWT_ACCESS_TOKEN_EXPIRES = 3600
    JWT_TOKEN_LOCATION = ['headers']

    allowed_origins = ["http://localhost:5173",
                          "http://localhost:5173/",
                            "http://localhost:5173/login",
                            "http://localhost:5173/home"
                       ]