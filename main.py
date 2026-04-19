import pymysql
import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
from datetime import datetime
 
HOST     = "mysql-1442b5b0-aucegypt-1ace.d.aivencloud.com"
PORT     = 25944
USER     = "avnadmin"
PASSWORD = os.environ.get("DB_PASSWORD", "")   
DATABASE = "defaultdb"
 
def get_db():
    return pymysql.connect(
        host=HOST, port=PORT, user=USER,
        password=PASSWORD, database=DATABASE,
        ssl={"ssl_mode": "REQUIRED"},
        cursorclass=pymysql.cursors.DictCursor
    )
 
app = FastAPI(
    title="Project - Data.gov - CSCE 2501 Milestone 3",
    docs_url="/docs",
    redoc_url="/redoc"
)
 
class NewUser(BaseModel):
    username: str
    email: str
    gender: Optional[str] = "Male or Female"
    birthdate: Optional[str] = None
    country: Optional[str] = None
 
class NewUsage(BaseModel):
    user_id: int
    dataset_id: int
    project_name: str
    project_category: str
 
@app.post("/users/register")
def register_user(user: NewUser):
    db = get_db()
    cur = db.cursor()
    try:
        cur.execute(
            "INSERT INTO `user` (username, email, gender, birthdate, country, registered_at) VALUES (%s,%s,%s,%s,%s,%s)",
            (user.username, user.email, user.gender, user.birthdate, user.country, datetime.now())
        )
        db.commit()
        return {"message": "User registered", "user_id": cur.lastrowid}
    except:
        raise HTTPException(status_code=400, detail="Username or email already exists")
    finally:
        db.close()
 
@app.post("/usage/add")
def add_usage(usage: NewUsage):
    db = get_db()
    cur = db.cursor()
    cur.execute(
        "INSERT INTO `usage` (user_id, dataset_id, project_name, project_category, usage_date) VALUES (%s,%s,%s,%s,%s)",
        (usage.user_id, usage.dataset_id, usage.project_name, usage.project_category, datetime.now())
    )
    db.commit()
    db.close()
    return {"message": "Usage added", "usage_id": cur.lastrowid}
 
@app.get("/usage/by-user/{user_id}")
def get_usage(user_id: int):
    db = get_db()
    cur = db.cursor()
    cur.execute(
        """SELECT u.usage_id, d.title, u.project_name, u.project_category, u.usage_date
           FROM `usage` u JOIN dataset d ON u.dataset_id = d.dataset_id
           WHERE u.user_id = %s""",
        (user_id,)
    )
    rows = cur.fetchall()
    db.close()
    return rows
 
@app.get("/datasets/by-org-type/{org_type}")
def datasets_by_org_type(org_type: str):
    db = get_db()
    cur = db.cursor()
    cur.execute(
        """SELECT d.title, o.name, o.org_type
           FROM dataset d JOIN organization o ON d.org_id = o.org_id
           WHERE o.org_type = %s LIMIT 100""",
        (org_type,)
    )
    rows = cur.fetchall()
    db.close()
    return rows
 
@app.get("/organizations/top5")
def top5_organizations():
    db = get_db()
    cur = db.cursor()
    cur.execute(
        """SELECT o.name, COUNT(d.dataset_id) AS dataset_count
           FROM organization o JOIN dataset d ON o.org_id = d.org_id
           GROUP BY o.org_id ORDER BY dataset_count DESC LIMIT 5"""
    )
    rows = cur.fetchall()
    db.close()
    return rows
 
@app.get("/datasets/by-format/{format}")
def datasets_by_format(format: str):
    db = get_db()
    cur = db.cursor()
    cur.execute(
        """SELECT DISTINCT d.title, dist.format
           FROM dataset d JOIN distribution dist ON d.dataset_id = dist.dataset_id
           WHERE dist.format = %s LIMIT 100""",
        (format,)
    )
    rows = cur.fetchall()
    db.close()
    return rows
 
@app.get("/datasets/by-tag/{tag}")
def datasets_by_tag(tag: str):
    db = get_db()
    cur = db.cursor()
    cur.execute(
        """SELECT d.title, t.name AS tag
           FROM dataset d
           JOIN dataset_tag dt ON d.dataset_id = dt.dataset_id
           JOIN tag t ON dt.tag_id = t.tag_id
           WHERE t.name = %s LIMIT 100""",
        (tag,)
    )
    rows = cur.fetchall()
    db.close()
    return rows
 
@app.get("/datasets/counts")
def dataset_counts():
    db = get_db()
    cur = db.cursor()
    cur.execute("SELECT o.name, COUNT(d.dataset_id) AS total FROM organization o JOIN dataset d ON o.org_id = d.org_id GROUP BY o.org_id ORDER BY total DESC LIMIT 20")
    by_org = cur.fetchall()
    cur.execute("SELECT t.name, COUNT(dt.dataset_id) AS total FROM topic t JOIN dataset_topic dt ON t.topic_id = dt.topic_id GROUP BY t.topic_id ORDER BY total DESC LIMIT 20")
    by_topic = cur.fetchall()
    cur.execute("SELECT `format`, COUNT(*) AS total FROM distribution GROUP BY `format` ORDER BY total DESC LIMIT 20")
    by_format = cur.fetchall()
    cur.execute("SELECT org_type, COUNT(d.dataset_id) AS total FROM organization o JOIN dataset d ON o.org_id = d.org_id GROUP BY org_type ORDER BY total DESC")
    by_org_type = cur.fetchall()
    db.close()
    return {"by_organization": by_org, "by_topic": by_topic, "by_format": by_format, "by_org_type": by_org_type}
 
@app.get("/datasets/top5-used")
def top5_used_datasets():
    db = get_db()
    cur = db.cursor()
    cur.execute(
        """SELECT d.title, COUNT(u.user_id) AS user_count
           FROM dataset d JOIN `usage` u ON d.dataset_id = u.dataset_id
           GROUP BY d.dataset_id ORDER BY user_count DESC LIMIT 5"""
    )
    rows = cur.fetchall()
    db.close()
    return rows
 
@app.get("/usage/distribution")
def usage_distribution():
    db = get_db()
    cur = db.cursor()
    cur.execute("SELECT project_category, COUNT(*) AS total FROM `usage` GROUP BY project_category")
    rows = cur.fetchall()
    db.close()
    return rows
 
@app.get("/tags/top-by-project")
def top_tags_by_project():
    db = get_db()
    cur = db.cursor()
    cur.execute(
        """SELECT u.project_category, t.name AS tag, COUNT(*) AS total
           FROM `usage` u
           JOIN dataset_tag dt ON u.dataset_id = dt.dataset_id
           JOIN tag t ON dt.tag_id = t.tag_id
           GROUP BY u.project_category, t.tag_id
           ORDER BY u.project_category, total DESC"""
    )
    rows = cur.fetchall()
    db.close()
    result = {}
    for row in rows:
        cat = row["project_category"]
        if cat not in result:
            result[cat] = []
        if len(result[cat]) < 10:
            result[cat].append({"tag": row["tag"], "count": row["total"]})
    return result